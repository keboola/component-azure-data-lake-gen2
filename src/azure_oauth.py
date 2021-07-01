'''
Template Component main class.

'''

import csv  # noqa
import logging
import os  # noqa
import re  # noqa
import sys
import json  # noqa
import requests
import datetime

import logging_gelf.formatters
import logging_gelf.handlers  # noqa
# from azure.storage.blob import BlockBlobService, PublicAccess  # noqa
from azure.identity import AuthorizationCodeCredential  # noqa
from azure.storage.blob import ContainerClient
from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa
from msal import token_cache  # noqa

from blob_procedure import Blob_Procedure

# configuration variables
KEY_ACCOUNT_NAME = 'account_name'
KEY_ACCOUNT_KEY = '#account_key'
KEY_CONTAINER_NAME = 'container_name'
KEY_NEW_FILE_ONLY = 'new_file_only'
# KEY_FILES = 'files'
KEY_FILE = 'file'
KEY_DEBUG = 'debug'

MANDATORY_PARS = [
    KEY_ACCOUNT_NAME,
    KEY_ACCOUNT_KEY,
    KEY_CONTAINER_NAME,
    KEY_FILE
]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.1.5'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        # Disabling list of libraries you want to output in the logger
        disable_libraries = [
            'azure.core.pipeline.policies.http_logging_policy'
        ]
        for library in disable_libraries:
            logging.getLogger(library).disabled = True

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def validate_config_params(self, params):
        '''
        Validating if input configuration contain everything needed
        '''

        # Credentials Conditions
        # Validate if config is blank
        if params == {}:
            logging.error(
                'Configurations are missing. Please configure your component.')
            sys.exit(1)

        # Validate if the configuration is empty
        empty_config = {
            'account_name': '',
            '#account_key': '',
            'container_name': ''
        }
        if params == empty_config:
            logging.error(
                'Configurations are missing. Please configure your component.')
            sys.exit(1)

        # Validating config parameters
        if params[KEY_ACCOUNT_NAME] == '' or params[KEY_ACCOUNT_KEY] == '':
            logging.error(
                "Credientials missing: Account Name, Access Key...")
            sys.exit(1)
        if params[KEY_CONTAINER_NAME] == '':
            logging.error(
                "Blob Container name is missing, check your configuration.")
            sys.exit(1)

    def refresh_token(self, refresh_token, client_id, client_secret):
        '''
        Refreshing Blob Account token
        '''

        logging.info('Authenticating...')
        request_url = 'https://login.microsoftonline.com/common/oauth2/token'
        request_body = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id,
            'client_secret': client_secret,
            'resource': 'https://storage.azure.com'
        }

        request = requests.post(url=request_url, data=request_body)

        if request.status_code in [200]:
            return request.json()['access_token']
        else:
            logging.error(
                f'Error in refreshing access token. Message: {request.json()}')

    def run(self):
        '''
        Main execution code
        '''

        params = self.cfg_params  # noqa
        self.validate_config_params(params)

        # Get proper list of tables
        now = int(datetime.datetime.utcnow().timestamp())
        account_name = params.get(KEY_ACCOUNT_NAME)
        account_key = params.get(KEY_ACCOUNT_KEY)  # noqa
        container_name = params.get(KEY_CONTAINER_NAME)
        file = params.get(KEY_FILE)
        new_file_only = params.get(KEY_FILE).get(KEY_NEW_FILE_ONLY)
        logging.debug(f'New File Only: {new_file_only}')
        account_url = '{}.blob.core.windows.net'.format(account_name)

        # Get state file
        state = self.get_state_file()  # noqa
        if state and new_file_only:
            last_run_timestamp = state.get('component').get('lastRunTimestamp')

        else:
            last_run_timestamp = None
            state = {}

        # OAuth Method
        authorization = self.get_authorization()
        auth_data = json.loads(authorization['#data'])
        # oauth_token = auth_data['access_token']
        refresh_token = auth_data['refresh_token'] if not state.get(
            '#refresh_token') else state.get('#refresh_token')
        active_directory_tenant_id = 'common'
        active_directory_application_id = authorization['appKey']
        active_directory_application_secret = authorization['#appSecret']

        access_token = self.refresh_token(
            refresh_token=refresh_token,
            client_id=active_directory_application_id,
            client_secret=active_directory_application_secret
        )

        # define scopes that are needed for azure app -> this is the same that was used to register the app
        # and the oAuth broker. It is needed for the refresh
        scope = ['https://storage.azure.com/.default']
        # create first synthetic event to store access_token and refresh_token from #data in Credentials cache
        # because this first step was done by the oAuth broker
        synth_first_event = {'response': {'access_token': access_token,
                                          'refresh_token': refresh_token},
                             'scope': scope}

        # add to cache
        pre_populated_cache = token_cache.TokenCache()
        pre_populated_cache.add(synth_first_event)

        # create auth_code credential with prepopulated cache -
        # we don't need auth_code and redirect url since its the first step
        token_credentials = AuthorizationCodeCredential(
            tenant_id=active_directory_tenant_id,
            client_id=active_directory_application_id,
            authorization_code='',
            redirect_uri='',
            client_secret=active_directory_application_secret,
            cache=pre_populated_cache
        )

        # Instantiate a BlobServiceClient using a token credential
        # from azure.storage.blob import BlobServiceClient
        # blob_service_client = BlobServiceClient(account_url="https://kbcblob.blob.core.windows.net",
        #                                         credential=token_credentials)
        # # [END create_blob_service_client_oauth]
        # # Get account information for the Blob Service
        # # account_info = blob_service_client.get_service_properties()

        # Initializing Blob Container client
        blob_container_client = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            # credential=account_key
            credential=token_credentials
        )

        # Processing blobs
        Blob_Procedure(
            blob_obj=blob_container_client,
            file=file,
            current_timestamp=now,
            DEFAULT_TABLE_SOURCE=f'{self.tables_in_path}/',
            DEFAULT_TABLE_DESTINATION=f'{self.files_out_path}/',
            last_run_timestamp=last_run_timestamp
        )

        # Updating State file
        # OAuth Piece
        state['#refresh_token'] = refresh_token

        if new_file_only:
            state['component'] = {}
            state['component']['lastRunTimestamp'] = now
            self.write_state_file(state)
            logging.debug('Latest State: {}'.format(state))

        logging.info("Blob Storage Extraction finished")


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True
    comp = Component(debug)
    comp.run()
