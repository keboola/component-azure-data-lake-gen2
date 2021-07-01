'''
Template Component main class.

'''

import logging
import sys

import logging_gelf.handlers  # noqa
import requests
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError, ClientAuthenticationError
# from azure.storage.blob import BlockBlobService, PublicAccess  # noqa
from azure.identity import AuthorizationCodeCredential  # noqa
from azure.storage.blob._shared.authentication import AzureSigningError
from azure.storage.filedatalake import DataLakeServiceClient
from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa
from msal import token_cache  # noqa

# configuration variables
KEY_ACCOUNT_NAME = 'account_name'
KEY_ACCOUNT_KEY = '#account_key'
KEY_CONTAINER_NAME = 'container_name'
KEY_NEW_FILE_ONLY = 'new_files_only'
KEY_FILE = 'file'
KEY_DEBUG = 'debug'
KEY_BLOB_DOMAIN = 'blob_domain'

MANDATORY_PARS = [
    KEY_ACCOUNT_NAME,
    KEY_ACCOUNT_KEY,
    KEY_CONTAINER_NAME,
    KEY_FILE
]
# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []



class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        '''
        Main execution code
        '''

        params = self.cfg_params  # noqa
        self.validate_config_params(params)

        # Get proper list of tables
        # now = int(datetime.datetime.utcnow().timestamp())
        account_name = params.get(KEY_ACCOUNT_NAME)
        account_key = params.get(KEY_ACCOUNT_KEY)  # noqa
        container_name = params.get(KEY_CONTAINER_NAME)
        file = params.get(KEY_FILE)

        # Validating user input file configuration
        if not file:
            logging.error('File configuration is missing.')
            sys.exit(1)

        # New file parameter if file configuration exists
        new_file_only = file.get(KEY_NEW_FILE_ONLY)
        logging.info(f'New File Only: {new_file_only}')

        # Get state file
        state = self.get_state_file()  # noqa
        if state and new_file_only:
            last_run_timestamp = state.get('lastRunTimestamp')
            logging.info(f'Extracting from: {last_run_timestamp}')

        # else:
        #     last_run_timestamp = None
        #     state = {}

        # Initializing Blob Container client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", account_name), credential=account_key)
        file_system_client = service_client.get_file_system_client(file_system=container_name)

        # Validating Credentials
        # Exceptions Handling
        try:
            paths = file_system_client.get_paths(max_results=10)
            logging.info(list(paths))
        except ClientAuthenticationError:
            # Credentials and Account Name validation
            logging.error(
                'The specified credentials [Account Name] & [Account Key] are invalid.')
            sys.exit(1)

        except ResourceNotFoundError as e:
            # Container validation
            logging.exception(e)
            sys.exit(1)
        except HttpResponseError as e:
            logging.exception(e)
            sys.exit(1)

        except Exception as e:
            # If there are any other errors, reach out support
            logging.exception(e)
            logging.error('Please validate your [Account Name].')
            sys.exit(1)


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






"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
