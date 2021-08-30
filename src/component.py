'''
Template Component main class.

'''

import logging
import ntpath
import dateparser
from fnmatch2 import fnmatch2
from datetime import datetime
from azure.core.exceptions import ServiceRequestError
from keboola.component.base import ComponentBase, UserException
from azure_data_lake.client import AzureDataLakeClient
from azure.storage.filedatalake._generated.models._models_py3 import StorageErrorException
import os

# configuration variables
KEY_ACCOUNT_NAME = 'account_name'
KEY_ACCOUNT_KEY = '#account_key'
KEY_FILE_SYSTEM = 'file_system'
KEY_NEW_FILE_ONLY = 'new_files_only'
KEY_FILE = 'file'
KEY_FILE_NAME = 'file_name'
KEY_ADD_TIMESTAMP = "add_timestamp"

REQUIRED_PARAMETERS = [KEY_ACCOUNT_NAME, KEY_ACCOUNT_KEY, KEY_FILE_SYSTEM, KEY_FILE]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters  # noqa

        account_name = params.get(KEY_ACCOUNT_NAME)
        account_key = params.get(KEY_ACCOUNT_KEY)  # noqa
        file_system = params.get(KEY_FILE_SYSTEM)
        file = params.get(KEY_FILE)
        file_pattern = file.get(KEY_FILE_NAME)
        add_timestamp = file.get(KEY_ADD_TIMESTAMP, False)

        # New file parameter if file configuration exists
        new_file_only = file.get(KEY_NEW_FILE_ONLY)
        logging.info(f'New File Only: {new_file_only}')

        # Get state file
        last_run_timestamp = 0
        state = self.get_state_file()  # noqa
        if state and new_file_only:
            last_run_timestamp = state.get('lastRunTimestamp', 0)
            logging.info(f'Extracting from: {last_run_timestamp}')

        # Initializing file Container client
        azure_client = AzureDataLakeClient(account_name, account_key, file_system)
        logging.getLogger("azure").setLevel(logging.WARNING)

        file_list = azure_client.list_directory_contents("")
        qualified_file_list = []
        try:
            qualified_file_list = self.qualify_files(file_pattern, file_list, last_run_timestamp)
        except StorageErrorException as e:
            if e.error.code == "AuthenticationFailed":
                raise UserException("Invalid client key, please recheck configuration") from e
            if e.error.code == "FilesystemNotFound":
                raise UserException("Invalid container name, please recheck configuration") from e
        except ServiceRequestError as e:
            raise UserException("Failed to connect, please check your Account Name in the configuration") from e

        for file in qualified_file_list:
            file_name = ntpath.basename(file["name"])
            out_file_name = file["name"].replace("/", "_")
            if add_timestamp:
                out_file_name = "_".join([str(file["last_modified"]), out_file_name])
            file_directory = ntpath.dirname(file["name"])
            out_file = self.create_out_file_definition(name=out_file_name)
            azure_client.download_file(file_directory, file_name, out_file)

        state = {'lastRunTimestamp': int(datetime.utcnow().timestamp())}
        self.write_state_file(state)

    @staticmethod
    def qualify_files(file_pattern, file_list, last_run_timestamp):
        qualified_files = []
        qualified_files_short = []

        for file in file_list:

            file_name = file.name
            file_last_modified = dateparser.parse(file.last_modified)
            file_last_modified = int(datetime.timestamp(file_last_modified))
            root, ext = os.path.splitext(file_name)

            match_bool = fnmatch2(file_name, file_pattern)
            logging.debug(
                f'Matched: {match_bool} | {file_pattern} <> {file_name}')

            if match_bool is True and ext:
                file_obj = {}
                if last_run_timestamp:
                    if file_last_modified >= int(last_run_timestamp):
                        logging.debug('New File: True')
                        file_obj['name'] = file_name
                        file_obj['last_modified'] = file_last_modified
                        qualified_files.append(file_obj)
                        qualified_files_short.append(file_name)

                    else:
                        logging.debug('New File: False')

                else:
                    # qualified_files += [file_name]
                    file_obj['name'] = file_name
                    file_obj['last_modified'] = file_last_modified
                    qualified_files.append(file_obj)
                    qualified_files_short.append(file_name)

        logging.info(f'Number of qualified file files: {len(qualified_files)}')
        logging.info(f'Qualified Files: {qualified_files_short}')

        return qualified_files


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
