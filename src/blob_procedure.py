import datetime
import logging
import sys
from pathlib import Path
from fnmatch2 import fnmatch2


class Blob_Procedure():

    def __init__(self, blob_obj,
                 file,
                 current_timestamp,
                 DEFAULT_TABLE_SOURCE="/data/in/tables/",
                 DEFAULT_TABLE_DESTINATION="/data/out/files/",
                 last_run_timestamp=None):
        self.blob_obj = blob_obj
        self.last_run_timestamp = last_run_timestamp
        self.current_timestamp = current_timestamp
        self.DEFAULT_TABLE_SOURCE = DEFAULT_TABLE_SOURCE
        self.DEFAULT_TABLE_DESTINATION = DEFAULT_TABLE_DESTINATION

        self.standard_process(file)

    def standard_process(self, file):
        try:
            all_blobs = list(self.blob_obj.list_blobs(retry_total=0))
        except Exception as err:
            logging.error(f'Error occured while listing blob files: {err}')
            logging.error('Please contact support.')
            sys.exit(1)

        # for file in files_to_download:
        logging.info('Processing [{}]...'.format(file['file_name']))
        blobs_to_download = self.qualify_files(
            file_pattern=file['file_name'], blob_files=all_blobs)

        # To add timestamp into the file or not
        add_timestamp_bool = file['add_timestamp'] if 'add_timestamp' in file else False

        # When there is nothing to download
        if len(blobs_to_download) == 0:
            return

        for _index, blob in enumerate(blobs_to_download):

            blob_full_filename = blob['name']
            blob_created_timestamp = blob['created_timestamp']
            logging.info(f'Downloading [{blob_full_filename}]...')

            # Blob Paths and filenames
            # blob_file_full_path needs to be relative path, otherwise the join will fail
            if blob_full_filename.startswith('/'):
                blob_full_filename = blob_full_filename.split(
                    '/', maxsplit=1)[1]

            output_dir_path = Path(self.DEFAULT_TABLE_DESTINATION)
            # blob_file_full_path = Path(blob_full_filename)
            blob_file_full_path = blob_full_filename.replace('/', '_')

            output_full_file_path = output_dir_path.joinpath(
                blob_file_full_path)

            # append unique id to file, because there can be blob and folder of a same name in one folder
            output_full_file_path = output_full_file_path.parent.joinpath(
                f'{_index}_{output_full_file_path.name}')

            # Append timestamp
            if add_timestamp_bool:
                output_full_file_path = output_full_file_path.parent.joinpath(
                    f'{blob_created_timestamp}_{output_full_file_path.name}')

            # Creating directory
            output_full_file_path.parent.mkdir(parents=True, exist_ok=True)

            logging.debug(f'{blob_file_full_path} --> {output_full_file_path}')

            # with open(output_destination, 'wb') as my_blob:
            with open(output_full_file_path, 'wb') as my_blob:
                blob_data = self.blob_obj.download_blob(
                    blob=blob_full_filename)
                blob_data.readinto(my_blob)

    def qualify_files(self, file_pattern, blob_files=None):
        '''
        Fetching BLOBs matches with GLOB configuration
        '''
        qualified_files = []
        qualified_files_short = []

        for blob in blob_files:

            blob_name = blob['name']
            blob_last_modified = datetime.datetime.timestamp(
                blob['last_modified'])
            blob_creation_date = datetime.datetime.timestamp(
                blob['creation_time'])

            match_bool = fnmatch2(blob_name, file_pattern)
            logging.debug(
                f'Matched: {match_bool} | {file_pattern} <> {blob_name}')

            if match_bool is True:
                blob_obj = {}
                if self.last_run_timestamp:
                    if int(blob_last_modified) >= int(self.last_run_timestamp):
                        logging.debug('New File: True')
                        # qualified_files += [blob_name]
                        blob_obj['name'] = blob_name
                        blob_obj['created_timestamp'] = blob_creation_date
                        qualified_files.append(blob_obj)
                        qualified_files_short.append(blob_name)

                    else:
                        logging.debug('New File: False')

                else:
                    # qualified_files += [blob_name]
                    blob_obj['name'] = blob_name
                    blob_obj['created_timestamp'] = int(blob_creation_date)
                    qualified_files.append(blob_obj)
                    qualified_files_short.append(blob_name)

        logging.info(f'Number of qualified blob files: {len(qualified_files)}')
        logging.debug(f'Qualified Files: {qualified_files_short}')

        return qualified_files
