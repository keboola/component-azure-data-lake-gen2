from azure.storage.filedatalake import DataLakeServiceClient


class AzureDataLakeClient():
    def __init__(self, account_name, account_key, file_system):
        service_client = self.initialize_storage_account(account_name, account_key)
        self.file_system_client = service_client.get_file_system_client(file_system=file_system)

    @staticmethod
    def initialize_storage_account(storage_account_name, storage_account_key):
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
        return service_client

    def list_directory_contents(self, directory=""):
        paths = self.file_system_client.get_paths(path=directory)
        return paths

    def download_file(self, directory, file_name, result_file):
        directory_client = self.file_system_client.get_directory_client(directory)
        out_file = open(result_file.full_path, 'wb')
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        out_file.write(downloaded_bytes)
        out_file.close()
