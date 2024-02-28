from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import os
import pandas as pd
import pyarrow as pa
import io


tenant_id = "e24ac094-efd8-4a6b-98d5-a129b32a8c9a"
client_id = "ed478ce2-c14e-4eeb-b865-3c73a3cb786b"
client_secret = "SfD8Q~CJ1JJROP7wmeBLlfJyecdOqdBPcmjdUbmx"
storage_account_name = "sancopilot"
file_system_name="rawfiles"
directory_path ="/"

def create_service_client(storage_account_name, tenant_id, client_id, client_secret):
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=credential)
    return service_client

def get_directory_client(service_client, file_system_name, directory_path):
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client(directory_path)
    return directory_client

def list_files(file_system_client, directory_path):
    file_list = file_system_client.get_paths(path=directory_path)
    for file in file_list:
        print(file.name)

def list_all_in_file_system(service_client, file_system_name):
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    paths_list = file_system_client.get_paths()
    for path in paths_list:
        print(path.name)

def create_directory(service_client, file_system_name, directory_name):
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.create_directory(directory_name)

def upload_file_to_directory(service_client, file_system_name, directory_name, local_file_path, remote_file_name):
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.get_file_client(remote_file_name)
    
    with open(local_file_path, 'rb') as local_file:
        file_data = local_file.read()
        file_client.upload_data(file_data, overwrite=True)

def upload_multiple_files(service_client, file_system_name, directory_name, local_directory_path):
    for filename in os.listdir(local_directory_path):
        local_file_path = os.path.join(local_directory_path, filename)
        upload_file_to_directory(service_client, file_system_name, directory_name, local_file_path, filename)

def download_file_from_directory(service_client, file_system_name, directory_name, remote_file_name, local_file_path):
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.get_file_client(remote_file_name)
    
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    
    with open(local_file_path, 'wb') as local_file:
        local_file.write(downloaded_bytes)

def download_multiple_files(service_client, file_system_name, directory_name, remote_file_names, local_directory_path):
    for filename in remote_file_names:
        local_file_path = os.path.join(local_directory_path, filename)
        download_file_from_directory(service_client, file_system_name, directory_name, filename, local_file_path)

def read_csv_from_adls(service_client, file_system_name, directory_name, file_name):
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.get_file_client(file_name)
    
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    
    csv_data = pd.read_csv(io.BytesIO(downloaded_bytes))
    return csv_data

def write_parquet_to_adls(service_client, file_system_name, directory_name, file_name, data):
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.get_file_client(file_name)
    
    parquet_data = data.to_parquet()
    file_client.upload_data(parquet_data, overwrite=True)

def read_csv_uploadparquet_to_adls(service_client, file_system_name, source_directory_name, target_directory_name,remote_file_names):
    for filename in remote_file_names:
        csv_data = read_csv_from_adls(service_client, file_system_name=file_system_name, directory_name=source_directory_name, file_name=filename)
        write_parquet_to_adls(service_client, file_system_name, target_directory_name, filename, csv_data)

# Usage:
service_client = create_service_client(storage_account_name=storage_account_name, tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
#directory_client = get_directory_client(service_client, file_system_name=file_system_name, directory_path=directory_path)
file_system_client = service_client.get_file_system_client(file_system=file_system_name)
#list_all_in_file_system(service_client, file_system_name=file_system_name)
#create_directory(service_client, file_system_name="rawfiles", directory_name="utkarshCopilot")
#list_all_in_file_system(service_client, file_system_name=file_system_name)
#upload_file_to_directory(service_client, file_system_name=file_system_name, directory_name="utkarshCopilot", local_file_path="C:/Users/admin/Desktop/utkarsh_test_files/size.csv", remote_file_name="size.csv")
#list_all_in_file_system(service_client, file_system_name=file_system_name)
#upload_multiple_files(service_client, file_system_name=file_system_name, directory_name="utkarshCopilot", local_directory_path="C:/Users/admin/Desktop/utkarsh_test_files/")
list_all_in_file_system(service_client, file_system_name=file_system_name)
#remote_file_names = ["currency.csv", "size.csv"]  # replace with your actual file names
remote_file_names = ["data2.csv"]  # replace with your actual file names
#download_multiple_files(service_client, file_system_name=file_system_name, directory_name="/", remote_file_names=remote_file_names, local_directory_path="C:/Users/admin/Desktop/utkarsh_test_files/download/")
#csv_data = read_csv_from_adls(service_client, file_system_name=file_system_name, directory_name="/", file_name="data1.csv")
#write_parquet_to_adls(service_client, file_system_name=file_system_name, directory_name="utkarshCopilot", file_name="data1.parquet", data=csv_data)

read_csv_uploadparquet_to_adls(service_client, file_system_name=file_system_name,source_directory_name="/",target_directory_name="utkarshCopilot", remote_file_names=remote_file_names)