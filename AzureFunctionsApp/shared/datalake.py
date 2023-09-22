# ./shared/datalake.py
import logging
import os
import datetime

from azure.storage.filedatalake import DataLakeServiceClient

# Upload the data to Azure Data Lake
# Required RBAC role - Storage Blob Data Owner
def upload_to_datalake(
    azure_credential,
    datalake_account_name,
    datalake_container_name,
    datalake_directory_name,
    file_name,
    data_str,
):

    # Get the client
    service_client = DataLakeServiceClient(
        account_url=f"https://{datalake_account_name}.dfs.core.windows.net",
        credential=azure_credential,
    )

    # Get the file system client
    file_system_client = service_client.get_file_system_client(file_system=datalake_container_name)

    # Get the directory client
    directory_client = file_system_client.get_directory_client(datalake_directory_name)

    # Get the file client
    file_client = directory_client.get_file_client(file_name)

    # Upload the data
    file_client.upload_data(data_str, overwrite=True)

    return file_name

def upload_df_to_datalake(
    azure_credential,
    datalake_account_name,
    datalake_container_name,
    datalake_directory_name,
    file_name,
    df,
):

    # get current year, month, day
    now = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    # Get the client
    service_client = DataLakeServiceClient(
        account_url=f"https://{datalake_account_name}.dfs.core.windows.net",
        credential=azure_credential,
    )

    # Get the file system client
    file_system_client = service_client.get_file_system_client(file_system=datalake_container_name)

    # Get the directory client
    directory_client = file_system_client.get_directory_client(datalake_directory_name)

    # create sub directory
    directory_client = directory_client.create_sub_directory(f"{file_name}")

    # create sub directories year=xxxx/month=xx/day=xx
    directory_client = directory_client.create_sub_directory(f"year={year}")
    directory_client = directory_client.create_sub_directory(f"month={month}")
    directory_client = directory_client.create_sub_directory(f"day={day}")

    file_name = f"{now.replace(microsecond=0, second=0, minute=0).isoformat()}.parquet"

    # Get the file client
    file_client = directory_client.get_file_client(file_name)

    # Upload the data
    file_client.upload_data(data=df.to_parquet(compression='gzip'), overwrite=True)

    return file_name
