# ./shared/blob_storage.py
import logging
import os

from azure.storage.blob import BlobServiceClient

# Upload a pandas dataframe to Azure Blob Storage
def upload_df_to_blob(azure_credential, account_name, container_name, blob_name, df):

    logging.info("upload_to_blob account_name=%s", account_name)

    # Get the account URL
    account_url = f"https://{account_name}.blob.core.windows.net"

    # Create a client
    blob_service_client = BlobServiceClient(account_url, credential=azure_credential)

    output_file_dest = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    output_file_dest.upload_blob(data=df.to_csv(index=False, sep='|'), overwrite=True)

    return output_file_dest.url