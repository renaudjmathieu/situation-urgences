import logging
import os
import urllib
import json
import chardet
import pandas as pd

from azure.storage.blob import BlobServiceClient
from shared.datalake import upload_df_to_datalake
from shared.azure_credential import get_azure_default_credential
from shared.blob_storage import upload_df_to_blob

def ingest_from_api(url, filename):

    # Get environment variables
    datalake_account_name = os.environ.get("ADLS_RESOURCE_NAME")
    datalake_container_name = os.environ.get("ADLS_CONTAINER_NAME")
    datalake_directory_name = os.environ.get("ADLS_DIRECTORY_NAME_BRONZE")

    # Get authentication to Key Vault with environment variables
    azure_default_credential = get_azure_default_credential()

    # request the url
    fileobj = urllib.request.urlopen(url)
    
    # use chardet to detect the encoding
    result = chardet.detect(fileobj.read())

    # parse the url with the correct encoding
    df = pd.read_json(url, encoding=result['encoding'])

    # get the records from the json
    records = df['result']['records']

    # upload the records (dataframe) to blob storage
    blob_url = upload_df_to_datalake(
        azure_default_credential,
        datalake_account_name,
        datalake_container_name,
        datalake_directory_name,
        filename,
        pd.DataFrame(records)
    )
    logging.info("blob uploaded: %s", blob_url)
    return True