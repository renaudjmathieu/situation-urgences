import json
import logging
import os

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

from shared.cloudetl import (
    return_blob_files,
    run_cloud_etl
)

from shared.azure_credential import (
    get_azure_default_credential,
    get_azure_key_credential,
)
from shared.bing_search import get_news
from shared.hash import get_random_hash
from shared.key_vault_secret import get_key_vault_secret
from shared.datalake import upload_to_datalake
from shared.transform import clean_documents
from shared.ingest import ingest_from_api

app = func.FunctionApp()

@app.function_name(name="demo_relational_data_cloudetl")
@app.route(route="cloudetl")  # HTTP Trigger
def demo_relational_data_cloudetl(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Parameters/Configurations
    arg_date = '2014-07-01'
    std_date_format = '%Y-%m-%d'
    processed_file_format = 'parquet'
    processed_file_prefix = 'financial_demo'

    # List of columns relevant for analysis
    cols = ['segment', 'country', 'units_sold', 'gross_sales', 'date']

    # List of columns to aggregate
    groupby_cols = ['segment', 'country', 'sale_year', 'sale_month']

    try:
        # Set variables from appsettings configurations/Environment Variables.
        key_vault_name = os.environ["KEY_VAULT_RESOURCE_NAME"]
        key_vault_Uri = f"https://{key_vault_name}.vault.azure.net"
        abs_secret_name = os.environ["KEY_VAULT_SECRET_NAME_ABS"]
        adls_secret_name = os.environ["KEY_VAULT_SECRET_NAME_ADLS"]

        abs_acct_name = os.environ["ABS_RESOURCE_NAME"]
        abs_acct_url = f'https://{abs_acct_name}.blob.core.windows.net/'
        abs_container_name = os.environ["ABS_CONTAINER_NAME_INGEST"]
        archive_container_name = os.environ["ABS_CONTAINER_NAME_ARCHIVE"]

        adls_acct_name = os.environ["ADLS_RESOURCE_NAME"]
        adls_acct_url = f'https://{adls_acct_name}.dfs.core.windows.net/'
        adls_fsys_name = os.environ["ADLS_CONTAINER_NAME"]
        adls_dir_name = os.environ["ADLS_DIRECTORY_NAME"]

        # Authenticate and securely retrieve Key Vault secret for access key value.
        az_credential = DefaultAzureCredential(
            exclude_shared_token_cache_credential=True, exclude_visual_studio_code_credential=True)

        secret_client = SecretClient(
            vault_url=key_vault_Uri, credential=az_credential)
        access_key_secret = secret_client.get_secret(abs_secret_name)

        # Initialize Azure Service SDK Clients
        abs_service_client = BlobServiceClient(
            account_url=abs_acct_url,
            credential=az_credential
        )

        abs_container_client = abs_service_client.get_container_client(
            container=abs_container_name)

        adls_service_client = DataLakeServiceClient(
            account_url=adls_acct_url,
            credential=az_credential
        )

        # Run ETL Application
        process_file_list = return_blob_files(
            container_client=abs_container_client,
            arg_date=arg_date,
            std_date_format=std_date_format
        )

        run_cloud_etl(
            source_container_client=abs_container_client,
            blob_file_list=process_file_list,
            columns=cols,
            groupby_columns=groupby_cols,
            datalake_service_client=adls_service_client,
            filesystem_name=adls_fsys_name,
            dir_name=adls_dir_name,
            file_format=processed_file_format,
            file_prefix=processed_file_prefix,
            service_client=abs_service_client,
            storage_account_url=abs_acct_url,
            source_container=abs_container_name,
            archive_container=archive_container_name
        )

    except Exception as e:
        logging.info(e)

        return func.HttpResponse(
            f"!! This HTTP triggered function executed unsuccessfully. \n\t {e} ",
            status_code=200
        )

    return func.HttpResponse("This HTTP triggered function executed successfully.")


@app.function_name(name="get_data_timed")
@app.schedule(schedule="0 15 0-23 * * *", 
              arg_name="mytimer") 
def get_data_timed(mytimer: func.TimerRequest) -> None:
    url_1 = 'https://www.donneesquebec.ca/recherche/api/3/action/datastore_search?resource_id=a9272cc9-8234-40d1-9806-9f6b4c75c20d'  
    url_2 = 'https://www.donneesquebec.ca/recherche/api/3/action/datastore_search?resource_id=b256f87f-40ec-4c79-bdba-a23e9c50e741'

    filename_1 = 'Releve_horaire_urgences_7jours'
    filename_2 = 'Releve_horaire_urgences_7jours_nbpers'

    ingest_from_api(url_1, filename_1)
    ingest_from_api(url_2, filename_2)

    return 'true'

@app.function_name(name="get_data_manual")
@app.route(route="get_data_manual")  # HTTP Trigger
def get_data_manual(req: func.HttpRequest) -> func.HttpResponse:
    url_1 = 'https://www.donneesquebec.ca/recherche/api/3/action/datastore_search?resource_id=a9272cc9-8234-40d1-9806-9f6b4c75c20d'  
    url_2 = 'https://www.donneesquebec.ca/recherche/api/3/action/datastore_search?resource_id=b256f87f-40ec-4c79-bdba-a23e9c50e741'

    filename_1 = 'Releve_horaire_urgences_7jours'
    filename_2 = 'Releve_horaire_urgences_7jours_nbpers'

    ingest_from_api(url_1, filename_1)
    ingest_from_api(url_2, filename_2)

    return 'true'


@app.function_name(name="api_blob_trigger")
@app.blob_trigger(arg_name="myblob", path="msdocs-python-cloud-etl-news-source/{name}",
                  connection="AzureWebJobsStorage")
def test_function(myblob: func.InputStream):

    logging.info("Python blob trigger function processed blob \nName: %s \nBlob Size: %s bytes",
                 myblob.name, myblob.length)
                 
    # read the blob content as a string.
    search_results_blob_str = myblob.read()

    # decode the string to Unicode
    blob_json = search_results_blob_str.decode("utf-8")

    # parse a valid JSON string and convert it into a Python dict
    try:

        # Get environment variables
        datalake_account_name = os.environ.get("DATALAKE_GEN_2_RESOURCE_NAME")
        datalake_container_name = os.environ.get(
            "DATALAKE_GEN_2_CONTAINER_NAME")
        datalake_directory_name = os.environ.get(
            "DATALAKE_GEN_2_DIRECTORY_NAME")

        # Get Data
        data = json.loads(blob_json)

        # Clean Data
        new_data_dictionary = clean_documents(data)

        # Prepare to upload
        json_str = json.dumps(new_data_dictionary)
        file_name = myblob.name.split("/")[1]
        new_file_name = f"processed_{file_name}"

        # Get authentication to Azure
        azure_default_credential = get_azure_default_credential()

        # Upload to Data Lake
        upload_to_datalake(azure_default_credential, datalake_account_name,
                            datalake_container_name, datalake_directory_name, new_file_name, json_str)
        logging.info(
            "Successfully uploaded to data lake, old: %s, new: %s", myblob.name, new_file_name
        )

    except ValueError as err:
        logging.info(
            "Error converting %s to python dictionary: %s", myblob.name, err)
