import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

from azure.storage.blob import StandardBlobTier

def return_blob_files(container_client, arg_date, std_date_format):
    start_date = datetime.strptime(
        arg_date, std_date_format).date() - timedelta(days=1)

    blob_files = [blob for blob in container_client.list_blobs(
    ) if blob.creation_time.date() >= start_date]

    return blob_files


def read_csv_to_dataframe(container_client, filename, file_delimiter=','):
    blob_client = container_client.get_blob_client(blob=filename)

    # Retrieve extract blob file
    blob_download = blob_client.download_blob()

    # Read blob file into DataFrame
    blob_data = StringIO(blob_download.content_as_text())
    df = pd.read_csv(blob_data, delimiter=file_delimiter)
    return df


def write_dataframe_to_datalake(df, datalake_service_client, filesystem_name, dir_name, filename):

    file_path = f'{dir_name}/{filename}'

    file_client = datalake_service_client.get_file_client(
        filesystem_name, file_path)

    processed_df = df.to_parquet(index=False)

    file_client.upload_data(
        data=processed_df, overwrite=True, length=len(processed_df))

    file_client.flush_data(len(processed_df))

    return True


def archive_cooltier_blob_file(blob_service_client, storage_account_url, source_container, archive_container, blob_list):

    for blob in blob_list:
        blob_name = blob.name
        source_blob_url = f'{storage_account_url}{source_container}/{blob_name}'

        # Copy source blob file to archive container and change blob access tier to 'Cool'
        archive_blob_client = blob_service_client.get_blob_client(
            archive_container, blob_name)

        archive_blob_client.start_copy_from_url(
            source_url=source_blob_url, standard_blob_tier=StandardBlobTier.Cool)

        (blob_service_client.get_blob_client(source_container,
         blob_name)).delete_blob(delete_snapshots='include')

    return True


def ingest_relational_data(container_client, blob_file_list):
    df = pd.concat([read_csv_to_dataframe(container_client=container_client,
                   filename=blob_name.name) for blob_name in blob_file_list], ignore_index=True)

    return df


def process_relational_data(df, columns, groupby_columns):
    # Remove leading and trailing whitespace in df column names
    processed_df = df.rename(columns=lambda x: x.strip())

    # Clean column names for easy consumption
    processed_df.columns = processed_df.columns.str.strip()
    processed_df.columns = processed_df.columns.str.lower()
    processed_df.columns = processed_df.columns.str.replace(' ', '_')
    processed_df.columns = processed_df.columns.str.replace('(', '')
    processed_df.columns = processed_df.columns.str.replace(')', '')

    # Filter DataFrame (df) columns
    processed_df = processed_df.loc[:, columns]

    # Filter out all empty rows, if they exist.
    processed_df.dropna(inplace=True)

    # Remove leading and trailing whitespace for all string values in df
    df_obj_cols = processed_df.select_dtypes(['object'])
    processed_df[df_obj_cols.columns] = df_obj_cols.apply(
        lambda x: x.str.strip())

    # Convert column to datetime: attempt to infer date format, return NA where conversion fails.
    processed_df['date'] = pd.to_datetime(
        processed_df['date'], infer_datetime_format=True, errors='coerce')

    # Convert object/string to numeric and handle special characters for each currency column
    processed_df['gross_sales'] = processed_df['gross_sales'].replace(
        {'\$': '', ',': ''}, regex=True).astype(float)

    # Capture dateparts (year and month) in new DataFrame columns
    processed_df['sale_year'] = pd.DatetimeIndex(processed_df['date']).year
    processed_df['sale_month'] = pd.DatetimeIndex(processed_df['date']).month

    # Get Gross Sales per Segment, Country, Sale Year, and Sale Month
    processed_df = processed_df.sort_values(by=['sale_year', 'sale_month']).groupby(
        groupby_columns, as_index=False).agg(total_units_sold=('units_sold', sum), total_gross_sales=('gross_sales', sum))

    return processed_df


def load_relational_data(processed_df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix):
    now = datetime.today().strftime("%Y%m%d_%H%M%S")
    processed_filename = f'{file_prefix}_{now}.{file_format}'
    write_dataframe_to_datalake(
        processed_df, datalake_service_client, filesystem_name, dir_name, processed_filename)
    return True


def run_cloud_etl(service_client, storage_account_url, source_container, archive_container, source_container_client, blob_file_list, columns, groupby_columns, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix):
    df = ingest_relational_data(source_container_client, blob_file_list)
    df = process_relational_data(df, columns, groupby_columns)
    result = load_relational_data(
        df, datalake_service_client, filesystem_name, dir_name, file_format, file_prefix)
    result = archive_cooltier_blob_file(
        service_client, storage_account_url, source_container, archive_container, blob_file_list)

    return result
