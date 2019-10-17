import os

from google.cloud import storage
from google.cloud import bigquery

from bq_external_table.utils import logger

log = logger.get_logger()


def authenticate_gcp(credentials_file_path):
    """
    Method that sets the environment variable for the GCP application credentials.

    :param credentials_file_path: The path for the credentials file path.
    """
    log.info("Authenticating into GCP.")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file_path


def get_bigquery_client():
    """
    Method that gets the GCP Big Query client object.

    :return: The GCP Big Query client object
    """
    log.info("Getting BigQuery client instance.")
    return bigquery.Client()


def create_gcp_storage_bucket(bucket_name):
    """
    Method that creates a GCP storage bucket.

    :param bucket_name: The name of the GCP bucket.
    """
    storage_client = storage.Client()
    bucket = storage_client.create_bucket(bucket_name)
    log.info('Bucket {} created'.format(bucket.name))


def delete_gcp_storage_bucket(bucket_name):
    """
    Method that deletes a GCP storage bucket. Bucket must be empty.

    :param bucket_name: The name of the GCP bucket.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()
    log.info('Bucket {} deleted'.format(bucket.name))


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """
    Method that uploads objects to Cloud Storage bucket

    :param bucket_name: The name of the GCP bucket.
    :param destination_blob_name: The name of the destination blob.
    :param source_file_name: The name of the source file.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    log.info('File {} uploaded to {}.'.format(source_file_name, destination_blob_name))


def get_list_of_blobs(bucket_name, blob_prefix):
    """
    Method that lists objects in a Cloud Storage bucket, according to a certain prefix, used for filtering.

    :param bucket_name: The name of the GCP bucket.
    :param blob_prefix: The string prefix used to filter blobs to load.
    :return The list of objects in the bucket.
    """
    return [blob.name for blob in storage.Client().list_blobs(bucket_name) if blob.name.startswith(blob_prefix)]


def delete_blob(bucket_name, blob_name):
    """
    Method that deletes objects in Cloud Storage bucket

    :param bucket_name: The name of the GCP bucket.
    :param blob_name: The name of the blob.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    log.info('Blob {} deleted.'.format(blob_name))


def create_bigquery_dataset(bigquery_client, dataset_name, dataset_location):
    """
    Method that creates a GCP BigQuery data set.

    :param bigquery_client: The GCP BigQuery client.
    :param dataset_name: The dataset name.
    :param dataset_location: The data set location.
    """
    dataset_id = get_dataset_id(bigquery_client=bigquery_client, dataset_name=dataset_name)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = dataset_location
    dataset = bigquery_client.create_dataset(dataset)
    log.info("Created dataset {}.{}".format(bigquery_client.project, dataset.dataset_id))


def get_dataset_id(bigquery_client, dataset_name):
    """
    Method that gets the GCP BigQuery dataset id, from its name.

    :param bigquery_client: The GCP BigQuery client.
    :param dataset_name: The dataset name.
    :return: The data set id.
    """
    return "{}.{}".format(bigquery_client.project, dataset_name)


def delete_bigquery_dataset(bigquery_client, dataset_name):
    """
    Method that deletes a GCP BigQuery data set.

    :param bigquery_client: The GCP BigQuery client.
    :param dataset_name: The data set name.
    """
    dataset_id = get_dataset_id(bigquery_client=bigquery_client, dataset_name=dataset_name)
    bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    log.info("Deleted dataset '{}'.".format(dataset_id))


def load_bucket_data_into_bigquery_external_table(bigquery_client, dataset_name, bucket_name, table_id, blob_prefix):
    """
    Method that loads raw data of files with a prefix from a GCP storage bucket into a GCP BigQuery external table.

    :param bigquery_client: The GCP BigQuery client.
    :param dataset_name: The name of the data set.
    :param bucket_name: The name of the bucket.
    :param table_id: The table id.
    :param blob_prefix: The string prefix used to select the blobs to load.
    """
    dataset_ref = bigquery_client.dataset(dataset_name)
    job_config = bigquery.LoadJobConfig()
    list_of_blobs = get_list_of_blobs(bucket_name=bucket_name, blob_prefix=blob_prefix)
    if get_blob_type(list_of_blobs) == "CSV":
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True
    else:
        job_config.source_format = bigquery.SourceFormat.AVRO

    for blob in list_of_blobs:
        load_job(bigquery_client=bigquery_client,
                 bucket_name=bucket_name,
                 file_name=blob,
                 dataset_ref=dataset_ref,
                 job_config=job_config,
                 table_id=table_id)


def get_blob_type(list_of_blobs):
    """
    Method that gets the type of objects in a bucket.

    :param list_of_blobs: The list with all the objects in a bucket.
    :return: The bucket object type.
    """
    return [blob.split(".")[-1] for blob in list_of_blobs][0].upper()


def load_job(bigquery_client, bucket_name, file_name, dataset_ref, job_config, table_id):
    """
    Method that runs a GCP BigQuery load job.

    :param bigquery_client: The GCP BigQuery client.
    :param bucket_name: The bucket name.
    :param file_name: The file name.
    :param dataset_ref: The data set reference.
    :param job_config: The load job config.
    :param table_id: The table id.
    """
    uri = "gs://{}/{}".format(bucket_name, file_name)
    load_job_details = bigquery_client.load_table_from_uri(uri, dataset_ref.table(table_id), job_config=job_config)
    log.info("Starting job {}".format(load_job_details.job_id))
    load_job_details.result()
    log.info("Job finished.")
    destination_table = bigquery_client.get_table(dataset_ref.table(table_id))
    log.info("Loaded {} rows.".format(destination_table.num_rows))
