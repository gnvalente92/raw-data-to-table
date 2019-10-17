from bq_external_table import gcp_interfacer
from bq_external_table.set_run_variables import set_run_variables
from bq_external_table.utils import logger, utils_functions
from bq_external_table.utils import args_parser

log = logger.get_logger()


def main():
    arguments = args_parser.parse_arguments()
    log_location, credentials_file_path, dataset_name, buckets = set_run_variables(
        json_config=utils_functions.load_json_config(path=arguments.json_config))
    logger.setup_logger(log_location)
    gcp_interfacer.authenticate_gcp(credentials_file_path=credentials_file_path)
    bigquery_client = gcp_interfacer.get_bigquery_client()
    for bucket, blobs in buckets.items():
        for blob_details in blobs:
            gcp_interfacer.load_bucket_data_into_bigquery_external_table(bigquery_client=bigquery_client,
                                                                         dataset_name=dataset_name,
                                                                         bucket_name=bucket,
                                                                         table_id=blob_details.get("table_name"),
                                                                         blob_prefix=blob_details.get("blob_prefix"))
