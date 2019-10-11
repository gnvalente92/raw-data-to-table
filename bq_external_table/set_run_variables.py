def set_run_variables(json_config):
    """
    Method that sets the variables required for the GCP actions.

    :param json_config: The actions config json.
    :return: The log file location, the credentials file path,the dataset name, the GCP location of artifacts and a
    dictionary containing bucket and table names.
    """
    list_of_buckets = json_config.get("list_of_buckets")
    buckets_and_tables = {k: k for k in list_of_buckets}
    return json_config.get("log_location"), json_config.get("credentials_file_path"), json_config.get(
        "dataset_name"), buckets_and_tables
