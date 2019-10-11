# BigQuery External Table Application

This application deals with data migration between GCP buckets and BigQuery external tables.

## Assumptions
 - This implementation requires that the user that will run the project has a GOOGLE_APPLICATION_CREDENTIALS json file with the appropriate permissions to run the required actions.
 
 ## Requirements for running the code
 
The developments were made under the assumption that there will be one run for each data set in BigQuery. For each data set, meaning each run, these are the steps for required:

 - Create a configuration JSON file. It is possible to find an example of this configuration file below and it is necessary to provide the following information:
 - credentials_file_path - The JSON file for the GCP service account.
 - dataset_name - The name of the data set to be populated with tables (each storage bucket will correspond to a BigQuery table). The data set needs to be created in advance.
 - list_of_buckets - list containing the bucket names to migrate to the selected dataset.
 - log_location - local log file for the application run.
 
## Running the code
The steps required to run the code are depicted below. For this, it is necessary to be in the project folder and have Python distribution and pip (the use of a Python virtual environment is recommended).

 - This step will generate a package with the code.
```shell
python setup.py sdist
```
 - Next, it is necessary to pip install the code. Because the requirements were configured, this step will bring up and install all the external packages required to run the code. 

```shell
pip install bq-external-table
```

 - Final step is to run the code with the desired JSON config file.

```shell
bq-external-table -j <PATH_TO_JSON_CONFIG_FILE>
```

