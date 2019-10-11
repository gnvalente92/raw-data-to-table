import os
import unittest
import uuid

import google as google
from google.cloud import storage

from bq_external_table import gcp_interfacer


class TestGCPInterfacer(unittest.TestCase):

    def setUp(self):
        self.credentials_file_path = os.path.abspath(
            "bq_external_table/resources/credentials/hsbc-gv-sandbox-bcab4fc6e84a.json")
        unique_id = uuid.uuid4()
        self.bucket_name = "hsbc-storage-bucket-{}".format(unique_id)
        self.dataset_name = "hsbc_dataset_{}".format(unique_id).replace("-", "_")
        self.table_name = "hsbc_table_{}".format(unique_id).replace("-", "_")
        gcp_interfacer.authenticate_gcp(credentials_file_path=self.credentials_file_path)
        self.bigquery_client = gcp_interfacer.get_bigquery_client()

    def test_credentials_file_path(self):
        expected = self.credentials_file_path
        actual = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        self.assertEqual(expected, actual)

    def test_create_gcp_storage_bucket(self):
        gcp_interfacer.create_gcp_storage_bucket(bucket_name=self.bucket_name)
        storage_client = storage.Client()
        actual = storage_client.get_bucket(bucket_or_name=self.bucket_name)
        gcp_interfacer.delete_gcp_storage_bucket(bucket_name=self.bucket_name)
        self.assertIsNotNone(actual)

    def test_delete_gcp_storage_bucket(self):
        gcp_interfacer.create_gcp_storage_bucket(bucket_name=self.bucket_name)
        gcp_interfacer.delete_gcp_storage_bucket(bucket_name=self.bucket_name)
        storage_client = storage.Client()
        self.assertRaises(google.api_core.exceptions.NotFound, storage_client.get_bucket, self.bucket_name)

    def test_upload_blob(self):
        gcp_interfacer.create_gcp_storage_bucket(bucket_name=self.bucket_name)
        destination_blob_name = "user_data.csv"
        source_file_name = "/".join([os.path.abspath("integration_tests/resources/csv_data/"), destination_blob_name])
        actual = gcp_interfacer.upload_blob(bucket_name=self.bucket_name,
                                            source_file_name=source_file_name,
                                            destination_blob_name=destination_blob_name)
        gcp_interfacer.delete_blob(bucket_name=self.bucket_name, blob_name=destination_blob_name)
        gcp_interfacer.delete_gcp_storage_bucket(bucket_name=self.bucket_name)
        self.assertIsNone(actual)

    def test_list_blobs(self):
        gcp_interfacer.create_gcp_storage_bucket(bucket_name=self.bucket_name)
        destination_blob_name = "user_data.csv"
        source_file_name = "/".join([os.path.abspath("integration_tests/resources/csv_data/"), destination_blob_name])
        gcp_interfacer.upload_blob(bucket_name=self.bucket_name,
                                   source_file_name=source_file_name,
                                   destination_blob_name=destination_blob_name)
        actual = gcp_interfacer.get_list_of_blobs(bucket_name=self.bucket_name)
        gcp_interfacer.delete_blob(bucket_name=self.bucket_name, blob_name=destination_blob_name)
        gcp_interfacer.delete_gcp_storage_bucket(bucket_name=self.bucket_name)
        expected = [destination_blob_name]
        self.assertEqual(expected, actual)

    def test_delete_blob(self):
        gcp_interfacer.create_gcp_storage_bucket(bucket_name=self.bucket_name)
        destination_blob_name = "user_data.csv"
        source_file_name = "/".join(
            [os.path.abspath("integration_tests/resources/csv_data/"), destination_blob_name])
        gcp_interfacer.upload_blob(bucket_name=self.bucket_name,
                                   source_file_name=source_file_name,
                                   destination_blob_name=destination_blob_name)
        actual = gcp_interfacer.delete_blob(bucket_name=self.bucket_name, blob_name=destination_blob_name)
        gcp_interfacer.delete_gcp_storage_bucket(bucket_name=self.bucket_name)
        self.assertIsNone(actual)

    def test_get_bigquery_client(self):
        actual = gcp_interfacer.get_bigquery_client()
        self.assertIsNotNone(actual)

    def test_create_bigquery_dataset(self):
        gcp_interfacer.create_bigquery_dataset(bigquery_client=self.bigquery_client,
                                               dataset_name=self.dataset_name,
                                               dataset_location="US")
        actual = self.bigquery_client.get_dataset(dataset_ref=self.dataset_name)
        gcp_interfacer.delete_bigquery_dataset(bigquery_client=self.bigquery_client, dataset_name=self.dataset_name)
        self.assertIsNotNone(actual)

    def test_load_bucket_data_into_bigquery_external_table_using_csv_data(self):
        gcp_interfacer.authenticate_gcp(credentials_file_path=self.credentials_file_path)
        gcp_interfacer.create_gcp_storage_bucket(bucket_name=self.bucket_name)
        destination_blob_name = "user_data.csv"
        source_file_name = "/".join([os.path.abspath("integration_tests/resources/csv_data/"), destination_blob_name])
        gcp_interfacer.upload_blob(bucket_name=self.bucket_name,
                                   source_file_name=source_file_name,
                                   destination_blob_name=destination_blob_name)

        gcp_interfacer.create_bigquery_dataset(bigquery_client=self.bigquery_client,
                                               dataset_name=self.dataset_name,
                                               dataset_location="US")

        actual = gcp_interfacer.load_bucket_data_into_bigquery_external_table(bigquery_client=self.bigquery_client,
                                                                              dataset_name=self.dataset_name,
                                                                              bucket_name=self.bucket_name,
                                                                              table_id=self.table_name)
        self.assertIsNone(actual)
        gcp_interfacer.delete_blob(bucket_name=self.bucket_name, blob_name=destination_blob_name)
        gcp_interfacer.delete_gcp_storage_bucket(bucket_name=self.bucket_name)
        gcp_interfacer.delete_bigquery_dataset(bigquery_client=self.bigquery_client, dataset_name=self.dataset_name)

    def test_load_bucket_data_into_bigquery_external_table_using_avro_data(self):
        gcp_interfacer.authenticate_gcp(credentials_file_path=self.credentials_file_path)
        gcp_interfacer.create_gcp_storage_bucket(bucket_name=self.bucket_name)
        destination_blob_name_1 = "userdata1.avro"
        destination_blob_name_2 = "userdata2.avro"
        source_file_name_1 = "/".join(
            [os.path.abspath("integration_tests/resources/avro_data/"), destination_blob_name_1])
        gcp_interfacer.upload_blob(bucket_name=self.bucket_name,
                                   source_file_name=source_file_name_1,
                                   destination_blob_name=destination_blob_name_1)

        source_file_name_2 = "/".join(
            [os.path.abspath("integration_tests/resources/avro_data/"), destination_blob_name_2])
        gcp_interfacer.upload_blob(bucket_name=self.bucket_name,
                                   source_file_name=source_file_name_2,
                                   destination_blob_name=destination_blob_name_2)

        gcp_interfacer.create_bigquery_dataset(bigquery_client=self.bigquery_client,
                                               dataset_name=self.dataset_name,
                                               dataset_location="US")

        actual = gcp_interfacer.load_bucket_data_into_bigquery_external_table(bigquery_client=self.bigquery_client,
                                                                              dataset_name=self.dataset_name,
                                                                              bucket_name=self.bucket_name,
                                                                              table_id=self.table_name)
        self.assertIsNone(actual)
        gcp_interfacer.delete_blob(bucket_name=self.bucket_name, blob_name=destination_blob_name_1)
        gcp_interfacer.delete_blob(bucket_name=self.bucket_name, blob_name=destination_blob_name_2)
        gcp_interfacer.delete_gcp_storage_bucket(bucket_name=self.bucket_name)
        gcp_interfacer.delete_bigquery_dataset(bigquery_client=self.bigquery_client, dataset_name=self.dataset_name)


if __name__ == '__main__':
    unittest.main()
