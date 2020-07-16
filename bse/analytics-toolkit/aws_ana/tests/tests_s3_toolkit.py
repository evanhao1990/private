import os
import sys
import unittest
import pandas as pd
import boto3
from io import StringIO
from moto import mock_s3

# Set directories
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.join(ROOT_DIR, 'tests')
FIXTURES_DIR = os.path.join(TESTS_DIR, 'fixtures')

# Append root to path
sys.path.append(ROOT_DIR)
from s3_toolkit import s3Toolkit

BUCKET = "my_test_bucket.io"

@mock_s3
class Tests3Toolkit(unittest.TestCase):
    def setUp(self):
        s3 = s3Toolkit(
            bucket=BUCKET,
            aws_access_key_id="fake_key",
            aws_secret_access_key="fake_secret",
        )
        client = s3.client

        # Make fake bucket
        client.create_bucket(Bucket=BUCKET)

        # Upload files to fake bucket
        client.upload_file(
            Filename=os.path.join(FIXTURES_DIR, "example.csv"),
            Bucket=BUCKET,
            Key="test_downloads/example.csv",
        )
        client.upload_file(
            Filename=os.path.join(FIXTURES_DIR, "example.json"),
            Bucket=BUCKET,
            Key="test_downloads/example.json",
        )
        client.upload_file(
            Filename=os.path.join(FIXTURES_DIR, "example.csv"),
            Bucket=BUCKET,
            Key="test_downloads/test_sub_folder/example.csv",
        )
        client.upload_file(
            Filename=os.path.join(FIXTURES_DIR, "example.json"),
            Bucket=BUCKET,
            Key="test_downloads/test_sub_folder/example.json",
        )

        # Assign class to access methods for testing
        self.s3 = s3

    def tearDown(self):
        s3 = boto3.resource(
            "s3", aws_access_key_id="fake_key", aws_secret_access_key="fake_secret"
        )
        bucket = s3.Bucket(BUCKET)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

    def test_list_directories(self):
        top_level = self.s3.list_directories(prefix="", delimiter="/")
        sub_level = self.s3.list_directories(prefix="test_downloads/", delimiter="/")
        self.assertListEqual(
            top_level + sub_level,
            ["test_downloads/", "test_downloads/test_sub_folder/"],
        )

    def test_list_all_files(self):
        expected = [
            "test_downloads/example.csv",
            "test_downloads/example.json",
            "test_downloads/test_sub_folder/example.csv",
            "test_downloads/test_sub_folder/example.json",
        ]
        actual = self.s3.list_files_in_directory(prefix="test_downloads/")
        self.assertListEqual(expected, actual)

    def test_list_all_files_exclude_sub(self):
        expected = ["test_downloads/example.csv", "test_downloads/example.json"]
        actual = self.s3.list_files_in_directory(
            prefix="test_downloads/", exclude_sub=True
        )
        self.assertListEqual(expected, actual)

    def test_list_all_files_exclude_sub_2(self):
        expected = [
            "test_downloads/test_sub_folder/example.csv",
            "test_downloads/test_sub_folder/example.json",
        ]
        actual = self.s3.list_files_in_directory(
            prefix="test_downloads/test_sub_folder/", exclude_sub=True
        )
        self.assertListEqual(expected, actual)

    def test_list_all_files_exclude_sub_3(self):
        expected = [
            "test_downloads/test_sub_folder/example.csv",
            "test_downloads/test_sub_folder/example.json",
        ]
        actual = self.s3.list_files_in_directory(
            prefix="test_downloads/test_sub_folder/", exclude_sub=False
        )
        self.assertListEqual(expected, actual)

    def test_read_csv_from_s3(self):
        actual = self.s3.read_csv_from_s3(
            s3_key="test_downloads/example.csv", delimiter="|"
        )
        expected = pd.read_csv(os.path.join(FIXTURES_DIR, "example.csv"), sep="|")
        pd.testing.assert_frame_equal(actual, expected)

    def test_stream_csv_from_s3(self):
        actual = self.s3.stream_csv_from_s3(
            s3_key="test_downloads/example.csv", delimiter="|"
        )
        expected = pd.read_csv(os.path.join(FIXTURES_DIR, "example.csv"), sep="|")
        pd.testing.assert_frame_equal(actual, expected)

    def test_upload_file(self):
        self.s3.upload_file(
            filename=os.path.join(FIXTURES_DIR, "example.json"),
            s3_key="test_uploads/example.json",
        )
        try:
            response = self.s3.client.get_object(
                Bucket=BUCKET, Key="test_uploads/example.json"
            )
            success = 1
        except:
            success = 0

        self.assertEqual(success, 1)

    def test_multipart_upload(self):
        self.s3.multi_part_file_upload(
            filename=os.path.join(FIXTURES_DIR, "example.csv"),
            s3_key="test_uploads/example.csv",
        )
        try:
            response = self.s3.client.get_object(
                Bucket=BUCKET, Key="test_uploads/example.csv"
            )
            success = 1
        except:
            success = 0

        self.assertEqual(success, 1)

    def test_upload_dataframe(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4]})
        self.s3.upload_dataframe(
            df=df, s3_key="test_uploads/example_df.csv", delimiter="|", index=False
        )

        # Read uploaded csv
        csv_obj = self.s3.client.get_object(
            Bucket=BUCKET, Key="test_uploads/example_df.csv"
        )
        body = csv_obj["Body"]
        csv_string = body.read().decode("utf-8")
        df_actual = pd.read_csv(StringIO(csv_string), sep="|")

        pd.testing.assert_frame_equal(df, df_actual)


if __name__ == "__main__":
    unittest.main()
