import pandas as pd
import boto3
from boto3.s3.transfer import TransferConfig
import os
from src.configure_logging import configure_logging
from io import StringIO
import time
import datetime
logger = configure_logging(logger_name=__name__, handler=False)


class s3Toolkit(object):
    def __init__(
        self,
        bucket,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region_name=None,
    ):
        self.bucket = bucket
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.client = self.create_client()

    def create_client(self):
        """
        Creates an S3 client object.

        If `aws_access_key_id` `aws_secret_access_key` and `region_name` are None, default behaviour of Boto3
        is to read from environment variables, or ~/.aws/config.

        """

        client = boto3.client(
            service_name="s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )

        return client

    def _bucket_check(self, bucket):
        """
        Overrides the bucket parameter should the user want to use a different bucket inside one of the methods,
        without reinstantiating the class.

        Parameters
        ----------
        bucket : string
            S3 bucket name

        Returns
        -------
        bucket : string

        """
        if not bucket:
            bucket = self.bucket
        return bucket

    def list_directories(self, prefix, delimiter="/", bucket=None):
        """
        List all "directories" under a given search directory.
        Typically you will want to include the "/" i.e "myfolder/".

        Parameters
        ----------
        Prefix : string
            Which directory to search in.
        Delimiter : string
            Which delimiter to split
        Bucket : string
            Override the default bucket at class initialization

        Returns
        -------
        directories : list
            A list of all directories inside the search path

        """

        result = self.client.list_objects(
            Bucket=self._bucket_check(bucket), Prefix=prefix, Delimiter=delimiter
        )

        if "CommonPrefixes" in result.keys():
            directories = [k.get("Prefix") for k in result.get("CommonPrefixes")]
        else:
            directories = []

        return directories

    def list_files_in_directory(self, prefix, exclude_sub=False, bucket=None):
        """
        Will list all files under a given directory.
        By default this includes files inside subdirectories inside the search directory.

        Parameters
        ----------
        Prefix : string
            Which directory to search in.
        Delimiter : string
            Which delimiter to split
        Bucket : string
            Override the default bucket at class initialization

        Returns
        -------
        files : list
            A list of files

        """
        result = self.client.list_objects(
            Bucket=self._bucket_check(bucket), Prefix=prefix
        )

        if 'Contents' in result.keys():
            files = [k.get("Key") for k in result.get("Contents")]

        else:
            files = []

        if exclude_sub is True and 'Contents' in result.keys():
            len_filter = min([len(f.split("/")) for f in files])
            files = [f for f in files if len(f.split("/")) == len_filter]

        return files

    def download_file(self, s3_key, filename, bucket=None):
        """
        Download a file from S3 to local.

        Parameters
        ----------
        s3_key : string
            file location in S3
        filename : string
            file location on local machine

        """

        try:
            self.client.download_file(
                Bucket=self._bucket_check(bucket), Key=s3_key, Filename=filename
            )
            logger.info(f"Successfully downloaded {s3_key} to {filename}")
        except Exception as e:
            logger.error(f"Failed to download {s3_key} to {filename}: Exception {e}")

    def read_csv_from_s3(
        self, s3_key, bucket=None, encoding="utf-8", delimiter=";", header="infer"
    ):
        """
        Read a csv directly into dataframe from S3

        Parameters
        ----------
        s3_key: string
            File location in S3
        bucket : string
            Overide the default bucket at class initialization
        encoding : string
            Which decoder to use on the CSV
        delimiter : string
            Which delimiter to use in pd.read_csv()
        header : int, list of int, default ‘infer’
            None = no header
            'infer' = first row header.
            see pandas.read_csv() for full documentation.

        Returns
        -------
        df : dataframe
            Output dataframe

        """

        response = self.client.get_object(Bucket=self._bucket_check(bucket), Key=s3_key)
        stream = response["Body"]
        csv_string = stream.read().decode(encoding)
        df = pd.read_csv(StringIO(csv_string), sep=delimiter, header=header)

        return df

    def stream_csv_from_s3(
        self,
        s3_key,
        bucket=None,
        encoding="utf-8",
        delimiter=";",
        line_bytes=1024,
        header="infer",
    ):
        """
        Stream a CSV line by line into memory, then create dataframe.

        Ensures that the payload from AWS is always small, compared to `read_csv_from_s3` where entire payload is read at once.

        (Payload is the data transfer from S3 to machine.)

        Parameters
        ----------
        s3_key: string
            File location in S3
        bucket : string
            Overide the default bucket at class initialization
        encoding : string
            Which decoder to use on the CSV
        delimiter : string
            Which delimiter to use in pd.read_csv()
        line_bytes : int
            How many bytes at each line -> increase byte size if you have a lot of data per row.
        header : int, list of int, default ‘infer’
            None = no header
            'infer' = first row header.
            see pandas.read_csv() for full documentation.

        Returns
        -------
        df : Dataframe

        """

        response = self.client.get_object(Bucket=self._bucket_check(bucket), Key=s3_key)
        stream = response["Body"]
        lines = []
        for line in stream.iter_lines(chunk_size=line_bytes):
            lines.append(line.decode(encoding))

        csv_string = "\n".join([l for l in lines])
        df = pd.read_csv(StringIO(csv_string), sep=delimiter, header=header)

        return df

    def upload_file(self, filename, s3_key, bucket=None):
        """
        Upload a file to S3.


        Parameters
        ----------
        filename: string
            Local path to file
        bucket : string
            Overide the default bucket at class initialization
        s3_key : string
            File location in S3

        """

        self.client.upload_file(
            Filename=filename, Bucket=self._bucket_check(bucket), Key=s3_key
        )

    def upload_dataframe(self, df, s3_key, bucket=None, delimiter="|", index=False):

        try:
            csv_obj = StringIO()
            df.to_csv(csv_obj, sep=delimiter, index=index)
            self.client.put_object(
                Body=csv_obj.getvalue(), Bucket=self._bucket_check(bucket), Key=s3_key
            )

        except Exception as e:
            logger.error(f"Failed to write dataframe to {s3_key}. Exception : {e}")

    def multi_part_file_upload(self, filename, s3_key, bucket=None, max_concurrency=4):
        """
        Upload large file concurrently in multiple parts.

        Parameters
        ----------
        local_key : string
            Local key
        s3_key : string
            S3 location

        Returns
        -------

        """

        config = TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=max_concurrency,
            multipart_chunksize=1024 * 25,
            use_threads=True,
        )
        try:
            self.client.upload_file(
                Filename=filename,
                Bucket=self._bucket_check(bucket),
                Key=s3_key,
                Config=config,
            )

        except Exception as e:
            logger.error(f"Failed to upload file to {s3_key}. Exception: {e}")

    def move_object_in_s3(self, old_key, new_key, bucket=None):
        """
        Move object from one place to another.

        Parameters
        ----------
        old_key : string
            Old s3 location
        new_key :
            New s3 location

        """

        self.client.copy_object(
            Bucket=self._bucket_check(bucket),
            CopySource=f"{self._bucket_check(bucket)}/{old_key}",
            Key=new_key,
        )
        self.client.delete_object(Bucket=self._bucket_check(bucket), Key=old_key)

    def partition_table_upload(self, df, prefix):
        """
         Upload DataFrame to S3, folder partitioned by year/month/day.

         Parameters
         ----------
         df: DataFrame

         prefix: str
             S3 key of the parent folder, without "/"
         env: str

         Returns
         -------

         """
        # if the type of cells is date, we format them into string YYYY-MM-DD
        if type(df.date[0]) == datetime.date:
            df.date = df.date.apply(lambda x: x.strftime("%Y-%m-%d"))

        dates = set(df.date)
        for date in dates:

            df_date = df.loc[df['date'] == date].copy()
            year = date.split('-')[0]
            month = date.split('-')[1]
            day = date.split('-')[2]

            s3_key = f'{prefix}/year={year}/month={month}/day={day}/{year+month+day}.csv'
            try:
                self.upload_dataframe(df=df_date, s3_key=s3_key, delimiter=';')
                logger.info(f'Inserting {len(df_date)} records into {s3_key}')
            except Exception as e:
                logger.error(f'Fail to upload {s3_key}. Exception: {e}')


class AthenaToolkit(object):
    def __init__(
        self,
        bucket,
        database,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region_name=None,
    ):
        self.bucket = bucket
        self.database = database
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.output_location = f's3://{bucket}/athena_output'
        self.client = self.create_client()

    def create_client(self):
        """
        Creates an S3 client object.

        If `aws_access_key_id` `aws_secret_access_key` and `region_name` are None, default behaviour of Boto3
        is to read from environment variables, or ~/.aws/config.

        """

        client = boto3.client(
            service_name="athena",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )

        return client

    def _execute_query(self, query):
        """
        Execute query statement
        Parameters
        ----------
        query: str
            Athena query statement

        Returns
        -------
        QueryExecutionId: str
            Unique id for this execution
        """

        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.database
            },
            ResultConfiguration={
                'OutputLocation': self.output_location
            }
        )

        execution_id = response['QueryExecutionId']

        return execution_id

    def _get_result(self, execution_id):
        """
        Get query execution metadata and query status using the unique QueryExecutionId.
        If query status is "queued" or "running", we wait and check the status again until we have a final status of the query.

        Parameters
        ----------
        execution_id: str
            Athena QueryExecutionId
        Returns
        -------
            Execution metadata, execution status
        """

        status = 'none'
        n = 0
        while status not in ['SUCCEEDED', 'FAILED', 'CANCELLED'] and n <= 6:
            time.sleep(3**n)    # 1, 3, 9, 27, 81, 243, 729 seconds
            result = self.client.get_query_execution(QueryExecutionId=execution_id)
            status = result.get('QueryExecution', {}).get('Status', {}).get('State', 'N/A')
            n += 1
            logger.info(f'Current status: {status}')

        return result, status

    def _read_result(self, execution_id):
        """
        Read status, State change reason and Data processed from execution metadata.

        """
        result, status = self._get_result(execution_id)
        extra_info = result.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason', 'None')
        data_mb = result.get('QueryExecution', {}).get('Statistics', {}).get('DataScannedInBytes', '0')
        data_mb = round(int(data_mb)/2**20, 1)
        if status != 'SUCCEEDED':
            logger.warning(f'Query execution {status}. Data scanned {data_mb}MB. Extra info: {extra_info}')
        logger.info(f'Query execution {status}. Data scanned {data_mb}MB. Extra info: {extra_info}')
        # in case logger level is set to warning or logging is not used in script
        print(f'Query execution {status}. Data scanned {data_mb}MB. Extra info: {extra_info}')

    def execute(self, query):
        """
        Execute and log status, data scanned and if failed the reason of the failure.

        """
        execution_id = self._execute_query(query)
        self._read_result(execution_id)

    def fetch(self, query):
        """
        To be built in the future.

        """
        pass
