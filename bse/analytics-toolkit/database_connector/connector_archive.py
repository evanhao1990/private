from __future__ import print_function
## Monkey patch to speed up inserts using pandas
## https://github.com/pandas-dev/pandas/issues/8953
from pandas.io.sql import SQLTable
def _execute_insert(self, conn, keys, data_iter):
    # print "Using monkey-patched _execute_insert"
    data = [dict((k, v) for k, v in zip(keys, row)) for row in data_iter]
    conn.execute(self.insert_statement().values(data))
SQLTable._execute_insert = _execute_insert

from sqlalchemy import create_engine
from sqlalchemy.sql import text
import boto3
import psycopg2
from io import StringIO, BytesIO
import pandas as pd
import numpy as np
from collections import OrderedDict
import os
import sys


class DatabaseRedshift(object):
    """
    Collection of methods used to write large datasets to redshift. 

    Utilizes AWS infrastructure for fast inserts into DWH. Python -> S3 -> Redshift.

    Setup
    ------------------------------------
    You will need to request your dev-admin access credentials from vault: https://vault.bseint.io
    
    Store your credentials as environment variables. AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY.
    

    Example usage
    ------------------------------------
    with DatabaseRedshift() as db:
        df = db.fetch("select * from mart.dim_brand") # Fetch data
        db.insert(df=df, schema='sandbox_ana', table='example_1', s3_csv_name='example_1.csv') # Insert DF to DWH

    """

    def __init__(self, verbose=False):
        self.connection = None
        self.verbose = verbose

    def __enter__(self):
        self.connection = self.create_connection()
        return self

    def __exit__(self, *args):
        self.connection.close()

    def create_connection(self):
        """
        Create a connection to the database - note this is not an engine object, but a connection object.
        
        """
        
        # Account settings
        db_user = os.environ.get('db_user_rs')
        db_password = os.environ.get('db_password_rs')
        db_host = os.environ.get('db_host_rs')
        db_name = 'dwh'
        db_port = 5439
        
        # Connect
        connect = psycopg2.connect(dbname=db_name, host=db_host, port=db_port, user=db_user, password=db_password)

        return connect

    def get_redshift_dtype(self, dtype):
        """
        Align the datatypes between pandas and redshift.
        
        """

        if 'int' in dtype:
            return 'BIGINT'
        elif 'float' in dtype:
            return 'FLOAT'
        elif 'datetime' in dtype:
            return 'TIMESTAMP'
        elif 'bool' in dtype:
            return 'BOOLEAN'
        else:
            return 'VARCHAR(1000)'

    def get_converted_dtypes(self, df):
        """
        Get the datatypes of the dataframe, and convert to redshift datatype.

        """

        return [self.get_redshift_dtype(v.name) for v in df.dtypes.values]
 
    def create_empty_table(self, df, schema, table, custom_column_dtypes):
        """
        Creates the empty redshift table prior to inserting.

        custom_column_dtypes: dict
           can be used to pass custom datatypes to the query. All other columns not found in the dict will use auto-detect.
           e.g. {'a':'VARCHAR(250)', 'b':'NUMERIC(20,4)'}

        """

        # Get column names and datatypes -> create a dict with name:dtype as key:value pair
        column_names = list(df.columns)
        column_dtypes = self.get_converted_dtypes(df)
        columns_and_dtypes = {x:y for x, y in zip(column_names, column_dtypes)}

        # Allow for custom dtypes  -> update keys if custom dtypes are passed.
        if custom_column_dtypes:
            columns_and_dtypes.update(custom_column_dtypes)

        # Keep order of columns - so use OrderedDict and convert back to string
        cd_order = OrderedDict(columns_and_dtypes)
        columns_and_dtypes = ', '.join(['{} {}'.format(x, y) for x, y in zip(cd_order.keys(), cd_order.values())])

        # Create queries
        drop_query = "drop table if exists {}.{}".format(schema, table)
        create_query = "create table {0}.{1} ({2})".format(schema, table, columns_and_dtypes)

        # Execute queries
        cursor = self.connection.cursor()
        cursor.execute(drop_query)
        cursor.execute(create_query)
        self.connection.commit()

        print("Table {}.{} created successfully".format(schema, table))

    def transfer_to_s3(self, df, s3_csv_name, bucket='bse-analytics-dev.bseint.io', delimiter=';'):
        """
        Transfer dataframe to s3 bucket
        
        """
        
        s3 = boto3.resource('s3')

        if sys.version_info.major == 2:
            # Compatibility issue with python 2 and 3
            csv_buffer = BytesIO()
        else:
            csv_buffer = StringIO()

        # Write df as in memory object
        df.to_csv(csv_buffer, index=False, sep=delimiter)  # csv_buffer object is the in memory df
        s3.Object(bucket, 'dwh/{}'.format(s3_csv_name)).put(
            Key="dwh/{}".format(s3_csv_name), Body=csv_buffer.getvalue());
        print("Saved {} to {}/dwh/".format(s3_csv_name, bucket))

    def s3_to_redshift(self, s3_filepath, schema, table, delimiter=';'):
        """
        Transfer file from s3 bucket to redshift

        """

        # S3 settings - use vault to request temporary credentials for 1 month at a time.
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        
        # SQL command
        sql = """
        COPY {0}.{1}
        FROM '{2}'
        credentials 'aws_access_key_id={3};aws_secret_access_key={4}'
        DELIMITER '{5}'
        IGNOREHEADER 1
        """.format(schema, table, s3_filepath, aws_access_key_id, aws_secret_access_key, delimiter)

        # Create cursor and execute
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit() # Commit all changes
        
        print("Data written to {}.{}".format(schema, table))

    def insert(self, df, schema, table, s3_csv_name, bucket='bse-analytics-dev.bseint.io', delimiter=';', custom_column_dtypes=None):
        """
        Inserts data from python into redshift. Intermediate storage in S3.

        df: DataFrame
        schema: 'sandbox_ana', 'sandbox_dev', 'sandbox_prod'
        table: name of redshift table inserting into
        s3_csv_name: name of csv to be stored in S3
        bucket: S3 bucket
        delimiter: csv delimiter
        custom_column_dtypes:
            overwrite automatic datatype detection with your own datatypes.
            Pass a dictionary with col_name:datatype as key,value pair. Any columns not found in this dict still
            get the auto-detection applied.

            e.g. {'a':'VARCHAR(250)', 'b':'NUMERIC(20,4)'}

        """

        # Create the s3_filepath
        s3_filepath = "s3://{}/{}/{}".format(bucket, 'dwh', s3_csv_name)

        # Execute all steps
        self.create_empty_table(df=df, schema=schema, table=table, custom_column_dtypes=custom_column_dtypes)
        self.transfer_to_s3(df=df, s3_csv_name=s3_csv_name, bucket=bucket, delimiter=delimiter)
        self.s3_to_redshift(s3_filepath=s3_filepath, schema=schema, table=table, delimiter=delimiter)

    def fetch(self, query, params=None):
        """
        Fetch data from the DWH

        """

        cursor = self.connection.cursor()
        cursor.execute(query, vars=params)
        data = list(cursor.fetchall())
        cols = [c[0] for c in cursor.description]
        df = pd.DataFrame(data, columns=cols)

        return df

    def execute(self, query, params=None, multi=False):
        """
        Execute statements only.

        """

        if multi:
            statements = query.replace('\n', '').split(';')
            statements = [s for s in statements if s not in ['', ';']]
            cursor = self.connection.cursor()

            for s in statements:
                cursor.execute(s, vars=params)
                self.connection.commit()

        else:
            cursor = self.connection.cursor()
            cursor.execute(query, vars=params)
            self.connection.commit()

    def redshift_to_s3(self, query, params=None, filename=None, bucket='bse-analytics-dev.bseint.io', delimiter=';', parallel='OFF'):
        """
        Transfers data directly from redshift to S3.

        query: the select statement
        params: parameter dict to pass to the query. Specifier= %(key)s
        filename: the filename to write to s3
        bucket: S3 bucket name
        delimiter: ';' for csv
        parallel: ON/OFF should the file be processed in parallel or not.

        If parallel is turned ON your data will be written to mutliple files.

        If parallel is turned OFF (default) your data will be written to a single file
        so long that the file is <6.5gb. In the case it is greater, AWS will automatically
        start writing splits of that file.

        """

        def escape_quotes(q):
            return q.replace("\'", "\\'")

        if not filename:
            print("Please specify the filename inside S3: e.g. data.csv")
            return 1

        # Get AWS credentials
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        # Create s3 path
        s3_filepath = "s3://{}/{}/{}".format(bucket, 'uploads', filename)

        # Create query
        unload_query = """
        UNLOAD ('{0}') 
        TO '{1}'
        CREDENTIALS 'aws_access_key_id={2};aws_secret_access_key={3}'
        DELIMITER '{4}'
        PARALLEL {5}
        ALLOWOVERWRITE

        """.format(escape_quotes(query), s3_filepath,
            aws_access_key_id, aws_secret_access_key, delimiter, parallel)

        cursor = self.connection.cursor()
        cursor.execute(unload_query, vars=params)
        print("{} uploaded to {}".format(filename, s3_filepath))



class Database(object):
    """
    A collection of methods to write small datasets (<10,000 rows) to redshift.

    Writes directly to the redshift without an intermediate step in S3. 

    Usage
    --------------------
    with Database() as db:
        df = db.fetch("select * from mart.dim_brand")
        db.insert(df=df, schema='sandbox', table='example_1', mode='fail')

    modes = ['fail', 'append', 'replace']

    """

    def __init__(self, verbose=False):
        self.db_connection = None
        self.db_engine = None
        self.verbose = verbose

    def __enter__(self):
        self.db_engine = self.create_engine()
        self.db_connection = self.create_connection()
        return self

    def __exit__(self, *args):
        self.db_connection.close()
        self.db_engine.dispose()

    def create_engine(self):
        """
        First create and engine object. We can then dispose of this object aswell as closing connection,
        to avoid "too many connections error"

        """

        db_user = os.environ.get('db_user_rs')
        db_password = os.environ.get('db_password_rs')
        db_host = os.environ.get('db_host_rs')
        db_name = 'dwh'
        db_port = 5439
        connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_password,
                                                                 db_host, db_port, db_name)

        try:
            engine = create_engine(connection_string)
            return engine

        except Exception as e:
            raise e

    def create_connection(self):
        """
        Opens a connection to the DB

        Returns
        -------
        con : a database connection

        """

        try:
            con = self.db_engine.connect()
            return con

        except Exception as e:
            raise e

    def fetch(self, query, params=None):
        """
        Fetch data from the DWH. Use this when your sql query has rows to return.

        Parameters
        ----------
        query  : string
                 A SQL query string (parametrize using %(param)s
        params : dictionary
                 Parameters to be passed to the query string.

        """

        con = self.db_connection
        df = pd.read_sql(sql=query, con=con, params=params, parse_dates=True)

        return df

    def insert(self, df, schema, table, mode='fail'):
        """
        Insert data into a database table

        mode types = 'fail', 'append', 'replace'


        Parameters
        ----------
        table  : string
                 Table name
        schema : string
                 schema - (sandbox_dev, sandbox_prod)
        df     : a pandas dataframe
                 A dataframe of results to be written to the DB
        mode   : string
                 insert mode
        """

        con = self.db_connection
        df.to_sql(name=table, schema=schema, con=con, if_exists=mode, index=False)

        if self.verbose:
            print("Insert to {}.{} success".format(schema, table))

    def execute(self, query, params={}, multi=False):
        """
        Execute a query. Use this when your sql query has no rows to return., i.e table creation.

        Parameters
        ----------
        query  : string
                 A SQL query string.
        params : dictionary
                 Parameters to be passed to the query string.

        Notes
        -----
        statements can be paramterized using :key where key is the dict key.

        query = "create table sandbox_dev.example_table (my_column varchar(:s))"
        con.execute(text(query), {'s':50})

        """

        def _remove_trailing_semi(query):
            """
            Removes the final semi-colon in a query, if it exists

            Returns a string if single statement, returns list if multi.

            """

            query_split = query.split(';')

            if len(query_split) == 1:
                return query_split[0]
            else:
                if not query_split[-1]:
                    query_split.pop(-1)
                return query_split

        con = self.db_connection
        query = _remove_trailing_semi(query)

        if multi:
            for statement in query:
                con.execute(text(statement), params)
        else:
            con.execute(text(query), params)

        if self.verbose:
            print("Execution success")