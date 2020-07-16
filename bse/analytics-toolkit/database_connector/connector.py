from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
import sys
import psycopg2
from io import StringIO, BytesIO
import boto3
from collections import OrderedDict
from datetime import datetime
import decimal


class HandleS3(object):
    def __init__(self, bucket):
        self.bucket = bucket

    def transfer_to_s3(self, df, key, delimiter, index):
        """
        Transfer CSV from memory to S3.
        Uses Boto3 which will automatically search for AWS keys in env variables.

        Parameters
        ----------
        df : Dataframe
            Data to be transferred
        key : string
            s3 key (folder/filename.csv)
        delimiter : string
            Which delimiter to use
        index : bool
            Write the index or not

        Returns
        -------
        Transfers csv to S3

        """

        s3 = boto3.resource("s3")
        csv_buffer = StringIO()

        # Write df as in memory object
        df.to_csv(
            csv_buffer, index=index, sep=delimiter
        )  # csv_buffer object is the in memory df
        s3.Object(self.bucket, key).put(Key=key, Body=csv_buffer.getvalue())
        print(f"Saved {key} to {self.bucket}")

    def generate_s3_to_redshift_query(self, key, schema, table, delimiter):
        """
        Generates the S3 to redshift commands.

        Parameters
        ----------
        key : string
            S3 key (folder/filename)
        schema : string
            Schema name
        table : string
            Table name
        delimiter : string
            Delimiter to use

        Returns
        -------
        query : string
            COPY query to be passed into execute

        """

        # Keys
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        s3_location = f"s3://{self.bucket}/{key}"
        query = f"""
        COPY {schema}.{table}
        FROM '{s3_location}'
        credentials 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
        DELIMITER '{delimiter}'
        IGNOREHEADER 1
        EMPTYASNULL
        BLANKSASNULL
        """

        return query


class DatabaseRedshift(object):
    def __init__(
        self, bucket="bse-analytics-dev.bseint.io", key_prefix="dwh", verbose=False
    ):
        self.connection = None
        self.bucket = bucket
        self.key_prefix = key_prefix
        self.verbose = verbose

    def __enter__(self):
        self.connection = self.create_connection()
        return self

    def __exit__(self, *args):
        self.connection.close()

    def create_connection(self):
        """
        Create a connection to the database.

        Returns
        -------
        connection : connection class
            Handles the connection to a PostgresSQL database instance. It encapsulates a database session.

        """

        # Account settings
        db_user = os.environ.get("db_user_rs")
        db_password = os.environ.get("db_password_rs")
        db_host = os.environ.get("db_host_rs")
        db_name = "dwh"
        db_port = 5439

        # Connection
        connection = psycopg2.connect(
            dbname=db_name,
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
        )

        return connection

    def create_connection_pandas(self):
        """
        Create a connection that can be used by pandas.to_sql(). We will use this for small inserts.

        """

        db_user = os.environ.get("db_user_rs")
        db_password = os.environ.get("db_password_rs")
        db_host = os.environ.get("db_host_rs")
        db_name = "dwh"
        db_port = 5439
        connection_string = (
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )

        try:
            engine = create_engine(connection_string)
            return engine

        except Exception as e:
            raise e

    def _get_redshift_dtype(self, dtype):
        """
        Map pandas datatypes to redshift datatypes

        Parameters
        ----------
        dtype : string
            Pandas datatype string

        Returns
        -------
        dtype : string
            Redshift datatype string

        """

        if "int" in dtype:
            return "BIGINT"
        elif "float" in dtype:
            return "FLOAT"
        elif "datetime" in dtype:
            return "TIMESTAMP"
        elif "bool" in dtype:
            return "BOOLEAN"
        else:
            return "VARCHAR(1000)"

    def _get_converted_dtypes(self, df):
        """
        Converts pandas datatypes to redshift compatible datatypes.

        If a column contains only nan's, then by default the datatype of the column is `int`.

        Parameters
        ----------
        df : Dataframe
            Input dataframe

        Returns
        -------
        dtypes : list
            A list of Redshift datatypes

        """

        # All this does is find the first non-null value and get the datatype.
        df = df.reset_index(drop=True)  # make sure index is unique

        types_ = []

        for col in df.columns:
            series_ = df[col]

            # If column just contains NULLS, default datatype == float (type of np.nan)
            if series_.isnull().all():
                types_.append(type(np.nan))
            else:
                inferred_type = type(series_.loc[series_.first_valid_index()])
                types_.append(inferred_type)

        return [self._get_redshift_dtype(str(t)) for t in types_]

    def _get_columns_and_datatypes(self, df):
        """
        Takes dataframe columns and datatypes and maps to redshift datatypes.

        Parameters
        ----------
        df : Dataframe
            Input dataframe

        Returns
        -------
        columns_and_dtypes : OrderedDict
            An orderedDictionary of {column_names:dtype}

        """

        column_names = list(df.columns)
        column_dtypes = self._get_converted_dtypes(df)
        columns_and_dtypes = OrderedDict(
            {k: v for k, v in zip(column_names, column_dtypes)}
        )

        return columns_and_dtypes

    def _from_dict_to_string(self, d):
        """
        Converts dictionary keys, values to a single string, separated by commas.

        Parameters
        ----------
        d : dict
            Dictionary to be converted

        Returns
        -------
        s : string
            String expansion of the dictionary

        """

        s = ", ".join(["{} {}".format(k, v) for k, v in zip(d.keys(), d.values())])
        return s

    def _create_empty_table(self, df, schema, table, custom_column_dtypes, drop_table):
        """
        Creates an empty table according to column names and dtypes of input dataframe.

        Parameters
        ----------
        df : Dataframe
            Input dataframe used to get column names and dtypes
        schema : string
            Schema name
        table : string
            Table name
        custom_column_dtypes : dict
            Dictionary listing {column name:dtype} as the key, value pair. Will over-ride auto-detection.
        drop_table : bool
            Drop an existing table. Will replace with new empty table.

        Returns
        -------
        An empty table is created in the DWH

        """

        # Get column names and datatypes
        columns_and_dtypes = self._get_columns_and_datatypes(df)

        # Allow for custom dtypes -> update keys if custom dtypes are passed.
        if custom_column_dtypes:
            columns_and_dtypes.update(custom_column_dtypes)

        # Generate string
        cd_string = self._from_dict_to_string(columns_and_dtypes)

        # Create queries
        drop_query = "drop table if exists {}.{}".format(schema, table)
        create_query = "create table {0}.{1} ({2})".format(schema, table, cd_string)

        if self.verbose:
            print(f"Executing: {drop_query}")
            print(f"Executing: {create_query}")

        # Execute queries
        cursor = self.connection.cursor()
        if drop_table:
            cursor.execute(drop_query)
        cursor.execute(create_query)
        self.connection.commit()

        print("Table {}.{} created successfully".format(schema, table))

    def _generate_insert_query(self, df, schema, table):
        """
        Generate insert statements

        Parameters
        ----------
        df : Dataframe
            Input dataframe
        schema : string
            Schema name
        table : string

        Returns
        -------
        query : string
            A query string to be passed into execute()

        """

        values = ", ".join(map(str, [tuple(i) for i in df.values]))
        query = f"insert into {schema}.{table} values {values};"
        query = query.replace("nan", "NULL")  # nan's need to be nulls

        return query

    def _check_table_exists(self, schema, table):
        """
        Boolean function to check whether table exists.

        Parameters
        ----------
        schema : string
            Name of schema
        table : string
            Name of table

        Returns
        -------
        gate : Bool
            True / False table exists.

        """

        query = f"""
        select true where EXISTS (SELECT *
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}')
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        if not result:  # Will be None if False
            result = False
        else:
            result = list(result)[0]

        return result

    def _convert_decimal_to_float(self, df):
        """
        Change decimal.Decimal types into float, if they exist.

        Parameters
        ----------
        df : Dataframe
            Input dataframe

        Returns
        -------
        df : Dataframe
            Dataframe with fixed datatypes

        """

        if not df.empty:

            for col in df.select_dtypes([np.object]):  # select all objects
                type_ = type(
                    df[col].loc[df[col].first_valid_index()]
                )  # get type(first non-nan value)
                if type_ == decimal.Decimal:
                    df[col] = df[col].astype(float)

        return df

    def _check_duplicate_index(self, df):
        """
        Check to see whether dataframe index is unique. 
        
        This warning can be ignored if you expect a duplicated index.
        
        Parameters:
        -----------
        df : Dataframe
            Input dataframe
        """
        if True in set(df.index.duplicated()):
            print("Warning: dataframe index is not unique.")

    def _basic_insert(
        self, df, schema, table, mode, index, index_label, chunksize, dtype, method
    ):
        """
        Insert rows into table. Used when len(df) < 5000.

        Parameters
        ----------
        df : Dataframe
            Input dataframe
        schema : string
            Schema name
        table : string
            Table name
        mode : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.
            * fail: Raise an Error
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.
        index : bool, default True
            Write DataFrame index as a column. Uses `index_label` as the column
            name in the table.
        index_label : string or sequence, default None
            Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
            A sequence should be given if the DataFrame uses MultiIndex.
        chunksize : int, optional
            Rows will be written in batches of this size at a time. By default,
            all rows will be written at once.
        dtype : dict, optional
            Specifying the datatype for columns. The keys should be the column
            names and the values should be the SQLAlchemy types or strings for
            the sqlite3 legacy mode.
        method : {None, 'multi'}, default None
            Controls the SQL insertion clause used:
            * None : Uses standard SQL ``INSERT`` clause (one per row).
            * 'multi': Pass multiple values in a single ``INSERT`` clause.

        """
        engine = self.create_connection_pandas()
        connection = engine.connect()
        df.to_sql(
            name=table,
            con=connection,
            schema=schema,
            if_exists=mode,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=method,
        )

        connection.close()
        engine.dispose()
        print(f"Data written to {schema}.{table}")

    def _large_insert(self, df, schema, table, s3_csv_name, delimiter, index):
        """
        Wrapper function to run large inserts on rows > 5000.
        Utilizes S3->Redshift, which is handled in HandleS3().

        Parameters
        ----------
        df : Dataframe
            Input dataframe
        schema : string
            Schema name
        table : string
            Table name
        s3_csv_name : string
            Name of file sent to S3
        delimiter : string
            Delimiter to use

        Returns
        -------
        Runs a large insert.

        """

        if not s3_csv_name:
            s3_csv_name = datetime.now().strftime("%Y%m%d_%H%M%S") + "_df.csv"
            print(f"No name passed to `s3_csv_name` defaulting to {s3_csv_name}")

        s3 = HandleS3(bucket=self.bucket)
        key = f"{self.key_prefix}/{s3_csv_name}"
        s3.transfer_to_s3(df=df, key=key, delimiter=delimiter, index=index)
        query = s3.generate_s3_to_redshift_query(
            key=key, schema=schema, table=table, delimiter=delimiter
        )
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        print(f"Data written to {schema}.{table}")

    def create_empty_table(
        self,
        df=None,
        schema=None,
        table=None,
        custom_column_dtypes=None,
        drop_table=True,
    ):
        """
        Creates an empty table according to column names and dtypes of input dataframe.
        User wrapper ontop of internal function `_create_empty_table`

        Parameters
        ----------
        df : Dataframe
            Input dataframe used to get column names and dtypes
        schema : string
            Schema name
        table : string
            Table name
        custom_column_dtypes : dict
            Dictionary listing {column name:dtype} as the key, value pair. Will over-ride auto-detection.
        drop_table : bool
            Drop an existing table. Will replace with new empty table.

        Returns
        -------
        An empty table is created in the DWH

        """
        self._create_empty_table(
            df=df,
            schema=schema,
            table=table,
            custom_column_dtypes=custom_column_dtypes,
            drop_table=drop_table,
        )

    def fetch(self, query, params=None, batchsize=10000):
        """
        Fetch data from the DWH.

        Parameters
        ----------
        query : string
            Query string with optional
        params : dict
            Parameter dict {param:value}. Placeholders = %(param)s.
        batchsize : int
            Data is returned from DB in batches. How big do you want these batches to be?
            10000 is a good default.

        Returns
        -------
        df : Dataframe
            Results dataframe

        """

        cursor = self.connection.cursor(name="fetch_result")
        cursor.execute(query, vars=params)

        df = pd.DataFrame()

        while True:
            # consume result over a series of iterations
            records = cursor.fetchmany(size=batchsize)

            if not records:
                break

            rows = []
            for r in records:
                rows.append(r)

            df = df.append(rows)

        cols = [c[0] for c in cursor.description]

        if df.empty:
            df = pd.DataFrame(columns=cols)
        else:
            df.columns = cols
            df = df.reset_index(drop=True)

        df = df.replace({None: np.nan})  # Convert Nones to nans for consistency.
        df = self._convert_decimal_to_float(df)
        cursor.close()

        return df

    def insert(
        self,
        df,
        schema,
        table,
        mode="fail",
        s3_csv_name=None,
        delimiter="|",
        index=False,
        index_label=None,
        chunksize=None,
        custom_column_dtypes=None
    ):
        """
        Inserts data into the DWH. For large inserts, we utilize S3->Redshift.

        Parameters
        ----------
        df : Dataframe
            Data to be inserted
        schema : string
            Schema name
        table : string
            Table name
        mode : string
            Insert mode : ['fail', 'append', 'replace']
        s3_csv_name : string
            Name of the csv file sent to S3
        delimiter : string
            Which delimiter to use with CSV
        index : Bool
            Should the index be written aswell
        index_label : string
            If index=True, what to call the index column in the table. (Basic insert only).
        chunksize : int
            Chunks a dataframe and inserts by chunk. (Basic insert only. Should not be higher than 5000).
        custom_column_dtypes : dict
            Dictionary of {column:dtype} that will override auto-type detection.

        Returns
        -------
        Inserts data into DWH.

        """

        self._check_duplicate_index(df)

        if mode == "replace":

            if len(df) < 5000:
                self._basic_insert(
                    df=df,
                    schema=schema,
                    table=table,
                    mode=mode,
                    index=index,
                    index_label=index_label,
                    chunksize=chunksize,
                    dtype=custom_column_dtypes,
                    method="multi"
                )

            else:
                self._create_empty_table(
                    df=df,
                    schema=schema,
                    table=table,
                    custom_column_dtypes=custom_column_dtypes,
                    drop_table=True,
                )

                self._large_insert(
                    df=df,
                    schema=schema,
                    table=table,
                    s3_csv_name=s3_csv_name,
                    delimiter=delimiter,
                    index=index,
                )

        elif mode == "fail":

            if len(df) < 5000:
                self._basic_insert(
                    df=df,
                    schema=schema,
                    table=table,
                    mode=mode,
                    index=index,
                    index_label=index_label,
                    chunksize=chunksize,
                    dtype=custom_column_dtypes,
                    method="multi"
                )

            else:

                if self._check_table_exists(schema, table):
                    self.connection.close()
                    raise Exception(f"Table {schema}.{table} exists!")

                else:

                    self._create_empty_table(
                        df=df,
                        schema=schema,
                        table=table,
                        custom_column_dtypes=custom_column_dtypes,
                        drop_table=False,
                    )

                    self._large_insert(
                        df=df,
                        schema=schema,
                        table=table,
                        s3_csv_name=s3_csv_name,
                        delimiter=delimiter,
                        index=index,
                    )

        elif mode == "append":

            if chunksize is None:
                chunksize = 5000

            self._basic_insert(
                df=df,
                schema=schema,
                table=table,
                mode=mode,
                index=index,
                index_label=index_label,
                chunksize=chunksize,
                dtype=custom_column_dtypes,
                method="multi"
            )

        else:
            raise Exception(f"{mode} is not an option. Chose fail|replace|append.")

    def execute(self, query, params=None, multi=False):
        """
        For executing statements only.

        Parameters
        ----------
        query : string
            Query to execute
        params : dict
            Parameters to be passed to Query. Placeholder = %(param)s
        multi : Bool
            Multi-execute mode.

        Returns
        -------
        Executes queries only.

        """

        if multi:
            statements = query.replace("\n", "").split(";")
            statements = [s for s in statements if s not in ["", ";"]]
            cursor = self.connection.cursor()

            for s in statements:
                cursor.execute(s, vars=params)
                self.connection.commit()

        else:
            cursor = self.connection.cursor()
            cursor.execute(query, vars=params)
            self.connection.commit()
