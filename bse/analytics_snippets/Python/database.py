import mysql.connector
import pandas as pd
import os


class Database(object):
    """
    A wrapper class for connecting to the database in analyses projects.

    Design
    ------
    We make this class a context manager, by defining __enter__ and __exit__ functions.
    This is to ensure that only one connection is opened, and that when we are finished with it,
    it is closed. This is enabled using the with statement in python.

    Usage
    -----
    For simple queries:

    with Database() as db:
        query = "select * from mart.fact_orderline fo limit 10"
        df = db.fetch(query=query)


    For complex queries:

    with Database() as db:
        query = "set @d1 = %(run_date)s;
                 select dd.DimDateID from mart.dim_date as dd where dd.DimDate = @d1;"

        df = fb.fetch(query=query, params={'run_date':'2018-05-01'}, multi=True)

    """

    def __init__(self):
        self.db_connection = None

    def __enter__(self):
        self.db_connection = self.create_connection()
        return self

    def __exit__(self, *args):
        self.db_connection.close()

    def create_connection(self):
        """
        Opens a connection to the DB

        Returns
        -------
        con : a database connection

        """

        db_user = os.environ.get('db_user')
        db_password = os.environ.get('db_password')
        db_host = os.environ.get('db_host')
        db_name = 'mart'

        try:
            con = mysql.connector.connect(user=db_user, password=db_password, host=db_host,
                                          database=db_name)
            return con

        except mysql.connector.Error as e:
            raise e

    def decode(self, df):
        """
        Decodes bytearrays to strings.
        For compatibility between python 2.0 & 3.0, connector returns strings as bytearrays.

        Returns
        -------
        df : a decoded dataframe.

        """

        if not df.empty:
            df = df.applymap(lambda x: x.decode('utf-8') if isinstance(x, bytearray) else x)

        return df

    def fetch(self, query, params=None, multi=False):
        """
        Fetch data from the DWH. Use this when your sql query has rows to return.

        Parameters
        ----------
        query  : string
                 A SQL query string
        params : dictionary
                 Parameters to be passed to the query string.
        multi  : Bool
                 Specify whether the query has single, or multiple statements.

        """

        con = self.db_connection
        cursor = con.cursor()

        if multi:
            for statement in cursor.execute(query, params=params, multi=True): pass
        else:
            cursor.execute(query, params=params)

        rows = cursor.fetchall()
        cols = [i[0] for i in cursor.description]
        df = pd.DataFrame(data=rows, columns=cols)
        df = self.decode(df)

        return df

    def insert(self, table, df):
        """
        Insert data into a database table

        Note: Column names in dataframe cannot have spaces.

        Parameters
        ----------
        table  : string
                 Table name
        df     : a pandas dataframe
                 A dataframe of results to be written to the DB

        """

        con = self.db_connection
        cursor = con.cursor()

        # Create strings for query
        columns = df.columns.tolist()
        cols_string = ', '.join(columns)
        values_string = ', '.join(['%s' for s in range(len(columns))])

        query = """
            INSERT INTO sandbox.{} ({}) 
            VALUES ({})
            """.format(table, cols_string, values_string)

        try:
            cursor.executemany(query, df.values.tolist())
            con.commit()

        except mysql.connector.Error as e:
            raise e

    def execute(self, query, params=None, multi=False):
        """
        Execute a query. Use this when your sql query has no rows to return., i.e table creation.

        Parameters
        ----------
        query  : string
                 A SQL query string.
        params : dictionary
                 Parameters to be passed to the query string.
        multi  : Bool
                 Specify whether the query has single, or multiple statements.

        """

        con = self.db_connection
        cursor = con.cursor()

        if multi:
            for statement in cursor.execute(query, params=params, multi=True): pass
        else:
            cursor.execute(query, params=params)

        con.commit()

        print "Execution success"
