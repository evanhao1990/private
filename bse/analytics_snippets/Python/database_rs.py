from sqlalchemy import create_engine
from sqlalchemy.sql import text
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

    with Database() as db:
        # Fetch
        query = "select * from mart.fact_orderline fo limit %(l)s"
        df = db.fetch(query=query, params={'l':10})

        # Insert
        db.insert(table='example', schema='sandbox_dev', df=df, mode='append')

        # Execute
        query_1 = "create table sandbox_dev.example_1 (my_column varchar(%(v)s)""
        db.execute(query=query_1, params={'v':50})

    """

    def __init__(self, verbose=False):
        self.db_connection = None
        self.verbose = verbose

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

        db_user = os.environ.get('db_user_rs')
        db_password = os.environ.get('db_password_rs')
        db_host = os.environ.get('db_host_rs')
        db_name = 'dwh'
        db_port = 5439
        connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_password,
                                                                 db_host, db_port, db_name)

        try:
            engine = create_engine(connection_string)
            con = engine.connect()

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

    def insert(self, table, schema, df, mode='fail'):
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
            print "Insert to {}.{} success".format(schema, table)

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

        query = "create table sandbox_dev.example_table (my_column varchar(%(v)s))"
        con.execute(text(query), {'v':50})

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
                con.execute(statement, params)
        else:
            con.execute(query, params)

        if self.verbose:
            print "Execution success"

