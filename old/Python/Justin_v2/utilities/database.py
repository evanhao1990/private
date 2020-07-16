import pandas as pd
from datetime import datetime
from dateutil import relativedelta
from sqlalchemy import create_engine
from sqlalchemy import exc # SQLAlchemyError is the default to catch all errors
import os
import argparse
from boto import kms
from binascii import unhexlify
from ConfigParser import RawConfigParser



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

    def __init__(self, env, verbose):
        self.env = env
        self.db_connection = None
        self.verbose = verbose

    def __enter__(self):
        self.db_connection = self.create_connection()
        return self

    def __exit__(self, *args):
        pass

    def _get_repo_path(self):
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

    def _get_sql_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'SQL')

    def create_connection(self, db_type='target'):
        """Opens a connection to the DB. """

        def decrypt(value):
            kms_obj = kms.connect_to_region(os.environ.get('AWS_DEFAULT_REGION'))
            try:
                plaintext = kms_obj.decrypt(unhexlify(value))['Plaintext']
            except Exception, e:
                raise Exception('Unable to decrypt value: ' + str(e))
            return plaintext

        config = RawConfigParser()
        config_file = os.path.join(self._get_repo_path(), 'config.ini')
        config.read(config_file)
        environment = self.env

        db_user = config.get(environment, 'db_user')
        db_password = decrypt(config.get(environment, 'db_password'))
        db_host = config.get(environment, 'db_host')
        db_name = config.get(environment, db_type + '_db_name')

        # db_user = os.environ['db_user']
        # db_password = os.environ['db_password']
        # db_host = '10.1.110.181'
        # db_name = 'sandbox'

        try:
            con = create_engine("mysql+mysqldb://%s:%s@%s:3306/%s?charset=utf8" %
                                (db_user, db_password, db_host, db_name))
            return con

        except exc.SQLAlchemyError, e:
            print "Error: %s" % e.args[0]
            raise e

    def _fetch(self, query, params=None):
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

    def _insert(self, table, schema, df, mode='fail'):
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

    def _execute(self, query, params={}, multi=False):
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
        con.execute(query, {'v':50})

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

class InteractWithDB(Database):

    def __init__(self, env, verbose=False):
        super(InteractWithDB, self).__init__(env, verbose)
        self.schema = "sandbox"

    def write_to_sandbox(self, df):
        """
        Write page scrape to sandbox.

        """

        self._insert(table='justin_v2', schema=self.schema, df=df, mode='append')