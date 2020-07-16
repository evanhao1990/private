import mysql.connector
import os
import pandas as pd
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
    Depending on whether you are running a script on your local machine, or on the server, the env
    should be set as 'dev' (for local) 'prod' (for production). We do this usually by using argparse in
    main, so we don't have to hardcode this variable.

    """

    
    def __init__(self, env):
        self.env = env
        self.db_connection = None

    def __enter__(self):
        self.db_connection = self.create_connection()
        return self

    def __exit__(self, *args):
        self.db_connection.close()

    def get_repo_path(self):
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath( __file__))))

    def get_sql_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'SQL')

    def create_connection(self, db_type='target'):
        """
        Opens a connection to the DB

        Returns
        -------
        con : a database connection

        """

        def decrypt(value):
            kms_obj = kms.connect_to_region(os.environ.get('AWS_DEFAULT_REGION'))
            try:
                plaintext = kms_obj.decrypt(unhexlify(value))['Plaintext']
            except Exception, e:
                raise Exception('Unable to decrypt value: {}'.format(e))
            return plaintext

        config = RawConfigParser()
        config_file = os.path.join(self.get_repo_path(), 'config.ini')
        config.read(config_file)

        environment = self.env
        user = config.get(environment, 'db_user')
        password = decrypt(config.get(environment, 'db_password'))
        host = config.get(environment, 'db_host')
        name = config.get(environment, db_type + '_db_name')

        try:
            con = mysql.connector.connect(user=user, password=password, host=host, database=name)
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