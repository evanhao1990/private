from google.oauth2 import service_account
import pandas_gbq
import os

class BigQuery(object):
	
	def __init__(self, project_id='ga-360-bigquery-api'):
		self.credentials = self.get_credentials()
		self.project_id = project_id
	
	def get_credentials(self):
	    """
	    get google keys from json file. Path of json file is stored in environment variables
	    """
	    # read path from environment variable
	    try:
	        key_path=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_BQ')
	    except Exception as e:
	        print ("No relevant environment variable(s) found")
	        raise

	    # read credentials from json file
	    try:
	        credentials = service_account.Credentials.from_service_account_file(
	            key_path
	        )
	    except Exception as e:
	        print ("Read google security keys failed")
	        raise

	    return credentials

	def fetch(self, sql):
	    """
	    fetch data from big query
	    
	    Parameter(s):
	    	sql (string): standard sql
	    
	    Return(s):
	    	df (dataframe): fetched data
	    """

	    # read data
	    try:
	        df = pandas_gbq.read_gbq(sql, project_id=self.project_id, credentials=self.credentials)
	    except Exception as e:
	        print ("data fetch failed due to {}".format(e))
	         
	    return df

	def write_to_bq(self, df, table, schema, mode='fail'):
		"""
		Write data to bigquery
		Parameters:
			df (dataframe): data
			table (string): name of table
			schema (string): data_tests or ANA_tables (those are our bigquery sandbox)
			mode （string): fail, replace or append
		"""
		destination_table = "{}.{}".format(schema, table)
	    
		try:
			pandas_gbq.to_gbq(df, destination_table, self.project_id,
                              if_exists=mode,credentials=self.credentials)
		except:
			print ('Write to BigQuery failed')
    

