import boto3
import pandas as pd
import os, sys
import time
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Athena settings
DATABASE = 'kafka'
MAX_DURATION = 300
logger.info("DATABASE: {}".format(DATABASE))
logging.info("MAX_DURATION: {}".format(MAX_DURATION))

# S3 settings
S3_OUTPUT = 's3://bse-analytics-prod.bseint.io/customer-satisfaction'
logger.info("S3_OUTPUT: {}".format(S3_OUTPUT))


class QueryAthena(object):

    def __init__(self):
        pass


    def read_query(self):
        q_path = os.path.join('sql', 'athena-query-rory.sql')
        return open(q_path, 'r').read()


    def execute_query(self):
        """
        Exectute the query in Athena

        """

        logging.info("Reading query")
        query = self.read_query()

        # Start client
        client = boto3.client('athena')

        # Start query
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': S3_OUTPUT}
        )

        # Get query execution id
        query_execution_id = response['QueryExecutionId']
        logger.info("QueryExecutionId: {}".format(query_execution_id))

        start = time.time()
        i = 0
        while (time.time() - start) < MAX_DURATION:
            i += 1

            # Get query status
            query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
            query_execution_status = query_status['QueryExecution']['Status']['State']
            logger.info("STATUS: {}".format(query_status))

            if query_execution_status == 'SUCCEEDED':
                logger.info("STATUS: {}".format(query_execution_status))
                break

            if query_execution_status == 'FAILED':
                raise Exception('STATUS: {}'.format(query_execution_status), query_status)

            else:
                logger.info("ATTEMPT {} STATUS: {} ".format(i, query_execution_status))
                time.sleep(1)

        else:
            client.stop_query_execution(QueryExecutionId=query_execution_id)
            raise Exception('TIME OUT: Query ran longer than {}'.format(MAX_DURATION))

        # Get query results
        result = client.get_query_results(QueryExecutionId=query_execution_id)

        return result


    def results_to_df(self, results):
        """
        Convert results object to dataframe

        """

        logging.info("Turning results into DF")

        columns = [col['Label'] for col in
                   results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

        listed_results = []

        for res in results['ResultSet']['Rows'][1:]:
            values = []

            for field in res['Data']:
                try:
                    values.append(list(field.values())[0])
                except:
                    values.append(list(' '))

            listed_results.append(dict(zip(columns, values)))

        return pd.DataFrame(listed_results)


    def get_athena_result(self):
        """
        Exectute a query, return the results as df

        """

        result = self.execute_query()
        df = self.results_to_df(result)

        return df