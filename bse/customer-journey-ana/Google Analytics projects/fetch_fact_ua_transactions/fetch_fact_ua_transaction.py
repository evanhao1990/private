"""
This is a snippet for how to fetch fact_ua_transactions.
Data contains transactions for "A - Web, BC - Overview (3 -Non-User ID)" on 2020-01-20.
To be built in the future:
    -get view_ids from dim_ua_profile in s3
    -format filename with proper dates
"""

from src.ga_connector import GoogleAnalytics
from src.s3_toolkit import s3Toolkit
from src.configure_logging import configure_logging
logger = configure_logging(logger_name=__name__)


def get_bucket_name(env):
    """"
    Gets Bucket Name according to the chosen environment

    Parameters:
    ----------
        env : string
            dev or prod
    Returns:
    ----------
        bucket_name: string
    """

    return f"bse-cjdb-{env}.bseint.io"


def main(env):

    # input request
    view_id = '112804024'
    date_ranges = [['2020-01-20', '2020-01-20']]
    dimensions = ['ga:date', 'ga:source', 'ga:medium', 'ga:campaign', 'ga:deviceCategory', 'ga:channelGrouping',
                  'ga:transactionId']
    metrics = ['ga:transactions']

    # fetching data
    ga = GoogleAnalytics()
    logger.info(f'Fetching from {view_id}')  # in the future should be view name
    df = ga.fetch(view_id=view_id, dimensions=dimensions,  metrics=metrics, date_ranges=date_ranges)

    # insert into S3 bucket
    bucket = get_bucket_name(env)
    s3_key = 'tables/ga/fact-ua-transactions/fact_ua_transactions.csv'
    s3 = s3Toolkit(bucket=bucket)
    s3.upload_dataframe(df=df, s3_key=s3_key, delimiter=';')
    logger.info(f'{len(df)} records inserted in to {bucket}/{s3_key}')


if __name__ == "__main__":
    main('dev')
