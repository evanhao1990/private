"""
This job is built because We need a place where we can 1. access manually 2. access from TC for both PROD jobs and DEV jobs.
Before we figure out another way to do this I’m putting it in Redshift for our convenience.
(The original table is uploaded to Redshift manually.)
"""

from src.rs_connector import DatabaseRedshift
from src.aws_toolkit import s3Toolkit
from src.utils import read_sql, get_bucket_name
from src.configure_logging import configure_logging
util_logger = configure_logging(logger_name='src.utils', level='WARNING')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

S3_DESTINATION_KEY = 'tables/redshift/sovendus-commission/sovendus-commission.csv'
TABLE_NAME = 'sovendus-commission'


def main(env):

    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket)

    sql = ''' select * from sandbox_ana.sovendus_commission'''

    try:
        logger.info(f'Fetching {TABLE_NAME}')
        with DatabaseRedshift() as db:
            df = db.fetch(sql)
        if df.empty:
            logger.warning('Empty DataFrame')
    except Exception as e:
        logger.error(f'Fail to fetch {TABLE_NAME}. Exception: {e}')

    logger.info(f'Uploading to {S3_DESTINATION_KEY}')
    s3.upload_dataframe(df=df, delimiter=';', s3_key=S3_DESTINATION_KEY)
    logger.info('Upload suceeded')
