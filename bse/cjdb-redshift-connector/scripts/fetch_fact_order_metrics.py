from src.rs_connector import DatabaseRedshift
from src.aws_toolkit import s3Toolkit
from src.utils import read_sql, get_bucket_name, update_table_partition
from src.configure_logging import configure_logging
util_logger = configure_logging(logger_name='src.utils', level='WARNING')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

S3_DESTINATION_KEY = 'tables/redshift/fact-order-metrics'
TABLE_NAME = 'fact-order-metrics'


def main(env, days):

    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket)

    sql = read_sql(filename='fact_order_metrics.sql', days=days)

    try:
        logger.info(f'Fetching {TABLE_NAME}')
        with DatabaseRedshift() as db:
            df = db.fetch(sql)
        if df.empty:
            logger.warning('Empty DataFrame')
    except Exception as e:
        logger.error(f'Fail to fetch {TABLE_NAME}. Exception: {e}')

    logger.info(f'Uploading to {S3_DESTINATION_KEY}')
    s3.partition_table_upload(df=df, prefix=S3_DESTINATION_KEY)
    logger.info('Upload suceeded, adding table partition')

    update_table_partition(env, table_name='fact_order_metrics')