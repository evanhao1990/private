from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

CHANNEL = 'awin'


def process_df(df):
    """
    Transforms the input dataframe:
    - strip column names
    - drop unnecessary columns and empty rows

    Parameters:
    ----------
    df: dataframe
        input dataframe from funnel exports
    """
    df.columns = [x.lower().replace('__awin', '') for x in df.columns]
    df = df[-(df['order_reference'].isnull())]
    df.drop(['date'], axis=1, inplace=True)
    df['transaction_timestamp'] = df.transaction_timestamp.str.split('T').str[0]
    df['validation_timestamp'] = df.validation_timestamp.str.split('T').str[0]
    df.rename(columns={'transaction_timestamp': 'date', 'validation_timestamp': 'validation_date'}, inplace=True)

    return df


def main(env, days):

    funnel_wrapper(env, days, CHANNEL, process_df)
    update_table_partition(env, table_name='fact_awin_cost')
    logger.info("Done!")
