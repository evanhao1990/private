from datetime import datetime
from src.aws_toolkit import s3Toolkit
from src.aws_toolkit import AthenaToolkit
from src.configure_logging import configure_logging
import pandas as pd
import numpy as np
import time
from dateutil.relativedelta import relativedelta
from definitions import *
logger = configure_logging(logger_name=__name__, handler=False)


def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def generate_days_keys(channel, days):
    """
    Generate 1. Required date range . 2. s3 file keys with corresponding month number

    Parameters
    ----------
    channel: string
        Name of the data source.

    days: int or list
        int: Number of days we want to back fill,starting from yesterday.
        list: A list of two elements:[start_date, end_date], with date formatted as yyyy-mm-dd.

    Returns
    -------
    date_range: list
        A list of two elements:[start_date, end_date], with date formatted as yyyy-mm-dd.

    keys: list
        A list of s3 file keys.
    """

    if len(days) == 1 and days[0].isdigit():
        yesterday = datetime.today() - relativedelta(days=1)
        end_date = yesterday
        x_days = abs(int(days[0]))

    elif len(days) == 2:
        try:
            dates = [datetime.strptime(x, "%Y-%m-%d") for x in days]
            end_date = max(dates)
            x_days = (end_date - min(dates)).days + 1

        except:
            logger.error('Variable "Days" can only be an integer or two dates')
            exit()

    else:
        logger.error('Variable "Days" can only be an integer or two dates')
        exit()

    day_list = [(end_date - relativedelta(days=int(n))).strftime("%Y-%m-%d") for n in np.arange(x_days)]
    date_range = [min(day_list), max(day_list)]
    months = set([x[:7] for x in day_list])
    prefix = f'funnel-import/{channel}/{channel}-'
    keys = [prefix+month+'.csv' for month in months]

    return date_range, keys


def funnel_wrapper(env, days, channel, func):
    """
    A function wraps the similar read and write processes when handling funnel data.

    Parameters
    ----------
    env: string
        Environment
    days: int
        Number of days we want to back fill.
    channel: string
        Name of the data source.
    func: function
        function to process the data from different data source.

    Returns
    -------

    """

    s3 = s3Toolkit(get_bucket_name(env))
    date_range, keys = generate_days_keys(channel=channel, days=days)

    for key in keys:
        t0 = time.time()
        filename = key.split('/')[-1]
        path = os.path.join(DATA_DIR, filename)
        logger.info(f'Processing {key}')
        s3.download_file(s3_key=key, filename=path)
        try:
            rules = lambda x: x not in ['Connection_type_code', 'Connection_id', 'Currency']
            df_source = pd.read_csv(path, sep=',', usecols=rules)
        except Exception as e:
            logger.warning(f'Fail to read {filename}. Exception: {e}')
            df_source = pd.DataFrame()

        t1 = time.time()
        t_read = round(t1 - t0, 1)

        if len(df_source) == 0:
            logger.warning('Empty DataFrame!')
            continue

        logger.debug('Processing DataFrame')
        df_final = df_source.loc[df_source['Date'].between(date_range[0], date_range[1])].copy()
        df_final = func(df_final)
        t2 = time.time()
        t_processing = round(t2 - t1, 1)

        logger.debug('Uploading to S3')
        s3.partition_table_upload(df_final, prefix=f'tables/funnel/{channel}')
        t3 = time.time()
        t_upload = round(t3 - t2, 1)

        logger.info(f'Done. Download: {t_read}s, data process: {t_processing}s, upload: {t_upload}s, total: {round(t3 - t0, 1)}s')
        try:
            logger.debug('Removing file')
            os.remove(path)
        except Exception as e:
            logger.warning(f'Fail to remove {filename}. Exception: {e}')


def get_brand_code_dict(channel):
    """
    Generate brand code dic according to data source

    """
    dic = {'gsc': {'Bestseller': 'BS',
                   'Bianco': 'BI',
                   'J Lindeberg': 'JL',
                   'Jack and Jones': 'JJ',
                   'Junarose': 'JR',
                   'Mamalicious': 'MM',
                   'Name It': 'NI',
                   'Noisy May': 'NM',
                   'Object': 'OC',
                   'Only': 'ON',
                   'Only and Sons': 'OS',
                   'Pieces': 'PC',
                   'Selected': 'SL',
                   'Vero Moda': 'VM',
                   'Vila': 'VL',
                   'Yas': 'YA'},

           'criteo': {'Bestseller': 'BS',
                      'J.Lindeberg': 'JL',
                      'Jack & Jones': 'JJ',
                      'Mamalicious': 'MM',
                      'Name It': 'NI',
                      'Only': 'ON',
                      'Pieces': 'PC',
                      'Selected Homme': 'SL',
                      'Vero Moda': 'VM',
                      'Vila': 'VL'}

           }
    brand_code_dict = dic.get(channel, '(not set)')

    return brand_code_dict


def update_table_partition(env, table_name):
    bucket = get_bucket_name(env)
    athena = AthenaToolkit(bucket=bucket, database='cjdb')
    query = f'MSCK REPAIR TABLE {table_name}'
    athena.execute(query)

