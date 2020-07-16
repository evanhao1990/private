from datetime import datetime
from src.aws_toolkit import AthenaToolkit
from src.aws_toolkit import s3Toolkit
from src.configure_logging import configure_logging
from definitions import *
from dateutil.relativedelta import *
import numpy as np
import pandas as pd

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


def update_table_partition(env, table_name):
    bucket = get_bucket_name(env)
    athena = AthenaToolkit(bucket=bucket, database='cjdb')
    query = f'MSCK REPAIR TABLE {table_name}'
    athena.execute(query)


def read_sql(filename, days=None):
    """
    Read sql query. Fill in date range if needed.

    Parameters
    ----------
    filename: str
    days: number or a list of two dates
        number: the number of days to backfill, starting from today
        a list of two dates: ['start_date','end_date']
    Returns
    -------

    """
    path = os.path.join(SQL_DIR, filename)
    f = open(path, 'r')
    sql = f.read()
    f.close()

    if days:
        if len(days) == 1 and days[0].isdigit():
            date_dic = {'start_date': f'current_date - {abs(int(days[0]))}', 'end_date': 'current_date'}
            sql = sql.format(**date_dic)
        if len(days) == 2:
            date_dic = {'start_date': f"'{days[0]}'", 'end_date': f"'{days[1]}'"}
            sql = sql.format(**date_dic)

    return sql


def generate_days_keys(prefix, days, suffix=None):
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
    day_list = [x.split('-') for x in day_list]
    if suffix:
        keys = [f'{prefix}/year={y}/month={m}/day={d}/{y}{m}{d}{suffix}.csv' for y, m, d in day_list]
    else:
        keys = [f'{prefix}/year={y}/month={m}/day={d}/{y}{m}{d}.csv' for y, m, d in day_list]
    return keys


def error_handler(func):
    def wrapper(*args, **kw):
        try:
            func(*args, **kw)
        except Exception as e:
            logger.error(f'Function "{func.__name__}" failed. Exception: {e}')
    return wrapper


def download_and_read(prefix, days, s3, cols=None, suffix=None):
    """
    Download file from s3 and read into DataFrame

    Parameters
    ----------
    prefix: string
        prefix of the s3 key
    days: int or list
        int: Number of days we want to back fill,starting from yesterday.
        list: A list of two elements:[start_date, end_date], with date formatted as yyyy-mm-dd.
    cols: list
        list of columns that are needed to read into DataFrame
    s3: s3 client

    suffix: string
        suffix of the file, only needed for files in tables/cost-allocation/orders/

    Returns
    -------
    DataFrame

    """

    # generate s3 keys and local folder paths
    folder = os.path.join(DATA_DIR, prefix.split('/')[-1])
    keys = generate_days_keys(prefix=prefix, days=days, suffix=suffix)
    paths = [os.path.join(folder, key.split('/')[-1]) for key in keys]

    # check if any files already downloaded locally
    l = list(zip(keys, paths))
    keys_to_download = [key for (key, path) in l if not os.path.exists(path)]
    paths_to_download = [path for (key, path) in l if not os.path.exists(path)]

    if not os.path.exists(folder):
        os.mkdir(folder)

    logger.debug(f'downloading {len(keys_to_download)} new files into {folder}')
    any(map(s3.download_file, keys_to_download, paths_to_download))  # any() here is only used to run the map() function

    def read_into_csv(path):
        df = pd.read_csv(filepath_or_buffer=path, sep=';', usecols=cols)
        return df

    logger.debug(f'reading {len(paths)} files from {folder}')
    df_final = pd.concat(map(read_into_csv, paths), sort=False)

    return df_final
