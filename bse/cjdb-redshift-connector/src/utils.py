from datetime import datetime
from src.aws_toolkit import AthenaToolkit
from src.configure_logging import configure_logging
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

