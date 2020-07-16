from src.ga_connector import GoogleAnalytics
from src.configure_logging import configure_logging
from src.s3_toolkit import s3Toolkit
from src.utils import get_timestamp, get_bucket_name, get_ua_profile_list
import time
import pandas as pd
import logging

logger = configure_logging(logger_name=__name__, level=logging.INFO)
TIMESTAMP = get_timestamp()


def main(env, **date_ranges):

    t0 = time.time()

    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket=bucket)

    profile_list = get_ua_profile_list('App', bucket)

    dimensions = ['ga:date', 'ga:deviceCategory']
    metrics = ['ga:sessions']

    # initialize connection and pull data from individual GA views
    ga = GoogleAnalytics()
    df_all = pd.DataFrame()
    for dim_ua_id, ua_id, name in profile_list:
        logger.info(f'Fetching {name} ({ua_id})')
        df = ga.fetch(view_id=ua_id, dimensions=dimensions,  metrics=metrics, **date_ranges)
        df['dim_ua_profile_id'] = dim_ua_id
        df_all = df_all.append(df, ignore_index=True)

    if len(df_all) == 0:
        logger.error('Empty DataFrame')
        exit()

    # pre-process data
    df_all.sessions = df_all.sessions.astype('int')
    df_all.dim_ua_profile_id = df_all.dim_ua_profile_id.astype('int')
    df_all.date = df_all.date.apply(lambda x: '-'.join([x[:4], x[4:6], x[-2:]]))
    df_all['row_last_updated_ts'] = TIMESTAMP

    dates = set(df_all.date)

    # split data into a file per day and store them separately
    for date in dates:
        df_date = df_all.loc[df_all.date == date].copy()
        s3_key = f'tables/ga/app-sessions/app_sessions_{date}.csv'
        s3.upload_dataframe(df=df_date, s3_key=s3_key, delimiter=';')
        logger.info(f'Inserting {date} {df_date.dim_ua_profile_id.nunique()} profiles {len(df_date)} records into {s3_key}')

    t1 = time.time()
    logger.info(f'Done, {len(dates)} dates inserted time spent {round(t1-t0,1)}s')