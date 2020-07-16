from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

CHANNEL = 'gsc'


def process_df(df):
    """
    Transforms the input dataframe:
    - strip column names
    - extract site_country from page url
    - get brand_code from brand dictionary
    - group by relevant dimensions: one row per date|site_brand|site_country|geo_country|device|query|page

    Parameters:
    ----------
    df: dataframe
        input dataframe from funnel exports
    """
    gsc_brand_code_dict = get_brand_code_dict(CHANNEL)
    df.columns = [x.lower().replace('__google_search_console', '') for x in df.columns]
    df['geo_country'] = df['country']
    df['countrycode'] = df['page'].apply(lambda x: x.split('/')[3])
    df['countrycode'] = df['countrycode'].str.upper()
    df['brandcode'] = df['connection_name'].map(gsc_brand_code_dict)
    df = df.groupby(['date', 'brandcode', 'countrycode', 'geo_country', 'device', 'query', 'page'], as_index=False)[['clicks', 'impressions', 'total_position']].sum()
    return df


def main(env, days):

    funnel_wrapper(env, days, CHANNEL, process_df)
    update_table_partition(env, table_name='fact_gsc_searches')
    logger.info("Done!")
