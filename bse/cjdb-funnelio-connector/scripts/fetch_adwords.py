from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

CHANNEL = 'adwords'

DEVICE_DIC = {'Mobile devices with full browsers': 'mobile',
              'Tablets with full browsers': 'tablet',
              'Computers': 'desktop',
              'Devices streaming video content to TV screens': 'desktop'}


def get_ad_group_cluster(ad_group_name):
    """
    Get ad group cluster base on ad group name. The rules to cluster the names is decided by marketing team.

    """
    ad_group_name = ad_group_name.lower()

    if 'visitor' in ad_group_name:
        ad_group_name = 'All Visitors'
    elif 'plp' in ad_group_name or 'pdp' in ad_group_name or 'product' in ad_group_name:
        ad_group_name = 'Product Viewers'
    elif 'abandoner' in ad_group_name:
        ad_group_name = 'Basket Abandoners'
    elif 'buyer' in ad_group_name:
        ad_group_name = 'Previous Buyers'
    elif 'converter' in ad_group_name:
        ad_group_name = 'Previous Buyers'

    return ad_group_name


def process_df(df):

    df.columns = [x.lower().replace('__adwords', '') for x in df.columns]

    df['ad_group_cluster'] = df['ad_group_name'].apply(lambda x: get_ad_group_cluster(x))
    df['device'] = df['device'].map(DEVICE_DIC)
    df.rename(columns={'campaign_id': 'adwords_campaign_id'}, inplace=True)

    cols_sq = [
        'date',
        'adwords_campaign_id',
        'device',
        'ad_type',
        'ad_group_name',
        'ad_group_type',
        'keyword_match_type',
        'ad_final_urls',

        'impressions',
        'video_views',
        'views',
        'interactions',
        'engagements',
        'total_top_impressions',
        'clicks',
        'cost',
        'total_absolute_top_impressions']

    df_final = df[cols_sq]
    return df_final


def main(env, days):

    funnel_wrapper(env, days, CHANNEL, process_df)
    update_table_partition(env, table_name='fact_adwords_cost')