from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

CHANNEL = 'facebook'

BRAND_DIC = {100271203445847: 'VM',
             10152580197681558: 'SL',
             1513432028887105: 'JJ',
             199715080159707: 'NI',
             505788223365177: 'VL'}

def process_df(df):

    df.columns = [x.lower().replace('__facebook_ads', '') for x in df.columns]
    df['brandcode'] = df.ad_account_id.map(BRAND_DIC)
    df['is_retail_campaign'] = 0
    df.loc[df.campaign_name.str.lower().str.find('retail') >= 0, 'is_retail_campaign'] = 1
    df.rename(columns={'amount_spent': 'cost', 'campaign_name': 'campaign', 'clicks_all': 'clicks',
                       'campaign_id': 'facebook_campaign_id'}, inplace=True)

    cols_sq = [
        'date',
        'brandcode',
        'facebook_campaign_id',
        'campaign',
        'campaign_objective',
        'is_retail_campaign',

        'impressions',
        'clicks',
        'cost']

    df_final = df[cols_sq]
    return df_final


def main(env, days):

    funnel_wrapper(env, days, CHANNEL, process_df)
    update_table_partition(env, table_name='fact_facebook_cost')
