from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

CHANNEL = 'bing'

DEVICE_DIC = {'Smartphone': 'mobile',
              'Tablet': 'tablet',
              'Computer': 'desktop'}


def get_brand(campaign_name):

    """
    Get brand from campaign name

    """

    campaign_name = campaign_name.lower()
    brand = 'na'

    if 'bestseller' in campaign_name:
        brand = 'BS'
    elif 'jack' in campaign_name:
        brand = 'JJ'
    elif 'vero' in campaign_name or 'vm' in campaign_name:
        brand = 'VM'
    elif 'pieces' in campaign_name:
        brand = 'PC'
    elif 'vila' in campaign_name:
        brand = 'VL'
    elif 'homme' in campaign_name:
        brand = 'SL'
    elif 'femme' in campaign_name:
        brand = 'SL'
    elif 'selected' in campaign_name:
        brand = 'SL'
    elif 'berg' in campaign_name:
        brand = "JL"
    elif 'sons' in campaign_name:
        brand = 'OS'
    elif 'only' in campaign_name:
        brand = 'ON'
    elif 'mama' in campaign_name:
        brand = 'MM'
    elif 'juna' in campaign_name:
        brand = 'JR'
    elif 'bianco' in campaign_name or 'step out' in campaign_name:
        brand = 'BI'
    elif 'yas' in campaign_name:
        brand = 'YA'
    elif 'object' in campaign_name:
        brand = 'OC'
    elif 'name' in campaign_name:
        brand = 'NI'
    elif 'noisy' in campaign_name:
        brand = 'NM'
    elif 'limited' in campaign_name or 'lmtd' in campaign_name:
        brand = 'NI'

    return brand


def get_country(name):
    countries = ['IE', 'GB', 'NL', 'DE', 'SE', 'PL', 'NO', 'DK', 'CH', 'FR', 'BE', 'FI', 'AT', 'ES', 'IT', 'PT', 'GR','UK']

    country = [x for x in countries if x in name]
    if len(country) != 1:
        country = ['na']

    return country[0]


def process_df(df):

    df.columns = [x.lower().replace('__bing', '') for x in df.columns]
    df['device'] = df['device_type'].map(DEVICE_DIC)
    df['brandcode'] = df.campaign_name.apply(lambda x: get_brand(x))
    df['countrycode'] = df.campaign_name.apply(lambda x: get_country(x))
    df.rename(columns={'spend': 'cost', 'campaign_id': 'bing_campaign_id', 'campaign_name': 'campaign'}, inplace=True)
    df.loc[df.countrycode == 'UK', 'countrycode'] = 'GB'
    cols_sq = [
                'date',
                'bing_campaign_id',
                'campaign',
                'brandcode',
                'countrycode',
                'campaigntype',
                'device',
                'assists',
                'conversions',
                'total_position',
                'clicks',
                'impressions',
                'cost'
             ]

    df_final = df[cols_sq]
    return df_final


def main(env, days):

    funnel_wrapper(env, days, CHANNEL, process_df)
    update_table_partition(env, table_name='fact_bing_cost')
