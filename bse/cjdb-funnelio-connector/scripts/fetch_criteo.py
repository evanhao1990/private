from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)

CHANNEL = 'criteo'


def process_df(df):
    brand_code_dic = get_brand_code_dict(CHANNEL)
    df.columns = [x.lower().replace('__criteo', '') for x in df.columns]

    df['countrycode'] = df['connection_name'].apply(lambda x: x.replace('(EUR)', '').strip()[-2:])
    df['brand_connection'] = df['connection_name'].apply(lambda x: x.replace('(EUR)', '').strip()[:-2].strip())
    df.drop(['campaign_id', 'connection_name'], axis=1, inplace=True)

    df['brandcode'] = df['brand_connection']
    df['brandcode'] = df['brandcode'].map(brand_code_dic)
    df.rename(columns={'campaign_name':'campaign'}, inplace=True)
    df.loc[df.countrycode == 'UK', 'countrycode'] = 'GB'
    df = df[['date', 'brand_connection', 'brandcode', 'countrycode'
        , 'campaign_bid_type', 'campaign', 'category_bid_type'
        , 'category_name', 'clicks', 'cost', 'impressions', 'order_value', 'order_value_post_view', 'sales'
        , 'sales_post_view_pv', 'sales_post_view_pv_nd', 'same_device_sales']]

    return df


def main(env, days):

    funnel_wrapper(env, days, CHANNEL, process_df)
    update_table_partition(env, table_name='fact_criteo_cost')