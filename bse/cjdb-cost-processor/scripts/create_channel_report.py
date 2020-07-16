from src.utils import get_bucket_name, download_and_read
from src.configure_logging import configure_logging
from src.aws_toolkit import s3Toolkit
import pandas as pd
from datetime import datetime
from definitions import *

util_logger = configure_logging(logger_name='src.utils', level='DEBUG')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__, level='DEBUG')


def process_dwh(df, is_fo=False):
    """
    this function is for shared use of fact-order-metrics and fact-ua-categories

    """
    if is_fo:
        df = df.loc[df.is_ga_tracked == 1].copy()

    df['data_source'] = '(other)'
    df.loc[(df.source == 'bing') & (df.medium == 'cpc'), 'data_source'] = 'bing'
    df.loc[(df.source == 'google') & (df.medium == 'cpc'), 'data_source'] = 'adwords'
    df.loc[df['source'].str.find('criteo') >= 0, 'data_source'] = 'criteo'
    df.loc[df.medium == 'organic', 'data_source'] = 'gsc'
    df.loc[df.channel_grouping == 'Direct', 'data_source'] = 'direct'
    df.loc[df.channel_grouping == 'Email', 'data_source'] = 'email'
    mask1 = df.source.str.contains('facebook|instagram|social', regex=True)
    mask2 = df.medium.str.contains('facebook|instagram|social', regex=True)
    df.loc[mask1 & mask2, 'data_source'] = 'facebook'
    if is_fo:
        df.loc[~df.cpa_total.isnull(), 'data_source'] = 'awin'
    else:
        df.loc[df.source.str.contains('awin'), 'data_source'] = 'awin'

    df['channel'] = '(other)'
    df.loc[(df.data_source == 'gsc'), 'channel'] = 'seo'
    df.loc[(df.data_source == 'facebook'), 'channel'] = 'paid social'
    df.loc[(df.data_source == 'criteo'), 'channel'] = 'display'
    df.loc[(df.data_source == 'awin'), 'channel'] = 'affiliate'
    df.loc[(df.data_source == 'adwords'), 'channel'] = df['cpc_channel']
    df.loc[(df.data_source == 'bing') & (df.campaign.str.contains('Shopping')), 'channel'] = 'shopping'
    df.loc[(df.data_source == 'bing') & (df.campaign.str.contains('Search')), 'channel'] = 'search'
    df.loc[(df.data_source == 'bing') & (~df.channel.isin(['shopping', 'search'])), 'channel'] = 'video'
    df.loc[(df.data_source == 'direct'), 'channel'] = 'direct'
    df.loc[(df.data_source == 'email'), 'channel'] = 'email'

    return df


def process_adwords(df):
    df['data_source'] = 'adwords'
    df.rename(columns={'cpc_channel':'channel'}, inplace=True)
    df_g = df.groupby(['date','brandcode','countrycode','data_source','channel'], as_index=False)[['impressions','clicks','cost']].sum()
    return df_g


def process_awin(df):
    df.rename(columns={'cpa_total': 'cost'}, inplace=True)
    df['impressions'] = 0
    df['clicks'] = 0
    df_g = df.groupby(['date','brandcode','countrycode','data_source','channel'], as_index=False)[['impressions','clicks','cost']].sum()
    return df_g


def process_bing(df):
    df['data_source'] = 'bing'
    df['channel'] = 'video'
    df.loc[df.campaign.str.contains('Shopping'), 'channel'] = 'shopping'
    df.loc[df.campaign.str.contains('Search'), 'channel'] = 'search'
    df_g = df.groupby(['date','brandcode','countrycode','data_source','channel'], as_index=False)[['impressions','clicks','cost']].sum()

    return df_g


def process_criteo(df):
    df['data_source'] = 'criteo'
    df['channel'] = 'display'
    df_g = df.groupby(['date', 'brandcode', 'countrycode', 'data_source', 'channel'], as_index=False)[
        ['impressions', 'clicks', 'cost']].sum()

    return df_g


def process_facebook(df):
    # ! we have impression and clicks data but only on brand level,
    # here we are using pre-allocated cost in order_cost table
    df.rename(columns={'cpa_total': 'cost'}, inplace=True)
    df['impressions'] = 0
    df['clicks'] = 0
    df_g = df.groupby(['date', 'brandcode', 'countrycode', 'data_source', 'channel'], as_index=False)[
        ['impressions', 'clicks', 'cost']].sum()

    return df_g


def process_gsc(df):
    countries = ['IE', 'GB', 'NL', 'DE', 'SE', 'PL', 'NO', 'DK', 'CH', 'FR', 'BE', 'FI', 'AT', 'ES', 'IT', 'PT', 'GR']
    df = df.loc[df.countrycode.isin(countries)].copy()
    df['cost'] = 0
    df['data_source'] = 'gsc'
    df['channel'] = 'seo'
    df_g = df.groupby(['date', 'brandcode', 'countrycode', 'data_source', 'channel'], as_index=False)[
        ['impressions', 'clicks', 'cost']].sum()

    return df_g


def main(env, days):
    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket)

    # download/read files
    df_adwords_campaign = s3.read_csv_from_s3(s3_key='tables/funnel/dim-adwords-campaign/dim-adwords-campaign.csv')

    fo_cols = ['date', 'brandcode', 'countrycode', 'source', 'medium', 'channel_grouping' ,'campaign', 'referenceorder', 'adwords_campaign_id','is_ga_tracked', 'gsiibc']
    df_fo = download_and_read(prefix='tables/redshift/fact-order-metrics', days=days, cols=fo_cols, s3=s3)

    adwords_cols = ['date', 'adwords_campaign_id', 'impressions', 'clicks', 'cost']
    df_adwords = download_and_read(prefix='tables/funnel/adwords', days=days, cols=adwords_cols, s3=s3)

    awin_cols = ['date', 'referenceorder', 'cpa_total']
    df_awin = download_and_read(prefix='tables/cost-allocation/order', days=days, cols=awin_cols, s3=s3, suffix='awin')

    bing_cols = ['date', 'brandcode', 'countrycode', 'campaign', 'impressions', 'clicks', 'cost']
    df_bing = download_and_read(prefix='tables/funnel/bing', days=days, cols=bing_cols, s3=s3)

    criteo_cols = ['date', 'brandcode', 'countrycode', 'impressions', 'clicks', 'cost']
    df_criteo = download_and_read(prefix='tables/funnel/criteo', days=days, cols=criteo_cols, s3=s3)

    facebook_cols = ['date', 'referenceorder', 'cpa_total']
    df_facebook = download_and_read(prefix='tables/cost-allocation/order', days=days, cols=facebook_cols, s3=s3, suffix='facebook')

    gsc_cols = ['date', 'brandcode', 'countrycode', 'impressions', 'clicks']
    df_gsc = download_and_read(prefix='tables/funnel/gsc', days=days, cols=gsc_cols, s3=s3)

    ua_cat_cols = ['date', 'brandcode', 'countrycode', 'source', 'medium', 'channel_grouping', 'campaign', 'adwords_campaign_id', 'sessions']
    df_ua_cat = download_and_read(prefix='tables/redshift/fact-ua-categories', days=days, cols=ua_cat_cols, s3=s3)

    # process data
    df_fo = df_fo.merge(df_adwords_campaign[['adwords_campaign_id','cpc_channel']], how='left')
    df_fo = df_fo.merge(df_awin[['referenceorder','cpa_total']], how='left')
    df_fo = process_dwh(df_fo, is_fo=True)

    df_ua_cat = df_ua_cat.merge(df_adwords_campaign[['adwords_campaign_id', 'cpc_channel']], how='left')
    df_ua_cat = process_dwh(df_ua_cat, is_fo=False)

    df_adwords = df_adwords.merge(df_adwords_campaign, how='left')
    df_adwords_g = process_adwords(df_adwords)

    df_awin = df_awin.merge(df_fo[['brandcode','countrycode','referenceorder','data_source','channel']], how='left')
    df_awin_g = process_awin(df_awin)

    df_bing_g = process_bing(df_bing)

    df_criteo_g = process_criteo(df_criteo)

    df_facebook = df_facebook.merge(df_fo[['brandcode','countrycode','referenceorder','data_source','channel']], how='left')
    df_facebook_g = process_facebook(df_facebook)

    df_gsc_g = process_gsc(df_gsc)

    df_source_g = pd.concat([df_adwords_g, df_awin_g, df_bing_g, df_criteo_g, df_facebook_g, df_gsc_g])

    df_fo_g = df_fo.groupby(['date','brandcode','countrycode','data_source','channel'], as_index=False).agg({'referenceorder':'count','gsiibc':'sum'})
    df_ua_cat_g = df_ua_cat.groupby(['date', 'brandcode', 'countrycode', 'data_source', 'channel'], as_index=False)[['sessions']].sum()

    df_final = df_source_g.merge(df_fo_g, how='outer').merge(df_ua_cat_g, how='outer')

    s3.partition_table_upload(df=df_final, prefix='tables/reports/channel-report')

