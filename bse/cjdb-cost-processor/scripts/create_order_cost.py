from src.utils import get_bucket_name, update_table_partition, error_handler, download_and_read
from src.configure_logging import configure_logging
from src.aws_toolkit import s3Toolkit
import pandas as pd
from datetime import datetime

util_logger = configure_logging(logger_name='src.utils', level='DEBUG')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)


def fo_bing_filter(df):
    """
    filter out bing orders from fact_order_metrics table

    """
    df_source = df.loc[(df.source == 'bing') & (df.medium == 'cpc')].copy()

    return df_source


def fo_facebook_filter(df):
    """
    filter out facebook orders from fact_order_metrics table

    """
    mask1 = df.source.str.contains('facebook|instagram|social', regex=True)
    mask2 = df.medium.str.contains('facebook|instagram|social', regex=True)
    df_source = df.loc[mask1 & mask2].copy()

    return df_source


def fo_adwords_filter(df):
    """
    filter out adwords orders from fact_order_metrics table

    """
    df_source = df.loc[~df.adwords_campaign_id.isnull()].copy()
    return df_source


def fo_criteo_filter(df):
    """
    filter out criteo orders from fact_order_metrics table

    """
    df_source = df.loc[df['source'].str.find('criteo') >= 0].copy()
    return df_source


def fo_sovendus_filter(df):
    """
    filter out sovendus orders from fact_order_metrics table

    """
    df_source = df.loc[df.is_sovendus_order == 1].copy()
    df_source = df_source[['countrycode', 'brandcode', 'date', 'referenceorder', 'gsii']].reset_index()
    return df_source


def cost_allocation_logic_ppc(df_fo_source, df_source, levels, source_name):
    """
    Logics to allocate ppc sources cost to orders on required level

    Parameters
    ----------
    df_fo_source: DataFrame
        DataFrame containing order info from fact_order_metrics pre-filtered for required source

    df_source: DataFrame
        DataFrame containing cost info from fact_source_cost table

    levels: List
        Levels that cost will be allocated on

    source_name: string
        name of the source

    Returns
    -------
    DataFrame
        DataFrame with ['date', 'referenceorder', 'cpa', 'cpa_extra', 'source_name']
    """

    # filter to keep only the combinations exist in cost table
    filters = df_source[levels].copy().drop_duplicates()
    df_fo_source = df_fo_source.merge(filters, how='inner', on=levels)

    # join cost and order table on required level
    df_cost = df_source.groupby(levels, as_index=False)[['cost']].sum()
    df_orders = df_fo_source.groupby(levels, as_index=False)[['referenceorder']].count()
    df_merged = df_cost.merge(df_orders, how='left', on=levels)

    # re-allocate the cost of campaigns with no orders (sum and distribute equally to all orders by date)
    df_x_cost = df_merged.loc[df_merged.referenceorder.isnull()].groupby(['date'], as_index=False)[['cost']].sum()
    df_x_orders = df_merged.loc[~df_merged.referenceorder.isnull()].groupby(['date'], as_index=False)[
        ['referenceorder']].sum()
    df_x_final = df_x_cost.merge(df_x_orders, how='left', on='date')
    df_x_final['cpa_extra'] = df_x_final.cost / df_x_final.referenceorder

    # calculate cost per acquisition(cpa)
    df_cpa = df_merged.loc[~df_merged.referenceorder.isnull()].copy()
    df_cpa['cpa'] = df_cpa.cost / df_cpa.referenceorder
    cols = levels.copy()
    cols.append('cpa')
    df_cpa = df_cpa[cols]  # keep only needed columns (required dimensions + cpa)

    # join back cpa table onto the original order table on required level
    df_order_cost = df_fo_source.merge(df_cpa, how='left', on=levels)
    # join back cpa_extra onto the original order table on date level
    df_order_cost = df_order_cost.merge(df_x_final[['date', 'cpa_extra']], on='date', how='left')

    # keep only needed columns and add data_source name
    df_order_cost = df_order_cost[['date', 'referenceorder', 'cpa', 'cpa_extra']]
    df_order_cost['cpa_total'] = df_order_cost.cpa + df_order_cost.cpa_extra
    df_order_cost['data_source'] = source_name

    return df_order_cost


@error_handler
def cost_allocator_ppc(df_fo_source, source_name, levels, env, days):
    """
    Use to allocate cost for ppc sources and upload results to s3

    Parameters
    ----------
    df_fo_source: DataFrame
        DataFrame from fact_order_metrics after sliced for specific data source orders
    source_name: string
        Name of the data source
    levels: list
        detail level that cost will be allocated on


    """
    folder_source = f'tables/funnel/{source_name}'

    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket)

    # reading cost table
    logger.info(f'reading {folder_source}')
    df_source = download_and_read(prefix=folder_source, days=days,s3=s3)

    # allocating cost
    logger.info('allocating cost')
    df_order_cost = cost_allocation_logic_ppc(df_fo_source, df_source, levels, source_name)

    # uploading to s3
    logger.info(f'uploading partitioned {source_name}.csv')
    s3.partition_table_upload(df=df_order_cost, prefix='tables/cost-allocation/order', suffix=source_name)


@error_handler
def cost_allocator_awin(df_fo_source, env, days):
    """
    Use to allocate cost for Awin  and upload results to s3

    """
    # prepare dataset
    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket)

    t = 'tables/funnel/awin'
    # logger.info(f'reading {t}')
    df_awin = download_and_read(prefix=t, days=days, s3=s3)

    # align mixed data types within the column
    df_awin.order_reference = df_awin.order_reference.apply(lambda x: str(x))

    df_cpa = df_awin.loc[df_awin.order_reference.str.isdigit()].copy()  # slicing for actual orders
    df_cpa.rename(columns={'transaction_commission_validation_date': 'cpa', 'date': 'awin_date'}, inplace=True)

    # dwh date sometimes is 1 or 2 days later than awin date, this step is to change date to dwh date
    df_fo_date = df_fo_source[['date','referenceorder']].copy()
    df_fo_date.referenceorder = df_fo_date.referenceorder.apply(lambda x: str(x))

    df_cpa = df_cpa.merge(df_fo_date, how='inner', left_on='order_reference', right_on='referenceorder')
    df_cpa = df_cpa[['date', 'referenceorder', 'cpa']]
    # df_cpa_extra = df_awin.loc[df_awin.order_reference.str.contains('bonus')].copy()  # slicing for bonus rows
    df_cpa_extra = df_awin.loc[~df_awin.order_reference.str.isdigit()].copy()  # slicing for bonus rows
    df_orders = df_cpa.groupby(['date'], as_index=False)[['referenceorder']].count()  # count number of orders per day
    df_bonus = df_cpa_extra.groupby(['date'], as_index=False)[['transaction_commission_validation_date']].sum()  # sum bonus commission

    # distributing bonus commission to all the orders within the same day
    df_merged = df_orders.merge(df_bonus, how='inner')
    df_merged['cpa_extra'] = df_merged['transaction_commission_validation_date'] / df_merged['referenceorder']

    df_order_cost = df_cpa.merge(df_merged[['date','cpa_extra']], how='left')
    df_order_cost.fillna(0, inplace=True)
    df_order_cost = df_order_cost[['date', 'referenceorder', 'cpa', 'cpa_extra']]
    df_order_cost['cpa_total'] = df_order_cost.cpa + df_order_cost.cpa_extra
    df_order_cost['data_source'] = 'awin'

    logger.info('uploading partitioned awin.csv')
    s3.partition_table_upload(df=df_order_cost, prefix='tables/cost-allocation/order', suffix='awin')


@error_handler
def cost_allocator_sovendus(df_fo_source, env):
    """
    Use to allocate cost for Sovendus and upload results to s3

    Parameters
    ----------
    df_fo_source: DataFrame
        DataFrame from fact_order_metrics after sliced for specific data source orders


    """
    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket)

    logger.info('reading sovendus-commission')
    df_svd = s3.read_csv_from_s3(s3_key='tables/redshift/sovendus-commission/sovendus-commission.csv')

    # find sovendus orders
    df_fo_svd = df_fo_source

    # if last date is null, fill current date
    today = datetime.strftime(datetime.today(), "%Y-%m-%d")
    df_svd.last_date.fillna(today, inplace=True)

    # join commission table on order table on brand country level
    # keep only the rows where date falls into the effective period
    logger.info('allocating cost')
    df_order_cost = df_fo_svd.merge(df_svd, on=['brandcode', 'countrycode'], how='left')
    df_order_cost = df_order_cost.loc[(df_order_cost.date >= df_order_cost.first_date) & (df_order_cost.date <= df_order_cost.last_date)].copy()
    df_order_cost['cpa'] = df_order_cost.gsii * df_order_cost.commission_rate
    df_order_cost['cpa_extra'] = 0
    df_order_cost = df_order_cost[['date', 'referenceorder', 'cpa', 'cpa_extra']]
    df_order_cost['cpa_total'] = df_order_cost.cpa + df_order_cost.cpa_extra
    df_order_cost['data_source'] = 'sovendus'

    logger.info('uploading partitioned sovendus.csv')
    s3.partition_table_upload(df=df_order_cost, prefix='tables/cost-allocation/order', suffix='sovendus')


def main(env, days):

    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket)

    logger.info('reading fact_order_metrics')
    df_fo = download_and_read(prefix='tables/redshift/fact-order-metrics', days=days, s3=s3)

    cost_allocator_ppc(df_fo_source=fo_bing_filter(df_fo),
                       source_name='bing',
                       levels=['date', 'brandcode', 'countrycode', 'device'],
                       env=env, days=days)

    cost_allocator_ppc(df_fo_source=fo_facebook_filter(df_fo),
                       source_name='facebook',
                       levels=['date', 'brandcode'],
                       env=env, days=days)

    cost_allocator_ppc(df_fo_source=fo_adwords_filter(df_fo),
                       source_name='adwords',
                       levels=['date', 'adwords_campaign_id', 'device'],
                       env=env, days=days)

    cost_allocator_ppc(df_fo_source=fo_criteo_filter(df_fo),
                       source_name='criteo',
                       levels=['date', 'brandcode', 'countrycode'],
                       env=env, days=days)

    cost_allocator_sovendus(df_fo_source=fo_sovendus_filter(df_fo),
                            env=env)

    cost_allocator_awin(df_fo_source=df_fo, env=env, days=days)

    update_table_partition(env, table_name='order_cost')