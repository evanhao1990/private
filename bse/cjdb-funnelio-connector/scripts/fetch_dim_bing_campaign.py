from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)


TABLE = 'dim-bing-campaign'
S3_DESTINATION_KEY = 'tables/funnel/dim-bing-campaign/dim-bing-campaign.csv'


def fetch_latest(s3, days):
    """
    Read campaign and campaign id combination from a certain date range.

    Parameters
    ----------
    days: int or list
        int: Number of days we want to go back,starting from yesterday.
        list: A list of two elements:[start_date, end_date], with date formatted as yyyy-mm-dd.

    Returns
    -------
    DataFrame
    """
    date_range, keys = generate_days_keys(channel=TABLE, days=days)

    df_all = pd.DataFrame()
    # download csv from s3 to local and read it into a DataFrame
    for key in keys:
        filename = key.split('/')[-1]
        path = os.path.join(DATA_DIR, filename)
        logger.info(f'Processing {key}')
        s3.download_file(s3_key=key, filename=path)
        try:
            rules = lambda x: x not in ['Connection_type_code', 'Connection_id', 'Currency']
            df = pd.read_csv(path, sep=',', usecols=rules)
            df_all = df_all.append(df, ignore_index=True)

        except Exception as e:
            logger.warning(f'Fail to read {filename}. Exception: {e}')

        # removing downloaded file
        os.remove(path)

    return df_all


def process(df):
    """
    Process DataFrame:
        1. For every campaign id, get the campaign name from the latest date
        2. Retrieve detailed information from campaign name
        3. join back to the origin DataFrame on campaign_id so that all the historical names also gets filled
        4. flag the latest name

    """

    df.columns = [x.lower().replace('__bing', '') for x in df.columns]
    df.sort_values(by=['campaign_id', 'date'], ascending=False, inplace=True)

    # find the lastest name, get info according to naming convention.
    df_latest = df.drop_duplicates(subset='campaign_id', keep='first')
    df_latest = df_latest[['campaign_id', 'campaign_name']]
    df_latest['bse_channel'] = df_latest.campaign_name.str.split('-').str[0].str.strip()
    df_latest['brandcode'] = df_latest.campaign_name.str.split('-').str[1].str.strip()
    df_latest['countrycode'] = df_latest.campaign_name.str.split('-').str[2].str.strip()
    df_latest['cpc_channel'] = df_latest.campaign_name.str.split('-').str[3].str.strip()
    df_latest['campaign_type'] = df_latest.campaign_name.str.split('-').str[4].str.strip()
    df_latest['x_attribute_1'] = df_latest.campaign_name.str.split('-').str[5].str.strip()
    df_latest['x_attribute_2'] = df_latest.campaign_name.str.split('-').str[6].str.strip()
    df_latest['is_latest'] = 1

    # join back the latest info on campaign_id, in this way all the historical campaigns will get the latest info
    df_all = df[['campaign_id', 'campaign_name']].drop_duplicates()
    cols1 = ['campaign_id','bse_channel','brandcode','countrycode','cpc_channel','campaign_type','x_attribute_1','x_attribute_2']
    df_all = df_all.merge(df_latest[cols1], how='left', on='campaign_id')

    # add "is_latest" flag so that when we join dim_bing_campaign we can apply filter to get unique record
    cols2 = ['campaign_id', 'campaign_name', 'is_latest']
    df_all = df_all.merge(df_latest[cols2], how='left', on=['campaign_id', 'campaign_name'])

    df_all['is_latest'] = df_all['is_latest'].fillna(0)
    df_all.fillna('(not set)', inplace=True)
    df_all.rename(columns={'campaign_name': 'campaign', 'campaign_id': 'bing_campaign_id'}, inplace=True)
    return df_all


def update_table(s3, df):
    """
    Update dim-bing-campaign.csv in tables/funnel/dim-bing-campaign/dim-bing-campaign.csv.

    """
    try:
        if S3_DESTINATION_KEY in s3.list_files_in_directory(prefix='tables/funnel/dim-bing-campaign'):
            # download existing dim_campaign table from s3
            # this table should contains the latest campaign info till last run
            # for those that are not in the newest list we created above, we use info from this table.
            # In this way we can keep all campaign info up-to-date.

            # get historical dim_bing_campaign
            df_historical = s3.read_csv_from_s3(s3_key=S3_DESTINATION_KEY)
            # find those campaigns that are not in the latest list
            df_to_append = df_historical.loc[~df_historical.bing_campaign_id.isin(df.bing_campaign_id)].copy()
            # append them to the latest list so we have a complete dim_bing_campaign list
            df_final = df.append(df_to_append, ignore_index=True, sort=False)
            # then overwrite existing file.
            s3.upload_dataframe(df=df_final, s3_key=S3_DESTINATION_KEY, index=False, delimiter=';')
            logger.info('dim-bing-campaign.csv updated')
        else:

            # if the file does not exist(this happens only for the first run), we simply upload all the data we have
            logger.info('dim-bing-campaign.csv not found, uploading new file')
            s3.upload_dataframe(df=df, s3_key=S3_DESTINATION_KEY, index=False, delimiter=';')

    except Exception as e:
        logger.error(f'Failed to update dim-bing-campaign. Exception: {e}')


def main(env, days):

    s3 = s3Toolkit(get_bucket_name(env))

    df_new = fetch_latest(s3, days)

    df_new = process(df_new)

    update_table(s3, df_new)




