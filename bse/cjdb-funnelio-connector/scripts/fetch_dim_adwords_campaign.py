from src.utils import *
from src.configure_logging import configure_logging

util_logger = configure_logging(logger_name='src.utils', level='INFO')
toolkit_logger = configure_logging(logger_name='src.aws_toolkit', level='WARNING')
logger = configure_logging(logger_name=__name__)


TABLE = 'dim-adwords-campaign'
S3_DESTINATION_KEY = 'tables/funnel/dim-adwords-campaign/dim-adwords-campaign.csv'


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
        1. For every campaign id, keep only the campaign name from the latest date.
        2. Retrieve detailed information from campaign name

    """

    df.columns = [x.lower().replace('__adwords', '') for x in df.columns]
    df.sort_values(by=['campaign_id', 'date'], ascending=False, inplace=True)
    df.drop_duplicates(subset='campaign_id', keep='first', inplace=True)
    df = df[['campaign_id', 'campaign']].copy()

    df['bse_channel'] = df.campaign.str.split('-').str[0].str.strip()
    df['brandcode'] = df.campaign.apply(lambda x: get_brand(x))
    df['countrycode'] = df.campaign.str.split('-').str[2].str.strip()
    df['cpc_channel'] = df.campaign.str.split('-').str[3].str.strip()
    df['campaign_type'] = df.campaign.str.split('-').str[4].str.strip()
    df['x_attribute_1'] = df.campaign.str.split('-').str[5].str.strip()
    df['x_attribute_2'] = df.campaign.str.split('-').str[6].str.strip()

    df.fillna('(not set)', inplace=True)
    df.rename(columns={'campaign_id': 'adwords_campaign_id'}, inplace=True)
    df.loc[df.countrycode=='UK','countrycode'] = 'GB'
    return df


def update_table(s3, df):
    """
    Update dim-adwords-campaign.csv in tables/funnel/dim-adwords-campaign/dim-adwords-campaign.csv.

    """
    try:
        if S3_DESTINATION_KEY in s3.list_files_in_directory(prefix='tables/funnel/dim-adwords-campaign'):
            # download existing dim_campaign table from s3
            # this table should contains the latest campaign info till last run
            # for those that are not in the newest list we created above, we use info from this table.
            # In this way we can keep all campaign info up-to-date.

            # get historical dim_adwords_campaign
            df_historical = s3.read_csv_from_s3(s3_key=S3_DESTINATION_KEY)
            # find those campaigns that are not in the latest list
            df_to_append = df_historical.loc[~df_historical.adwords_campaign_id.isin(df.adwords_campaign_id)].copy()
            # append them to the latest list so we have a complete dim_adwords_campaign list
            df_final = df.append(df_to_append, ignore_index=True, sort=False)
            # then overwrite existing file.
            s3.upload_dataframe(df=df_final, s3_key=S3_DESTINATION_KEY, index=False, delimiter=';')
            logger.info('dim-adwords-campaign.csv updated')
        else:

            # if the file does not exist(this happens only for the first run), we simply upload all the data we have
            logger.info('dim-adwords-campaign.csv not found, uploading new file')
            s3.upload_dataframe(df=df, s3_key=S3_DESTINATION_KEY, index=False, delimiter=';')

    except Exception as e:
        logger.error(f'Failed to update dim-adwords-campaign. Exception: {e}')


def main(env, days):

    s3 = s3Toolkit(get_bucket_name(env))

    df_new = fetch_latest(s3, days)

    df_new = process(df_new)

    update_table(s3, df_new)




