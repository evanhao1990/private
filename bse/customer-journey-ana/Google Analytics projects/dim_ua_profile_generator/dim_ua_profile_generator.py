"""
Normally the structure of GA tables is:
    Account --> Property --> View
A real example would be:
    Bestseller (Universal) --> 1 - Web - Brand - Only --> A - Web, ON - Overview(3 Non-User ID)
    To understand better you can visit https://ga-dev-tools.appspot.com/account-explorer/ and login in with GA account

What we do to get dim_ua_profile table:
    Firstly we loop through all the Properties within the main account (Bestseller (Universal)) to get information related with all the views under each Property.
    Secondly we filter for the Views' profiles we want -- we only need brand-country level Views to pull data from.
    Thirdly we add more columns with information we need -- sitebrand, sitecountry, table_updated_time and etc..
"""

import pandas as pd
from datetime import datetime
from src.configure_logging import configure_logging
from src.ga_connector import GoogleAnalytics
from src.s3_toolkit import s3Toolkit

# to avoid printing out logs from GA's module we need to setup our own logger
logger = configure_logging(logger_name=__name__)
ACCOUNT = 66188758


def get_bucket_name(env):
    """"
    Gets Bucket Name according to the chosen environment

    Parameters:
    ----------
        env : string
            dev or prod
    Returns:
    ----------
        bucket_name: string
    """

    return f"bse-cjdb-{env}.bseint.io"


def read_profiles_per_property(service, property):

    property_name = property['name']                   # property name
    pid = property['id']                               # property id

    if property_name == 'Roll-Up Property':
        property_source = 'N/A'                        # get property source (app or web) and type (brand or market)
        property_type = 'N/A'
    else:
        property_source = property_name.split(' ')[2]  # property source (app or web)
        property_type = property_name.split(' ')[4]    # property type (brand or market)

    # get all profiles in this property
    profiles = service.management().profiles().list(accountId=ACCOUNT, webPropertyId=pid).execute()

    ua_profile_id = [prf['id'] for prf in profiles.get('items')]  # list of all profile id
    ua_profile_name = [prf['name'] for prf in profiles.get('items')]  # list of all profile name
    profile_created_time = [prf['created'] for prf in profiles.get('items')]  # list of all profile created time

    # put into DataFrame
    df = pd.DataFrame({'ua_profile_id': ua_profile_id,
                       'name': ua_profile_name,
                       'profile_created_time': profile_created_time,
                       'property_name': property_name,
                       'property_source': property_source,
                       'property_type': property_type})

    return df


def process_profiles(df):

    # filter out profiles that we don't need
    # aggregate level = 'B', property_type = 'Brand' and profile name does not contain "User"
    df['level'] = df['name'].str.split(' ').str[0]
    df_dim_profile = df.loc[df.level == 'B'].copy()
    df_dim_profile = df_dim_profile.loc[df_dim_profile['name'].str.find('User') <= 0]
    df_dim_profile = df_dim_profile.loc[df_dim_profile['property_type'] == 'Brand']

    # add additional information
    df_dim_profile['site_brand'] = df_dim_profile['name'].str.split(',').str[1].str.split(' ').str[1]
    df_dim_profile['site_country'] = df_dim_profile['name'].str.split('-').str[-1].str.strip()
    df_dim_profile['table_updated_time'] = datetime.now()
    df_dim_profile['dim_ua_profile_id'] = df_dim_profile.index

    # reorder columns
    df_dim_profile = df_dim_profile[['dim_ua_profile_id', 'ua_profile_id', 'property_source', 'site_brand',
                                     'site_country', 'property_type', 'level', 'name', 'property_name',
                                     'profile_created_time', 'table_updated_time']]

    return df_dim_profile


def main(env):

    # authorization and connect
    ga = GoogleAnalytics(api_name='analytics', api_version='v3')
    service = ga._build_service()
    properties = service.management().webproperties().list(accountId=ACCOUNT).execute()

    # looping through all properties and views
    df_all = pd.DataFrame()
    for item in properties.get('items'):
        try:
            logger.info(f"Reading {item['name']}")
            df_profile = read_profiles_per_property(service,item)
            df_all = df_all.append(df_profile, ignore_index=True)
        except Exception as e:
            logger.error(f"Fail to read {item['name']}, Exception : {e}")

    df_dim_ua_profile = process_profiles(df_all)

    bucket = get_bucket_name(env)
    s3 = s3Toolkit(bucket=bucket)
    s3_key = 'tables/ga/dim-ua-profile/dim_ua_profile.csv'
    logger.info(f'Inserting {len(df_dim_ua_profile)} records into {s3_key}')
    s3.upload_dataframe(df=df_dim_ua_profile, s3_key=s3_key, delimiter=';')


if __name__ == '__main__':
    main('dev')
