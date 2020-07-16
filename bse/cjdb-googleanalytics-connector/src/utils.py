from datetime import datetime
from src.s3_toolkit import s3Toolkit


def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def get_ua_profile_list(property_source, bucket):
    """
    Get the list of ua profiles' database id, view id and name.

    Parameters
    ----------
    property_source: string
        the property source of the view, can be App or Web.

    bucket: string
        the name of the bucket
    Returns
    -------
    profile list: list
        a list of ua profiles' id, view_id and name.
    """

    # read ua_profile table from S3
    s3 = s3Toolkit(bucket=bucket)
    s3_key = 'tables/ga/dim-ua-profile/dim_ua_profile.csv'
    df_dim_profile = s3.read_csv_from_s3(s3_key=s3_key, delimiter=';')

    # filter for property source we need
    df_app = df_dim_profile.loc[df_dim_profile.property_source == property_source][['dim_ua_profile_id', 'ua_profile_id', 'name']]

    # get 3 columns and zip in to list for later use
    dim_ua_id = df_app.dim_ua_profile_id  # database id
    ua_id = df_app.ua_profile_id          # view id
    name = df_app.name                    # ua profile name,
    profile_list = list(zip(dim_ua_id, ua_id, name))

    return profile_list
