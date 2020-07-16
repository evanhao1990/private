import hashlib
import uuid
import locale
import sys
import os
import _locale
from googleads import adwords
from database_connector.connector import DatabaseRedshift
import logging
logging.basicConfig(level=logging.INFO)
_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

NAME = 'CJ test0525'
DESCRIPTION = 'CJ test0525 description'
SQL_FILENAME = 'sql1.sql'


def read_sql(filename):
    """"
    Reads SQL file in directory

    """
    sql_file_path = os.path.join('sql', filename)
    with open(sql_file_path, "r") as f:
        query = f.read()

    return query


def fetch_emails(filename):
    """
    Query Redshift database and get the list of emails

    """
    sql = read_sql(filename)

    try:
        logging.info('Reading from Redshift')
        with DatabaseRedshift() as db:
            df = db.fetch(sql)
    except Exception as e:
        logging.error(f'Fail to fetch from Redshift, exception: {e}')

    df_email = df.loc[df.iloc[:, 0].str.contains('@.*\.com$', regex=True)].copy()

    # check for number of columns
    if len(df.columns) > 1:
        logging.error('Too many columns. Input should be one DataFrame containing only ONE column of emails')
        exit()

    # check if the column contains emails
    if len(df_email) == 0:
        logging.error('No emails found')
        exit()

    # check if the column contains anything other than emails
    if len(df_email) != len(df):
        logging.warning(f'{len(df_email)} emails found out of {len(df)} records')

    # put emails into a list
    emails_list = list(df_email.iloc[:, 0])

    return emails_list


def main(client, emails):
    """
    (Sample code from Adwords API documentation)

    Adds a user list and populates it with hashed email addresses.
    Note: It may take several hours for the list to be populated with members. Email
    addresses must be associated with a Google account. For privacy purposes, the
    user list size will show as zero until the list has at least 1000 members. After
    that, the size will be rounded to the two most significant digits.

    """
    # Initialize appropriate services.
    user_list_service = client.GetService('AdwordsUserListService', 'v201809')

    user_list = {
      'xsi_type': 'CrmBasedUserList',
      'name': f'{NAME} #{uuid.uuid4()}',
      'description': f'{DESCRIPTION}',
      # CRM-based user lists can use a membershipLifeSpan of 10000 to indicate
      # unlimited; otherwise normal values apply.
      'membershipLifeSpan': 30,   # expired after 30 days
      'uploadKeyType': 'CONTACT_INFO'
  }

    # Create an operation to add the user list.
    operations = [{
      'operator': 'ADD',
      'operand': user_list
    }]

    result = user_list_service.mutate(operations)
    user_list_id = result['value'][0]['id']

    members = [{'hashedEmail': NormalizeAndSHA256(email)} for email in emails]

    mutate_members_operation = {
      'operand': {
          'userListId': user_list_id,
          'membersList': members
      },
      'operator': 'ADD'
    }

    logging.info('Uploading to Google Ads')
    response = user_list_service.mutateMembers([mutate_members_operation])

    if 'userLists' in response:
        for user_list in response['userLists']:
            print('User list with name "%s" and ID "%d" was added.'
            % (user_list['name'], user_list['id']))


def NormalizeAndSHA256(s):
    """Normalizes (lowercase, remove whitespace) and hashes a string with SHA-256.

    Args:
    s: The string to perform this operation on.

    Returns:
    A normalized and SHA-256 hashed string.
    """
    return hashlib.sha256(s.strip().lower().encode('utf-8')).hexdigest()


if __name__ == '__main__':

    # Initialize client object.
    adwords_client = adwords.AdWordsClient.LoadFromStorage()

    # Query Redshift database for email list
    emails = fetch_emails(filename=SQL_FILENAME)

    # Upload to Google Adwords
    main(adwords_client, emails=emails)