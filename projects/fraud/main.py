import requests
import json
import logging
import io
from google.cloud import bigquery
from datetime import date
from src import sent_email
logging.basicConfig(level="INFO")

URL = 'https://fraudshield.24metrics.com/api/v1/reports/conversion_scroll.json'

YESTERDAY = date.today().strftime('%Y-%m-%d')

API_TOKEN = 'w8RtRDQ2u0QlKcNNeEVmOZN5ODtwnwuAPnQFNwCgbihShKvRUkuwAYlPJ2pT'

PAYLOAD = {'user_id': '2452', 'api_token': API_TOKEN, 'tracker_id': '1272', 'date_start': YESTERDAY,
           'date_end': YESTERDAY, 'timezone': 'Europe/Berrlin', 'count': '4000', 'page': '1'}
HEAD = {'Accept': 'application/json',  'Content-Type': 'application/json'}

COLS = ['id', 'request_time', 'request_date', 'session_time', 'country_code', 'affiliate_label', 'product_label',
        'partner', 'sub_id', 'remote_address', 'status', 'reason', 'advanced_reason', 'isp', 'device', 'os', 'browser',
        'fingerprint', 'uuid']

COLS_X = ['fs_transaction_id', 'signupDevice', 'enduserId', 'emailDomain', 'email', 'partnerExtra', 'signupIp']

LOGS = []
creds_path = "/Users/haozhang/creds/gcp.json"


def parse_data(data):

    # get regular info
    dic = {col: data.get(col, '') for col in COLS}

    # get info inside data['extra_parameters'] into a dic
    extra_dic = {list(x.values())[0]: list(x.values())[1] for x in data.get('extra_parameters', {})}

    # pick data needed from extra_dic and put into the main dictionary
    for col in COLS_X:
        dic[col] = extra_dic.get(col, '')

    return dic


def check_and_save(dic, f):

    if dic.get('sub_id') == 'typein':
        pass
    elif dic.get('fs_transaction_id', '') == '':
        pass
    else:
        f.write(json.dumps(dic, separators=(',', ':'))+'\n')


def upload_to_bq(client, table, content):

    job_config = bigquery.LoadJobConfig(source_format='NEWLINE_DELIMITED_JSON', autodetect=True)

    client.load_table_from_file(content, rewind=True, destination=table, location='EU', job_config=job_config)


def make_call():
    # Reading from 24metrics
    content = io.StringIO()
    row_count = 1
    status_codes = []
    PAYLOAD['next_page_id'] = ''

    while row_count > 0:
        response = requests.post(URL, params=PAYLOAD, headers=HEAD)
        obj = json.loads(response.text)
        status_codes.append(response.status_code)

        for d in obj.get('data'):
            dic = parse_data(d)
            check_and_save(dic, content)

        PAYLOAD['next_page_id'] = obj.get('next_page_id', '')
        row_count = len(obj.get('data'))
        log = f"Next page id:{obj.get('next_page_id')}, row count:{row_count}, status code:{response.status_code}"
        logging.info(log)
        LOGS.append(log)

    return content, status_codes


def main():
    status_count = 0
    max_try = 5
    while status_count != 1 and max_try > 0:
        try:
            content, status_codes = make_call()
            status_count = len(set(status_codes))
        except Exception as e:
            status_count = 0
            log = f'API call failed. Exception: {e}. Remaining tries: {max_try-1}'
            logging.warning(log)
            LOGS.append(log)
            max_try -= 1
            status = 'Error'
            continue
        if status_count != 1:
            log = f'There was an api error, status check: {status_codes}. Exception: {e}. Remaining tries: {max_try-1}'
            logging.warning(log)
            LOGS.append(log)
            status = 'Warning'
        else:
            status = 'Success'
        max_try -= 1

    if status == 'Success':
        # Uploading to BQ
        client = bigquery.Client.from_service_account_json(creds_path)
        table = 'alg-hn-insights.Zz_test.fraud_test'
        upload_to_bq(client=client, table=table, content=content)

    else:
        # Send email
        subject = f'BIAutoAlert: {status}'
        recipients = ['evanhao1990@mgmail.com', 'hzhang@harlemnext.com']
        body = '\n'.join(LOGS)

        sent_email(recipients, subject, body)


if __name__ == '__main__':
    main()

