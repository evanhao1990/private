"""
To be built in the future:
    -metric filters
    -dimension and metric ordering
    -pagination if needed
Google documents https://developers.google.com/analytics/devguides/reporting/core/v4/basics
"""

from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import logging
import numpy as np
import pandas as pd
from datetime import date
from dateutil.relativedelta import *
from src.configure_logging import configure_logging
logger = configure_logging(logger_name=__name__)


class GoogleAnalytics(object):

    def __init__(self, api_name='analyticsreporting', api_version='v4'):
        """
        Default API is Google Analytics reporting API v4. Only when we are generating dim_ua_profile table we
        adjust api_name to 'analytics' and api_version to 'v3' in order to connect with GA management API.

        Parameters
        ----------
        api_name: string
            The name of the api to connect to. ('analyticsreporting' or 'analytics')

        api_version: string
            The api version to connect to. ('v4' for 'analyticsreporting' or 'v3' for 'analytics')

        """
        self.scopes = ['https://www.googleapis.com/auth/analytics']
        self.api_name = api_name
        self.api_version = api_version
        self.service = self._build_service()
        # self.account = 66188758

    def _get_credentials(self):
        """
        Read service account credentials from environment variables/ json file

        Returns
        -------
        credentials for service account

        """

        key_path = os.path.join(os.getcwd(), 'google-config', 'ga_service_account.json')

        try:
            credentials = service_account.Credentials.from_service_account_file(key_path, scopes=self.scopes)
        except Exception as e:
            logging.error(f"Failed to read credentials. Exception : {e}")
            raise

        return credentials

    def _build_service(self):
        """
        Get a service that communicates to a Google API.

        Returns
        -------
        A service that is connected to the specified API.

        """

        try:
            credentials = self._get_credentials()
        except Exception as e:
            logging.error(f"Failed to read credentials. Exception : {e}")
            raise

        try:
            # Build the service object.
            service = build(self.api_name, self.api_version, credentials=credentials)
        except Exception as e:
            logging.error(f"Failed to read build connection. Exception : {e}")
            raise

        return service

    def _format_date_range(self, date_ranges=None, days=None, months=None):
        """
        Format input  date range into GA query format, with the possibility of choosing last x days/months

        Parameters
        ----------
        date_ranges: list
            The specific date range of the query
        days: int
            x days before today till yesterday.
            eg. if today = '2020-02-04', days=-7 generates date range ['2019-01-28', '2020-02-03']
        months: int
            x month before today till yesterday.
            eg. if today = '2020-02-04', month=-7 generates date range ['2019-07-01', '2020-02-03']
        Returns
        -------
        Formated date range [[start_date, end_date],..]
        """

        yesterday = date.today() + relativedelta(days=-1)

        if date_ranges:
            if type(date_ranges) == list:
                if type(date_ranges[0]) == list:
                    d_range = date_ranges
                else:
                    d_range = [date_ranges]

        if days:
            days = int(days)
            if days > 0:
                logger.error('Future time selected')
                exit()
            d0 = date.today() + relativedelta(days=days)
            d_range = [d0, yesterday]
            d_range = [[x.strftime("%Y-%m-%d") for x in d_range]]

        if months:
            months = int(months)
            if months > 0:
                logger.error('Future time selected')
                exit()
            d0 = date.today() + relativedelta(months=months)
            d0 = d0.replace(day=1)
            d_range = [d0, yesterday]
            d_range = [[x.strftime("%Y-%m-%d") for x in d_range]]

        return d_range

    def _format_request(self, view_id, dimensions, metrics,  date_ranges, dimension_filters, dimension_filter_operator):
        """
        Put the dimensions/metrics/filters and etc into the format that fits GA API query

        Returns
        -------
        Formatted Google analytics request
        """
        request = [
            {
                'viewId': str(view_id),
                'dateRanges': [{'startDate': d[0], 'endDate': d[1]} for d in date_ranges],
                'dimensions': [{'name': name} for name in dimensions],
                'metrics': [{'expression': exp} for exp in metrics],
                'dimensionFilterClauses': [
                    {
                        'operator': dimension_filter_operator,
                        'filters': [{'dimensionName': fil[0], 'operator': fil[1], 'expressions': fil[2]} for fil in
                                    dimension_filters]
                    }
                ]
            }
        ]

        return request

    def _to_dataframe(self, data, dimensions, metrics):
        """
        Convert Google analytics response into pandas DataFrame
        Parameters
        ----------
        data: Google Analytics response object

        Returns
        -------
        DataFrame

        """
        data_dic = {f"{i}": [] for i in dimensions + metrics}
        for report in data.get('reports', []):
            rows = report.get('data', {}).get('rows', [])
            for row in rows:
                for i, key in enumerate(dimensions):
                    data_dic[key].append(row.get('dimensions', [])[i])  # Get dimensions
                date_range_values = row.get('metrics', [])
                for values in date_range_values:
                    all_values = values.get('values', [])  # Get metric values
                    for i, key in enumerate(metrics):
                        data_dic[key].append(all_values[i])

        df = pd.DataFrame(data=data_dic)
        df.columns = [col.split(':')[-1] for col in df.columns]

        return df

    def fetch(self, view_id, dimensions, metrics, dimension_filters=[], dimension_filter_operator='or', **date_ranges):
        """
        Fetch required data from Google analytics views and convert the result into a pandas dataframe.

        Parameters
        ----------
        view_id: int/string of numbers
            The id of the individual views, see more at https://ga-dev-tools.appspot.com/account-explorer/
        dimensions: list
            List of Google Analytics's dimentsion, see more at https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/
        metrics: list
            List of Google Analytics's metrics, see more at https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/
        dimension_filters: nested list
            List of list of filters to be applied on dimensions
            Single Filter format:[['dimensions','operator','expression']].
                common operators: REGEXP, BEGINS_WITH, ENDS_WITH, PARTIAL, EXACT, IN_LIST
            Multiple filters format: [[filter A],[filter B],...]
            see more at https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#dimensionfilterclause
        date_ranges: nested list
            2 elements list, with first element as start date, second element as end date.
            Multiple date ranges: [[start_date1,end_date1],[start_date2,end_date2],...]
        dimension_filter_operator: string
            Logical operator between dimension filters. ('OR', 'AND')

        Returns
        -------
        DataFrame

        """

        date_ranges = self._format_date_range(**date_ranges)

        request = self._format_request(view_id=view_id, dimensions=dimensions, metrics=metrics,
                                       dimension_filters=dimension_filters,
                                       dimension_filter_operator=dimension_filter_operator, date_ranges=date_ranges)

        max_try = 5
        while max_try >= 0:
            try:
                data = self.service.reports().batchGet(body={'reportRequests': request}).execute()
                df = self._to_dataframe(data=data, dimensions=dimensions, metrics=metrics)
                break
            except Exception as e:
                max_try -= 1
                if max_try == 0:
                    logger.error(f'Fetching {view_id} FAILED Exception: {e}')
                    df = pd.DataFrame()
                    break
                logger.warning(f'Fetching {view_id} failed, remaining tries: {max_try}. Exception: {e}')

        return df
