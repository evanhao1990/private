"""
To be built in the future:
    -metric filters
    -dimension and metric ordering
    -pagination if needed
Google documents https://developers.google.com/analytics/devguides/reporting/core/v4/basics
"""

from apiclient.discovery import build
from google.oauth2 import service_account
import os
import logging
import numpy as np
import pandas as pd


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

        key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_GA')

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

    def fetch(self, view_id, dimensions, metrics, date_ranges, dimension_filters=[], dimension_filter_operator='or'):
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

        request = self._format_request(view_id, dimensions, metrics, date_ranges, dimension_filters, dimension_filter_operator)
        try:
            data = self.service.reports().batchGet(body={'reportRequests': request}).execute()
        except Exception as e:
            logging.error(f'Fail to fetch data. Exception: {e}')
            raise

        df = self._to_dataframe(data=data, dimensions=dimensions, metrics=metrics)
        return df
