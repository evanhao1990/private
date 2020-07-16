
### BigQuery API

This notebook contains the first draft with code for which we can access BigQuery API. We didn't have time to figure this out completely but it is possible to get data for *simple* queries.

Note: The main issue with the BigQuery API is that the queries used in BigQuery console may not work when using the API

#### Import packages


```python
# Imports the Google Cloud client library
from google.cloud import bigquery
from google.cloud import storage

from pandas.io import gbq

import os
import pandas as pd
```

#### 1) Example query & dataframe output - using .json file with account credentials


```python
# BQ client - using json downloaded from bigquery/google account
bigquery_client = bigquery.Client.from_service_account_json("GA-360-BigQuery-API-9e8e351372a6.json")
```


```python
query_job = bigquery_client.query("""
    SELECT 
        date,
        CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
        totals.hits as h,
        totals.pageviews as pv,
        IF(totals.transactions is not null,totals.transactions,0) as tr
    
    FROM `ga-360-bigquery-api.113663276.ga_sessions_20180722`

    WHERE geoNetwork.country = "Germany"
  
    GROUP BY 1,2,3,4,5
    Having pv > 1
    """)
```


```python
results = query_job.result()
df = results.to_dataframe()
print df.head()
```

           date                    session_code  h  pv  tr
    0  20180722   15322398251898762278104450828  2   2   0
    1  20180722   15322703553310430775873930695  2   2   0
    2  20180722   15322554705470727247785870556  2   2   0
    3  20180722   15322807323449929011709240403  2   2   0
    4  20180722  153228924411130470864952545023  2   2   0
    

#### 2) Example query & dataframe output - using .json file location (set in enviromental variables)


```python
print os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
```

    C:\Users\tiago.pimentel\Desktop\TATP\credentials\GA-360-BigQuery-API-9e8e351372a6.json
    


```python
# bigquery.Client() will look for GOOGLE_APPLICATION_CREDENTIALS in my enviromental variables - contains path of json file with BQ credentials
client = bigquery.Client()

query = """SELECT 
        date,
        CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
        totals.hits as h,
        totals.pageviews as pv,
        IF(totals.transactions is not null,totals.transactions,0) as tr
    FROM `ga-360-bigquery-api.113663276.ga_sessions_20180222`

    WHERE geoNetwork.country = 'Germany'
  
    GROUP BY 1,2,3,4,5"""
```


```python
test_job = client.query(query)
res = test_job.result()
print res.to_dataframe().head()
```

           date                   session_code  h  pv  tr
    0  20180222  15192557221919101246045552810  1 NaN   0
    1  20180222  15193199983884794609316666749  1 NaN   0
    2  20180222  15193228935418687525637290864  1 NaN   0
    3  20180222  15192547138874045374479592624  1 NaN   0
    4  20180222  15193039961023167456032502821  1 NaN   0
    

#### Example API request - using storage


```python
# Explicitly use service account credentials by specifying the private key
storage_client = storage.Client.from_service_account_json("GA-360-BigQuery-API-9e8e351372a6.json")

# Make an authenticated API request
buckets = list(storage_client.list_buckets())
print 'List of buckets: ', buckets
print ''
print 'Project ID: ', storage_client.project

```

    List of buckets:  [<Bucket: ana_dim_tables>, <Bucket: bba876>, <Bucket: bse-ga-test>, <Bucket: rory-visitid>, <Bucket: rui_add_basket_analysis>, <Bucket: tiago_test>, <Bucket: ziyan_nfm>]
    
    Project ID:  ga-360-bigquery-api
    

#### Extras

Messy code with some tests which, for now, do not work

####  Example query - works in BQ console but not in API


```python
# Import BQ query from txt file
cwd = os.getcwd()
top_folder = os.path.dirname(cwd)
data_folder = os.path.join(top_folder,'BQ API')
bq_file = os.path.join(data_folder, 'bq_test.txt')

myfile = open(bq_file)
query = myfile.read()
```


```python
print query
```

    ### Loading time analysis
    # group sessions per average loading time
    # average page views, pdps and plps
    # average conversion rate
    
    SELECT
    case when master.avg_load_time < 1.5 then '1-1.5'
        when master.avg_load_time between 1.5 and 2 then '1.5-2'
        when master.avg_load_time between 2.0 and 2.5 then '2-2.5'
        when master.avg_load_time between 2.5 and 3 then '2.5-3'
        when master.avg_load_time between 3 and 3.5 then '3-3.5'
        when master.avg_load_time between 3.5 and 4 then '3.5-4'
        when master.avg_load_time between 4 and 4.5 then '4-4.5'
        when master.avg_load_time between 4.5 and 5 then '4.5-5'
        when master.avg_load_time between 5 and 5.5 then '5-5.5'
        when master.avg_load_time between 5.5 and 6 then '5.5-6'
        when master.avg_load_time between 6 and 6.5 then '6-6.5'
        when master.avg_load_time between 6.5 and 7 then '6.5-7'
        when master.avg_load_time between 7 and 7.5 then '7-7.5'
        when master.avg_load_time between 7.5 and 8 then '7.5-8'
        when master.avg_load_time between 8 and 8.5 then '8-8.5'
        when master.avg_load_time between 8.5 and 9 then '8.5-9'
        when master.avg_load_time between 9 and 9.5 then '9-9.5'
        when master.avg_load_time between 9.5 and 10 then '9.5-10'
        else '>11' end as loading_time_group,
        
    count(master.session_code) as number_sessions,
    sum(master.pv)/count(master.session_code) as pv_per_sess,
    sum(master.PLPs)/count(master.session_code) as PLPs_per_session,
    sum(master.PDPs)/count(master.session_code) as PDPs_per_session,
    sum(master.tr)/count(master.session_code) as conversion
    
    FROM (
          SELECT
          date,
          
          CONCAT(CAST(visitId AS STRING), CAST(fullVisitorId AS STRING)) as session_code,
          
          totals.hits as h,
          totals.pageviews as pv,
          IF(totals.transactions is not null,totals.transactions,0) as tr,
          
          # average loading time per event (site speed event) sum of loading time on events in session (seconds) / number of events with time in session
          (sum(IF(hits.type = 'EVENT' AND hits.eventInfo.eventCategory ='Site speed', hits.eventInfo.eventValue, 0))/1000) / 
            sum(IF(hits.type = 'EVENT' AND hits.eventInfo.eventCategory ='Site speed', 1, 0)) as avg_load_time,
    
          count(IF(hits.product.productListName = 'main',hits.product.productListPosition,null)) as PLPs,
          count(IF(hits.product.productListName = 'packshot', hits.product.productListPosition, null)) as PDPs
    
          FROM (TABLE_DATE_RANGE([ga-360-bigquery-api:112804024.ga_sessions_], TIMESTAMP('2018-02-03'), TIMESTAMP('2018-02-15')))
    
          group by 1,2,3,4,5
         ) master
    
    Where master.PLPs > 0
      and master.avg_load_time between 1 and 10
    group by 1
    order by 1
    
    


```python
test_job = client.query(query)
res = test_job.result()
res.to_dataframe().head()
```


    ---------------------------------------------------------------------------

    BadRequest                                Traceback (most recent call last)

    <ipython-input-12-f90324e197c8> in <module>()
          1 test_job = client.query(query)
    ----> 2 res = test_job.result()
          3 res.to_dataframe().head()
    

    C:\ecommerce\lib\site-packages\google\cloud\bigquery\job.pyc in result(self, timeout, retry)
       2641             not complete in the given timeout.
       2642         """
    -> 2643         super(QueryJob, self).result(timeout=timeout)
       2644         # Return an iterator instead of returning the job.
       2645         if not self._query_results:
    

    C:\ecommerce\lib\site-packages\google\cloud\bigquery\job.pyc in result(self, timeout)
        686             self._begin()
        687         # TODO: modify PollingFuture so it can pass a retry argument to done().
    --> 688         return super(_AsyncJob, self).result(timeout=timeout)
        689 
        690     def cancelled(self):
    

    C:\ecommerce\lib\site-packages\google\api_core\future\polling.pyc in result(self, timeout)
        118             # pylint: disable=raising-bad-type
        119             # Pylint doesn't recognize that this is valid in this case.
    --> 120             raise self._exception
        121 
        122         return self._result
    

    BadRequest: 400 Syntax error: Expected "," or "]" but got ":" at [50:50]

