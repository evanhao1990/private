
## BCM-779- Create Append script for dim_web_campaign table
#### ticket: https://bestseller.jira.com/browse/BCM-779
#### METHODOLOGY:
- Create Python Script that fetches historical data from Sandbox Reporting as 'Dim Marketing Campaign' (last time ran on Monday 30th September)
- Run the script for the last 7 days and append it to the above
- drop duplicates and replace the table with the updated one 
- Set warning to run this script every Monday to update the table (starting from Monday 7th October)


```python
import os, sys
#figures directory
fig_dir = os.path.join(os.path.dirname(os.getcwd()), 'figures')
data_dir = os.path.join(os.path.dirname(os.getcwd()), 'data')

#packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from statsmodels.stats.proportion import proportions_ztest
from scipy.stats import chi2_contingency
import time
import matplotlib.ticker as plticker

#Analytics-toolkit
from database_connector.connector import DatabaseRedshift
from plotting.plotting import *
from bigquery_api.connector import BigQuery

#settings 
%matplotlib inline
#plt.style.library['bsestyle_light']
plt.style.use('bsestyle_light')
pd.options.mode.chained_assignment = None

#make output tables wider and show more when columns has a lot of content (NO '...')
pd.set_option('display.width', 1200)
pd.set_option('max_colwidth', 200)
```

### 1. FETCH CURRENT TABLE FROM SANDBOX


```python
get_historical = """SELECT * FROM sandbox_reporting.dim_marketing_campaign"""
```


```python
with DatabaseRedshift() as db:
    df = db.fetch(get_historical)
```


```python
print(df.info())
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 289361 entries, 0 to 289360
    Data columns (total 4 columns):
    campaign           286588 non-null object
    source             289361 non-null object
    medium             289361 non-null object
    channelgrouping    289361 non-null object
    dtypes: object(4)
    memory usage: 8.8+ MB
    None
    

### 2. RUN SCRIPT FOR THE PAST 7 DAYS FOR ALL STOREFRONTS


```python
bq_account_dict = {'BS': '112804024',
                   'MM':'113556952',
                   'JL':'113608165',
                   'JR':'113608269',
                   'ON':'113635108',
                   'JJ':'113663276',
                   'VM':'113676549',
                   'VL':'113698901',
                   'BI':'124406727',
                   'NI':'113595925',
                   'NM':'113606041',
                   'OC':'113614154',
                   'OS':'113613067',
                   'PC':'113671938',
                   'SL':'159210175',
                   'YA':'113684854'}
```


```python
bq_query = """
            SELECT '{}' AS sitebrand,
                  trafficSource.campaign,
                  trafficSource.source,
                  trafficSource.medium,
                  channelGrouping AS channelgrouping
            FROM `ga-360-bigquery-api.{}.ga_sessions_*`
            WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) --last 7 days
            GROUP BY 1,2,3,4,5
"""
```


```python
bq = BigQuery()

df_appended = []
for account in bq_account_dict:
    dfi =bq.fetch(bq_query.format(account, bq_account_dict[account]))
    df_appended.append(dfi)

df_new = pd.concat(df_appended)
```


```python
print(df_new.head())
```

      sitebrand                                                            campaign        source               medium       channelgrouping
    0        BS  Bestseller - Bestseller - DE - Search - Branded - Other Categories        google                  cpc       Google Shopping
    1        BS                                                           (not set)        google                  cpc       Google Shopping
    2        BS              Bestseller - Vero Moda - BE - Search - Branded - Dutch        google                  cpc       Google Shopping
    3        BS                                                           (not set)  confirmation  transactional_email  Transactional_Emails
    4        BS                    Bestseller - J Lindeberg - FI - Search - Branded        google                  cpc       Google Shopping
    


```python
print(df_new.info())
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 13721 entries, 0 to 264
    Data columns (total 5 columns):
    sitebrand          13721 non-null object
    campaign           13721 non-null object
    source             13721 non-null object
    medium             13721 non-null object
    channelgrouping    13721 non-null object
    dtypes: object(5)
    memory usage: 643.2+ KB
    None
    


```python
#exclude sitebrand
df_new = df_new[['campaign','source','medium','channelgrouping']]
print(df_new.head(2))
```

                                                                 campaign  source medium  channelgrouping
    0  Bestseller - Bestseller - DE - Search - Branded - Other Categories  google    cpc  Google Shopping
    1                                                           (not set)  google    cpc  Google Shopping
    

### 3. APPEND NEW DATA TO HISTORICAL ONE AND DROP DUPLICATES


```python
#append the two dataframes
df_updated = df_new.append(df)
print(df_updated.info())
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 303082 entries, 0 to 289360
    Data columns (total 4 columns):
    campaign           300309 non-null object
    source             303082 non-null object
    medium             303082 non-null object
    channelgrouping    303082 non-null object
    dtypes: object(4)
    memory usage: 11.6+ MB
    None
    


```python
#reset index and drop duplicates, check size
df_updated = df_updated.reset_index(drop=True) #otherwise we have repetitive indexes due to the Concat function and thus we get duplicates when inserting in DWH
df_updated = df_updated.drop_duplicates() #we assume the enw and historical date have repetead fields
print(df_updated.info()) #we see that some rows were dropped
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 291686 entries, 0 to 303081
    Data columns (total 4 columns):
    campaign           288913 non-null object
    source             291686 non-null object
    medium             291686 non-null object
    channelgrouping    291686 non-null object
    dtypes: object(4)
    memory usage: 11.1+ MB
    None
    

### 4. EXPORT DATAFRAME BACK TO REDSHIFT REPLACING THE CURRENT HISTORICAL DATA


```python
with DatabaseRedshift() as db:
    db.insert(df=df_updated, schema='sandbox_reporting',table='dim_marketing_campaign', s3_csv_name='dim_marketing_campaign.csv', delimiter='~', mode='replace')
```

    Table sandbox_reporting.dim_marketing_campaign created successfully
    Saved dwh/dim_marketing_campaign.csv to bse-analytics-dev.bseint.io
    Data written to sandbox_reporting.dim_marketing_campaign
    
