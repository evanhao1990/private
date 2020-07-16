
## TIAGO: append new data to - Dim adwords campaign ID code


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
get_historical = """SELECT * FROM sandbox_reporting.dim_adwords_campaign_id"""
```


```python
with DatabaseRedshift() as db:
    df = db.fetch(get_historical)
```


```python
print(df.info())
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 4319 entries, 0 to 4318
    Data columns (total 2 columns):
    campaign      4319 non-null object
    campaignid    4319 non-null int64
    dtypes: int64(1), object(1)
    memory usage: 67.6+ KB
    None
    


```python
print(df.head(2))
```

                                          campaign  campaignid
    0  Brand - Vero Moda - FI - Shopping - Generic  2067445337
    1                               Mamalicious SE   623883538
    

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
                  trafficSource.adwordsClickInfo.campaignId
            FROM `ga-360-bigquery-api.{}.ga_sessions_*`
            WHERE (_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) --last 7 days
                 AND trafficSource.adwordsClickInfo.campaignId IS NOT NULL
            GROUP BY 1,2,3
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

      sitebrand                                                 campaign  campaignId
    0        BS                 Bestseller - YAS - DK - Search - Branded   651922494
    1        BS                       Bestseller - DE - Search - Branded   634961904
    2        BS  Bestseller - Bestseller - DE - Search - Branded - Exact  2053904851
    3        BS                                            Bestseller DK   651922452
    4        BS        Bestseller - Bestseller - DE - Shopping - Branded  1739658694
    


```python
print(df_new.info())
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 1883 entries, 0 to 46
    Data columns (total 3 columns):
    sitebrand     1883 non-null object
    campaign      1883 non-null object
    campaignId    1883 non-null int64
    dtypes: int64(1), object(2)
    memory usage: 58.8+ KB
    None
    


```python
#only needed columns
df_new = df_new[['campaign','campaignId']]
df_new.rename(columns={'campaignId':'campaignid'},inplace=True)
print(df_new.head(2))
```

                                       campaign  campaignid
    0  Bestseller - YAS - DK - Search - Branded   651922494
    1        Bestseller - DE - Search - Branded   634961904
    

### 3. APPEND NEW DATA TO HISTORICAL ONE AND DROP DUPLICATES


```python
#append the two dataframes
df_updated = df_new.append(df)
print(df_updated.info())
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 6202 entries, 0 to 4318
    Data columns (total 2 columns):
    campaign      6202 non-null object
    campaignid    6202 non-null int64
    dtypes: int64(1), object(1)
    memory usage: 145.4+ KB
    None
    


```python
#reset index and drop duplicates, check size
df_updated = df_updated.reset_index(drop=True) #otherwise we have repetitive indexes due to the Concat function and thus we get duplicates when inserting in DWH
df_updated = df_updated.drop_duplicates() #we assume the enw and historical date have repetead fields
print(df_updated.info()) #we see that some rows were dropped
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 4368 entries, 0 to 6201
    Data columns (total 2 columns):
    campaign      4368 non-null object
    campaignid    4368 non-null int64
    dtypes: int64(1), object(1)
    memory usage: 102.4+ KB
    None
    

### 4. EXPORT DATAFRAME BACK TO REDSHIFT REPLACING THE CURRENT HISTORICAL DATA


```python
with DatabaseRedshift() as db:
    db.insert(df=df_updated, schema='sandbox_reporting',table='dim_adwords_campaign_id', s3_csv_name='dim_adwords_campaign_id.csv', delimiter='~', mode='replace')
```

    Table sandbox_reporting.dim_adwords_campaign_id created successfully
    Data written to sandbox_reporting.dim_adwords_campaign_id
    
