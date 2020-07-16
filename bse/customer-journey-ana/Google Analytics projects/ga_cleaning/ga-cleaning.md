```python
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import logging
import numpy as np
import pandas as pd
from datetime import date
from dateutil.relativedelta import *


scopes = ['https://www.googleapis.com/auth/analytics.edit']
key_path = 'C:\ecommerce\keys\ga_service_account.json'
credentials = service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
service = build('analytics', 'v3', credentials=credentials)
```

### Get all ua profiles


```python
def read_profiles_per_property(service, property):
    """
    Read ua_profiles from property and format into DataFrame
    """
    property_name = property['name']                   # property name
    pid = property['id']                               # property id

    if property_name == 'Roll-Up Property':
        property_source = 'N/A'                        # get property source (app or web) and type (brand or market)
        property_type = 'N/A'
    else:
        property_source = property_name.split(' ')[2]  # property source (app or web)
        property_type = property_name.split(' ')[4]    # property type (brand or market)

    # get all profiles in this property
    profiles = service.management().profiles().list(accountId='66188758', webPropertyId=pid).execute()

    ua_profile_id = [prf['id'] for prf in profiles.get('items')]  # list of all profile id
    ua_profile_name = [prf['name'] for prf in profiles.get('items')]  # list of all profile name
    
    # put into DataFrame
    df = pd.DataFrame({'ua_profile_id': ua_profile_id,
                       'name': ua_profile_name,
                       'property_name': property_name,
                       'property_source': property_source,
                       'property_type': property_type})
    df['pid'] = pid
    return df
```


```python
properties = service.management().webproperties().list(accountId='66188758').execute()
```


```python
df_all = pd.DataFrame()
for item in properties.get('items'):

    df_profile = read_profiles_per_property(service, item)
    df_all = df_all.append(df_profile, ignore_index=True)
```


```python
df_all
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ua_profile_id</th>
      <th>name</th>
      <th>property_name</th>
      <th>property_source</th>
      <th>property_type</th>
      <th>pid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>106630793</td>
      <td>A - App, BC - Overview</td>
      <td>3 - App - Brand - Bestseller.com</td>
      <td>App</td>
      <td>Brand</td>
      <td>UA-66188758-1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>106645875</td>
      <td>B - App, BC - United Kingdom</td>
      <td>3 - App - Brand - Bestseller.com</td>
      <td>App</td>
      <td>Brand</td>
      <td>UA-66188758-1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>106647798</td>
      <td>B - App, BC - Switzerland</td>
      <td>3 - App - Brand - Bestseller.com</td>
      <td>App</td>
      <td>Brand</td>
      <td>UA-66188758-1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>106649030</td>
      <td>B - App, BC - Denmark</td>
      <td>3 - App - Brand - Bestseller.com</td>
      <td>App</td>
      <td>Brand</td>
      <td>UA-66188758-1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>106649048</td>
      <td>B - App, BC - Belgium</td>
      <td>3 - App - Brand - Bestseller.com</td>
      <td>App</td>
      <td>Brand</td>
      <td>UA-66188758-1</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>881</th>
      <td>179325638</td>
      <td>Web - Market - Netherlands (Non User-ID reports)</td>
      <td>Roll-Up Property</td>
      <td>N/A</td>
      <td>N/A</td>
      <td>UA-66188758-54</td>
    </tr>
    <tr>
      <th>882</th>
      <td>179326120</td>
      <td>Web - Market - Italy (Non User-ID reports)</td>
      <td>Roll-Up Property</td>
      <td>N/A</td>
      <td>N/A</td>
      <td>UA-66188758-54</td>
    </tr>
    <tr>
      <th>883</th>
      <td>179332248</td>
      <td>Web - Market - Spain (Non User-ID reports)</td>
      <td>Roll-Up Property</td>
      <td>N/A</td>
      <td>N/A</td>
      <td>UA-66188758-54</td>
    </tr>
    <tr>
      <th>884</th>
      <td>179332487</td>
      <td>Web - Market - Overview (Non User-ID reports)</td>
      <td>Roll-Up Property</td>
      <td>N/A</td>
      <td>N/A</td>
      <td>UA-66188758-54</td>
    </tr>
    <tr>
      <th>885</th>
      <td>189379348</td>
      <td>Web - Market - Poland (Non User-ID reports)</td>
      <td>Roll-Up Property</td>
      <td>N/A</td>
      <td>N/A</td>
      <td>UA-66188758-54</td>
    </tr>
  </tbody>
</table>
<p>886 rows × 6 columns</p>
</div>



### Delete userid accounts


```python
df_user_id = df_all.loc[(df_all.name.str.find('(User ID')>=0)&(df_all.name.str.find('Channa')<0)]
df_user_id.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ua_profile_id</th>
      <th>name</th>
      <th>property_name</th>
      <th>property_source</th>
      <th>property_type</th>
      <th>pid</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
# they are all deleted now
len(df_user_id)
```




    0




```python
l = list(zip(df_user_id.ua_profile_id,df_user_id.pid,df_user_id.name))
```


```python
import time
for ua_id, pid, name in l:
    print(f'Deleting {name}')
    service.management().profiles().delete(accountId='66188758', webPropertyId=pid, profileId=ua_id).execute()
    time.sleep(0.1)
```

    Deleting B - Web, GBR - Bianco.com (User ID)
    Deleting B - Web, SLH - Austria (User ID)
    Deleting B - Web, SLH - Sweden (User ID)
    Deleting B - Web, SLH - United Kingdom (User ID)
    Deleting B - Web, SLH - Denmark (User ID)
    Deleting B - Web, SLH - Spain (User ID)
    Deleting B - Web, SLH - Switzerland (User ID)
    Deleting C - Web, SLH - Rest of World (User ID)
    Deleting B - Web, SLH - Norway (User ID)
    Deleting B - Web, SLH - Germany (User ID)
    Deleting B - Web, SLH - France (User ID)
    Deleting B - Web, SLH - Finland (User ID)
    Deleting B - Web, SLH - Belgium (User ID)
    Deleting B - Web, SLH - Netherlands (User ID)
    Deleting B - Web, SLH - Italy (User ID)
    Deleting B - Web, SLH - Ireland (User ID)
    Deleting B - Web, BI - Norway (User ID)
    Deleting B - Web, BI - France (User ID)
    Deleting B - Web, BI - Germany (User ID)
    Deleting B - Web, BI - Netherlands (User ID)
    Deleting B - Web, BI - United Kingdom (User ID)
    Deleting B - Web, BI - Sweden (User ID)
    Deleting B - Web, BI - Austria (User ID)
    Deleting B - Web, BI - Switzerland (User ID)
    Deleting B - Web, BI - Denmark (User ID)
    Deleting B - Web, BI - Italy (User ID)
    Deleting B - Web, BI - Spain (User ID)
    Deleting B - Web, BI - Belgium (User ID)
    Deleting B - Web, BI - Ireland (User ID)
    Deleting C - Web, BI - Rest of World (User ID)
    

## Filters

### List all filters 


```python
filters = service.management().filters().list(accountId='66188758').execute()
```


```python
# a example of how filters are displayed
filters.get('items')[0]
```




    {'id': '22488494',
     'kind': 'analytics#filter',
     'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/filters/22488494',
     'accountId': '66188758',
     'name': 'CountryCode = NLD',
     'type': 'INCLUDE',
     'created': '2015-08-11T13:47:32.257Z',
     'updated': '2015-08-13T12:33:56.849Z',
     'parentLink': {'type': 'analytics#account',
      'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758'},
     'includeDetails': {'kind': 'analytics#filterExpression',
      'field': 'CUSTOM_DIMENSION',
      'matchType': 'MATCHES',
      'expressionValue': 'NLD',
      'caseSensitive': False,
      'fieldIndex': 1}}




```python
# convert the dic formatted info to DataFrame for readability.
id_ = [x.get('id') for x in filters.get('items')]
name_ = [x.get('name') for x in filters.get('items')]
type_ = [x.get('type') for x in filters.get('items')]

field_ = [x.get(x.get('type').lower()+'Details').get('field') for x in filters.get('items')]
matchtype_ = [x.get(x.get('type').lower()+'Details').get('matchType') for x in filters.get('items')]
expvalue_ = [x.get(x.get('type').lower()+'Details').get('expressionValue') for x in filters.get('items')]
casesensitive_ = [x.get(x.get('type').lower()+'Details').get('caseSensitive') for x in filters.get('items')]
fieldindex_ = [x.get(x.get('type').lower()+'Details').get('fieldIndex') for x in filters.get('items')]
df_filter = pd.DataFrame({'id':id_
                          ,'name':name_
                          ,'type':type_
                          ,'field':field_
                          ,'matchtype':matchtype_
                          ,'expvalue':expvalue_
                          ,'casesensitive':casesensitive_
                          ,'fileldindex':fieldindex_
                           })
```


```python
# field index = 1 actually means cd.index = 1
df_filter
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>type</th>
      <th>field</th>
      <th>matchtype</th>
      <th>expvalue</th>
      <th>casesensitive</th>
      <th>fileldindex</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>22488494</td>
      <td>CountryCode = NLD</td>
      <td>INCLUDE</td>
      <td>CUSTOM_DIMENSION</td>
      <td>MATCHES</td>
      <td>NLD</td>
      <td>False</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>22526432</td>
      <td>CountryCode = DEU</td>
      <td>INCLUDE</td>
      <td>CUSTOM_DIMENSION</td>
      <td>MATCHES</td>
      <td>DEU</td>
      <td>False</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>22528432</td>
      <td>CountryCode = BEL</td>
      <td>INCLUDE</td>
      <td>CUSTOM_DIMENSION</td>
      <td>MATCHES</td>
      <td>BEL</td>
      <td>False</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>22528529</td>
      <td>CountryCode = IRL</td>
      <td>INCLUDE</td>
      <td>CUSTOM_DIMENSION</td>
      <td>MATCHES</td>
      <td>IRL</td>
      <td>False</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>22528819</td>
      <td>CountryCode = SWE</td>
      <td>INCLUDE</td>
      <td>CUSTOM_DIMENSION</td>
      <td>MATCHES</td>
      <td>SWE</td>
      <td>False</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>201</th>
      <td>80755345</td>
      <td>SiteCoutry=Denmark(new)</td>
      <td>INCLUDE</td>
      <td>CUSTOM_DIMENSION</td>
      <td>MATCHES</td>
      <td>DK</td>
      <td>False</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>202</th>
      <td>80765258</td>
      <td>SiteCoutry=Germany(new)</td>
      <td>INCLUDE</td>
      <td>CUSTOM_DIMENSION</td>
      <td>MATCHES</td>
      <td>DE</td>
      <td>False</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>203</th>
      <td>80765364</td>
      <td>Exclude Bestseller Traffic 83.151.148(new)</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>MATCHES</td>
      <td>^83\.151\.148\.([1-9]|[1-9][0-9]|1([0-9][0-9])...</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>204</th>
      <td>80770373</td>
      <td>Exclude Ikwilbovenaan(new)</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>EQUAL</td>
      <td>185.127.111.252</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>205</th>
      <td>80778643</td>
      <td>Exclude QuantAds Traffic(new)</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>MATCHES</td>
      <td>X.X.X.X</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>206 rows × 8 columns</p>
</div>



### Create Site Country Filters

There are 3 different kinds of filters (include, exclude, advanced), so I did them in 3 sections.


```python
countries= [('Austria','AT'),
             ('Belgium','BE'),
             ('Denmark','DK'),
             ('Finland','FI'),
             ('France','FR'),
             ('Germany','DE'),
             ('Ireland','IE'),
             ('Italy','IT'),
             ('Netherlands','NL'),
             ('Norway','NO'),
             ('Poland','PL'),
             ('Spain','ES'),
             ('Sweden','SE'),
             ('Switzerland','CH'),
             ('United Kingdom','GB'),
             ('Rest of world','RW')]
```


```python
# inserting filters for every country
for countryname, countrycode in countries:
    service.management().filters().insert(
          accountId='66188758',
          body={
              'name': f'SiteCoutry={countryname}(new)',
              'type': 'INCLUDE',
              'includeDetails': {
                  'field': 'CUSTOM_DIMENSION',
                  'matchType': 'MATCHES',
                  'expressionValue': countrycode,
                  'caseSensitive': False,
                  'fieldIndex': 1
                  }
          }
      ).execute()
```

### Create Exclude IP filters


```python
# This are exsiting IP filters in GA
exc_l=['39344710','26455472','26111742','39351209','47393090','26133671','55936743']
df_exc = df_filter.loc[df_filter.id.isin(exc_l)]
df_exc
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
      <th>type</th>
      <th>field</th>
      <th>matchtype</th>
      <th>expvalue</th>
      <th>casesensitive</th>
      <th>fileldindex</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>109</th>
      <td>26111742</td>
      <td>Exclude Site Confidence Browser Bot</td>
      <td>EXCLUDE</td>
      <td>BROWSER</td>
      <td>MATCHES</td>
      <td>SiteCon Browser</td>
      <td>True</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>111</th>
      <td>26133671</td>
      <td>Exclude Bestseller Traffic 83.151.148</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>MATCHES</td>
      <td>^83\.151\.148\.([1-9]|[1-9][0-9]|1([0-9][0-9])...</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>128</th>
      <td>26455472</td>
      <td>Exclude QuantAds Traffic</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>MATCHES</td>
      <td>X.X.X.X</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>146</th>
      <td>39344710</td>
      <td>Exclude Bestseller 80.197.218.183</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>MATCHES</td>
      <td>80.197.218.183</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>147</th>
      <td>39351209</td>
      <td>Exclude Bestseller Traffic 185.9.141</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>MATCHES</td>
      <td>^185\.9\.141\.([1-9]{1}|1[0-4]{1})$</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>165</th>
      <td>47393090</td>
      <td>Exclude Bestseller - Amsterdam</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>MATCHES</td>
      <td>83.219.74.155</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>173</th>
      <td>55936743</td>
      <td>Exclude Ikwilbovenaan</td>
      <td>EXCLUDE</td>
      <td>GEO_IP_ADDRESS</td>
      <td>EQUAL</td>
      <td>185.127.111.252</td>
      <td>False</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
# I used all the origin settings here to create our own new fitlers
# The only difference is I changed "Exclude Site Confidence Browser Bot" fitler to casesensitive = False
l = list(zip(df_exc.name,df_exc.type,df_exc.field,df_exc.matchtype,df_exc.expvalue,df_exc.casesensitive))
for name,type,field,matchtype,expvalue,casesensitive in l:
    service.management().filters().insert(
          accountId='66188758',
          body={
              'name': f'{name}(new)',
              'type': 'EXCLUDE',
              'excludeDetails': {
                  'field': field,
                  'matchType': matchtype,
                  'expressionValue': expvalue,
                  'caseSensitive': False
                  }
          }
      ).execute()
```

### Create Whole URL filter


```python
# whole URL filter also copied from exsiting fitler 
service.management().filters().insert(
          accountId='66188758',
          body={
              'name': 'Whole URL(new)',
              'type': 'ADVANCED',
              'advancedDetails':{
                  'fieldA': 'PAGE_REQUEST_URI',
                  'extractA': '(.*)',
                  'fieldB': 'PAGE_HOSTNAME',
                  'extractB': '(.*)',
                  'outputToField': 'PAGE_REQUEST_URI',
                  'outputConstructor': '$B1$A1',
                  'fieldARequired': True,
                  'fieldBRequired': True,
                  'overrideOutputField': True,
                  'caseSensitive': False}
          }
      ).execute()
```




    {'id': '80729795',
     'kind': 'analytics#filter',
     'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/filters/80729795',
     'accountId': '66188758',
     'name': 'Whole URL(new)',
     'type': 'ADVANCED',
     'created': '2020-03-16T17:12:23.568Z',
     'updated': '2020-03-16T17:12:23.568Z',
     'parentLink': {'type': 'analytics#account',
      'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758'},
     'advancedDetails': {'fieldA': 'PAGE_REQUEST_URI',
      'extractA': '(.*)',
      'fieldB': 'PAGE_HOSTNAME',
      'extractB': '(.*)',
      'outputToField': 'PAGE_REQUEST_URI',
      'outputConstructor': '$B1$A1',
      'fieldARequired': True,
      'fieldBRequired': True,
      'overrideOutputField': True,
      'caseSensitive': False}}



### Create new views and link to filters


```python
# First find all the new filters we just created ('new' in filter name)
df_new_filters = df_filter.loc[df_filter.name.str.find('new')>0]
exclude_filter_ids = df_new_filters.loc[df_new_filters.type=='EXCLUDE','id'].values.tolist()
advanced_filter_ids = df_new_filters.loc[df_new_filters.type=='ADVANCED','id'].values.tolist()
```


```python
propertyid = 'UA-66188758-51' # SL property as a test
```


```python
# create country views for SL property and link them directly with our new fitlers
for countryname, countrycode in countries:

    # prepare all filter ids
    country_filter_id = df_new_filters.loc[df_new_filters.expvalue==countrycode,'id'].values.tolist()
    all_filters_ids = exclude_filter_ids + advanced_filter_ids+country_filter_id

    # create view
    new_view = service.management().profiles().insert(
          accountId='66188758',
          webPropertyId=propertyid,
          body={
              'name': f'Web - SL - {countryname}',
              'eCommerceTracking': True,
          }
      ).execute()

    new_view_id = new_view.get('id')

    # link filters to view
    for filter_id in all_filters_ids:
        service.management().profileFilterLinks().insert(
              accountId='66188758',
              webPropertyId=propertyid,
              profileId = new_view_id,
              body={'filterRef': {'id': filter_id}}
          ).execute()
```
