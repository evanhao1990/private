## Brief Intro
Normally the structure of GA tables is:

    Account --> Property --> View 
    
A real example would be:

    Bestseller (Universal) --> 1 - Web - Brand - Only --> A - Web, ON - Overview(3 Non-User ID)
    
    To understand better you can visit https://ga-dev-tools.appspot.com/account-explorer/ and login in with GA account
    
What we do to get dim_ua_profile table:
    
    Firstly we loop through all the Properties within the main account (Bestseller (Universal)) to get information related with all the views under each Property. 
    
    Secondly we filter for the Views' profiles we want -- we only need brand-country level Views to pull data from.
    
    Thirdly we add more columns with information we need -- sitebrand, sitecountry, table_updated_time and etc..

#### Example information inside a "Property" segment


```python
# first 2 properties in Bestseller (Universal)
analytics.management().webproperties().list(accountId=66188758).execute().get('items')[:2]
```




    [{'id': 'UA-66188758-1',
      'kind': 'analytics#webproperty',
      'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1',
      'accountId': '66188758',
      'internalWebPropertyId': '102587179',
      'name': '3 - App - Brand - Bestseller.com',
      'level': 'PREMIUM',
      'profileCount': 15,
      'industryVertical': 'SHOPPING',
      'defaultProfileId': '106630793',
      'dataRetentionTtl': 'INDEFINITE',
      'dataRetentionResetOnNewActivity': True,
      'permissions': {'effective': ['COLLABORATE', 'EDIT', 'READ_AND_ANALYZE']},
      'created': '2015-08-11T11:55:30.584Z',
      'updated': '2019-09-10T21:09:41.449Z',
      'parentLink': {'type': 'analytics#account',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758'},
      'childLink': {'type': 'analytics#profiles',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1/profiles'}},
     {'id': 'UA-66188758-2',
      'kind': 'analytics#webproperty',
      'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-2',
      'accountId': '66188758',
      'internalWebPropertyId': '102607226',
      'name': '3 - App - Brand - Jack & Jones',
      'websiteUrl': '--',
      'level': 'PREMIUM',
      'profileCount': 15,
      'industryVertical': 'SHOPPING',
      'defaultProfileId': '106648945',
      'dataRetentionTtl': 'INDEFINITE',
      'dataRetentionResetOnNewActivity': True,
      'permissions': {'effective': ['COLLABORATE', 'EDIT', 'READ_AND_ANALYZE']},
      'created': '2015-08-11T12:35:25.838Z',
      'updated': '2019-09-10T21:07:37.068Z',
      'parentLink': {'type': 'analytics#account',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758'},
      'childLink': {'type': 'analytics#profiles',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-2/profiles'}}]



#### Example information inside a "View" segment


```python
# first 2 Views in '3 - App - Brand - Bestseller.com'
analytics.management().profiles().list(accountId='66188758',webPropertyId='UA-66188758-1').execute().get('items')[:2]
```




    [{'id': '106630793',
      'kind': 'analytics#profile',
      'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1/profiles/106630793',
      'accountId': '66188758',
      'webPropertyId': 'UA-66188758-1',
      'internalWebPropertyId': '102587179',
      'name': 'A - App, BC - Overview',
      'currency': 'EUR',
      'timezone': 'Europe/Copenhagen',
      'websiteUrl': '--',
      'defaultPage': '--',
      'type': 'APP',
      'permissions': {'effective': ['COLLABORATE', 'EDIT', 'READ_AND_ANALYZE']},
      'created': '2015-08-11T11:55:30.584Z',
      'updated': '2018-05-31T07:02:15.931Z',
      'eCommerceTracking': False,
      'botFilteringEnabled': True,
      'parentLink': {'type': 'analytics#webproperty',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1'},
      'childLink': {'type': 'analytics#goals',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1/profiles/106630793/goals'}},
     {'id': '106645875',
      'kind': 'analytics#profile',
      'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1/profiles/106645875',
      'accountId': '66188758',
      'webPropertyId': 'UA-66188758-1',
      'internalWebPropertyId': '102587179',
      'name': 'B - App, BC - United Kingdom',
      'currency': 'GBP',
      'timezone': 'Etc/GMT',
      'websiteUrl': '--',
      'defaultPage': '--',
      'type': 'APP',
      'permissions': {'effective': ['COLLABORATE', 'EDIT', 'READ_AND_ANALYZE']},
      'created': '2015-08-11T13:33:34.182Z',
      'updated': '2018-05-31T07:02:15.931Z',
      'eCommerceTracking': False,
      'botFilteringEnabled': True,
      'parentLink': {'type': 'analytics#webproperty',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1'},
      'childLink': {'type': 'analytics#goals',
       'href': 'https://www.googleapis.com/analytics/v3/management/accounts/66188758/webproperties/UA-66188758-1/profiles/106645875/goals'}}]



## Main


```python
from apiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime

# initial setting
SERVICE_ACCOUNT_FILE = 'C:\\Users\\hao.zhang\\Desktop\\hao.zhang\\Python\\ga-service-account\\account\\service_account.json'
scopes = ['https://www.googleapis.com/auth/analytics']

# authorization and connect
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
analytics = build('analytics', 'v3', credentials=credentials)
properties = analytics.management().webproperties().list(accountId=66188758).execute()
```


```python
# looping through all properties and views
df_all = pd.DataFrame()

# for each property
for item in properties.get('items'):
    
    pname = item['name']                  # property name

    if  pname == 'Roll-Up Property':     
        psource = 'N/A'                   # get property source (app or web) and type (brand or market)
        ptype = 'N/A'
    else:
        psource = pname.split(' ')[2]
        ptype = pname.split(' ')[4]

    pid = item['id']                      # property id
    
    # get all profiles in this property
    profiles = analytics.management().profiles().list(
                    accountId='66188758',
                    webPropertyId=pid).execute()
    
    uaid = [prf['id'] for prf in profiles.get('items')]               # list of all profile id
    name = [prf['name'] for prf in profiles.get('items')]             # list of all profile name
    created = [prf['created'] for prf in profiles.get('items')]       # list of all profile created time
    
    # put into dataframe
    df = pd.DataFrame({'ua_profile_id':uaid,
                       'name':name,
                       'profile_created_time':created,
                       'property_name':pname,
                       'property_source':psource,
                       'property_type':ptype})
    
    df_all = df_all.append(df)
```


```python
print(df_all.head())
```

      ua_profile_id                          name      profile_created_time  \
    0     106630793        A - App, BC - Overview  2015-08-11T11:55:30.584Z   
    1     106645875  B - App, BC - United Kingdom  2015-08-11T13:33:34.182Z   
    2     106647798     B - App, BC - Switzerland  2015-08-11T13:31:30.987Z   
    3     106649030         B - App, BC - Denmark  2015-08-11T12:27:21.853Z   
    4     106649048         B - App, BC - Belgium  2015-08-11T13:29:32.991Z   
    
                          property_name property_source property_type level  
    0  3 - App - Brand - Bestseller.com             App         Brand     A  
    1  3 - App - Brand - Bestseller.com             App         Brand     B  
    2  3 - App - Brand - Bestseller.com             App         Brand     B  
    3  3 - App - Brand - Bestseller.com             App         Brand     B  
    4  3 - App - Brand - Bestseller.com             App         Brand     B  
    


```python
# filter out profiles that we don't need 
# aggregate level = 'B', property_type = 'Brand' and profile name does not contain "User"
df_all['level'] = df_all['name'].str.split(' ').str[0]
df_dim_profile = df_all.loc[df_all.level=='B'].copy()
df_dim_profile = df_dim_profile.loc[df_dim_profile['name'].str.find('User')<=0]
df_dim_profile = df_dim_profile.loc[df_dim_profile['property_type']=='Brand']

# add additional information
df_dim_profile['site_brand'] = df_dim_profile['name'].str.split(',').str[1].str.split(' ').str[1]
df_dim_profile['site_country'] = df_dim_profile['name'].str.split('-').str[-1].str.strip()
df_dim_profile['table_updated_time'] = datetime.now()
df_dim_profile['dim_ua_profile_id'] = df_dim_profile.index
```


```python
# reorder columns
df_dim_profile = df_dim_profile[['dim_ua_profile_id','ua_profile_id','property_source','site_brand',
                                 'site_country','property_type','level','name','property_name',
                                 'profile_created_time','table_updated_time']]
print(df_dim_profile.head())
```

       dim_ua_profile_id ua_profile_id property_source site_brand    site_country  \
    1                  1     106645875             App         BC  United Kingdom   
    2                  2     106647798             App         BC     Switzerland   
    3                  3     106649030             App         BC         Denmark   
    4                  4     106649048             App         BC         Belgium   
    5                  5     106649454             App         BC         Ireland   
    
      property_type level                          name  \
    1         Brand     B  B - App, BC - United Kingdom   
    2         Brand     B     B - App, BC - Switzerland   
    3         Brand     B         B - App, BC - Denmark   
    4         Brand     B         B - App, BC - Belgium   
    5         Brand     B         B - App, BC - Ireland   
    
                          property_name      profile_created_time  \
    1  3 - App - Brand - Bestseller.com  2015-08-11T13:33:34.182Z   
    2  3 - App - Brand - Bestseller.com  2015-08-11T13:31:30.987Z   
    3  3 - App - Brand - Bestseller.com  2015-08-11T12:27:21.853Z   
    4  3 - App - Brand - Bestseller.com  2015-08-11T13:29:32.991Z   
    5  3 - App - Brand - Bestseller.com  2015-08-11T13:34:56.432Z   
    
              table_updated_time  
    1 2020-01-27 14:08:23.497178  
    2 2020-01-27 14:08:23.497178  
    3 2020-01-27 14:08:23.497178  
    4 2020-01-27 14:08:23.497178  
    5 2020-01-27 14:08:23.497178  
    


```python
!jupyter nbconvert --to markdown "dim_ua_profile_generator.ipynb"
```

    [NbConvertApp] Converting notebook dim_ua_profile_generator.ipynb to markdown
    [NbConvertApp] Writing 11131 bytes to dim_ua_profile_generator.md
    


```python

```
