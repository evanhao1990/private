# Database connector
A package that handles working with redshift via python.

Ensure:

* environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` exist.
* environment variables `db_user_rs`, `db_password_rs`, `db_host_rs` exist.


## Usage guide
```python
from database_connector.connector import DatabaseRedshift
```

### Basic fetching
```python
with DatabaseRedshift() as db:
	df = db.fetch("select * from mart.dim_brand limit 10")
```
### Parameterized fetching
```python
params = {'limit':10}
with DatabaseRedshift() as db:
	df = db.fetch(query="select * from mart.dim_brand limit %(limit)s", params=params)
```

### Inserting
**If the number of rows in your dataframe is <= 5000**, then we directly insert data into the table using an "insert into {schema}.{table} values (values)" query.
```python
schema = 'sandbox_ana'
table = 'demo_table'

df = pd.DataFrame({'a':[1, 2, 3], 'b':[1, 2, 3]})
with DatabaseRedshift() as db:
	db.insert(df=df, schema=schema, table=table, mode='replace')
```
**If the number of rows in your dataframe is >5000**, then we first transfer data to S3, and then utilize "COPY" queries to transfer data from S3 to redshift. You can specify an S3 filename (or leave it empty, in which case a default name is generated `{current_datetime}_df.csv`.

AWS access keys `AWS_ACCESS_KEY_ID` `AWS_SECRET_ACCESS_KEY` should be configured as environment variables. The default `bucket` is `bse-analytics-dev.bseint.io`. The default `key_prefix` is `dwh`. 

Given this,  the following example  will write a file to the following S3 location. `s3://bse-analytics-dev.bseint.io/dwh/demo_table.csv`

```python
schema = 'sandbox_ana'
table = 'demo_table'

df = pd.DataFrame({'a':[1, 2, 3], 'b':[1, 2, 3]})
df = df.reindex(np.arange(10000)).ffill() # > 5000 rows.

with DatabaseRedshift() as db:
	db.insert(df=df, schema=schema, table=table, s3_csv_name='demo_table.csv', mode='replace')
```

Changing location can be achieved by overriding class variables `bucket` and `key_prefix` as follows:
```python
schema = 'sandbox_ana'
table = 'demo_table'
bucket = 'my_new_bucket'
key_prefix = 'my_new_folder'

with DatabaseRedshift(bucket=bucket, key_prefix=key_prefix) as db:
	db.insert(df=df, schema=schema, table=table, s3_csv_name='demo_table.csv', mode='replace')
```

There are three different insert modes: `replace` `fail` `append`.

`replace` will drop table, create new table, insert into new table.

`fail` will check if table exists, if yes: fail, else: create new table, insert into new table.

`append` will insert in batches of 5000 rows, if table does not exist it will create one.

```python
schema = 'sandbox_ana'
table = 'demo_table'

with DatabaseRedshift(bucket=bucket, key_prefix=key_prefix) as db:
	db.insert(df=df, schema=schema, table=table, mode='append')
```

### Executing
You can use the `.execute()` method to execute any type of redshift query.

```python
schema = 'sandbox_ana'
table = 'demo_table'
query = f"drop table if exists {schema}.{table};"

with DatabaseRedshift() as db:
	db.execute(query)

# Mutli-statement queries
query = f"""

create table if not exists {schema}.{table} as (
select * from mart.dim_brand db limit 10);

drop table if exists {schema}.{table};
"""

with DatabaseRedshift() as db:
	db.execute(query, multi=True)
```



## Other stuff / helper functions

`._check_table_exists(schema, table)`

`._generate_insert_query(df, schema, table)`

`.create_empty_table(df, schema, table)`

```python
schema = 'sandbox_ana'
table = 'demo_table'
df = pd.DataFrame({'a':[1, 2, 3], 'b':[1, 2, 3]})

with DatabaseRedshift() as db:
	# Will create an empty table. We pass a dataframe to get the columns and dtypes.
	db.create_empty_table(df=df, schema=schema, table=table)
```

### Transfer csv to S3 and move into redshift manually.
```python
from database_connector.connector import DatabaseRedshift, HandleS3

schema = 'sandbox_ana'
table = 'demo_table'
bucket = 'bse-analytics-dev.bseint.io'
key = 'dwh/test.csv'
delimiter='|'

# Data
df = pd.DataFrame({'a':[1, 2, 3], 'b':[1, 2, 3]})

# Move to S3
s3 = HandleS3(bucket='bse-analytics-dev.bseint.io')
s3.transfer_to_s3(df=df, key=key, delimiter=delimiter, index=False)

# Generate COPY query
query = s3.generate_s3_to_redshift_query(key=key, schema=schema, table=table, delimiter=delimiter)

# Transfer from S3 to Redshift
with DatabaseRedshift() as db:
    db.create_empty_table(df=df, schema=schema, table=table)
    db.execute(query)
```
