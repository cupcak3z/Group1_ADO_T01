import boto3
import csv
import io
import datetime
import pymysql
import pandas as pd

s3_client = boto3.client(
    's3',
    aws_access_key_id='AKIAYS2NT6CQBLEM3GPZ',
    aws_secret_access_key='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',
    region_name='us-west-2'
)

timeout = 10
connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="defaultdb",
    host="contoso-smallscale-production-contoso.g.aivencloud.com",
    password="AVNS_wIHNRrM7RHDX86_xSaF",
    read_timeout=timeout,
    port=12096,
    user="avnadmin",
    write_timeout=timeout,
)

s3_bucket_name = 'contoso-blah'
table_names = [
    'ACCOUNT',
    'CHANNEL',
    'CURRENCY',
    'CUSTOMER',
    'DATE',
    'EMPLOYEE',
    'ENTITY',
    'EXCHANGERATE',
    'GEOGRAPHY',
    'INVENTORY',
    'ITMACHINE',
    'ITSLA',
    'MACHINE',
    'ONLINESALES',
    'OUTAGE',
    'PRODUCT',
    'PRODUCTCATEGORY',
    'PRODUCTSUBCATEGORY',
    'PROMOTION',
    'SALES',
    'SALESQUOTA',
    'SALESTERRITORY',
    'SCENARIO',
    'STORE',
    'STRATEGYPLAN'
]

table_names = ['CURRENCY']
current_time = datetime.datetime.now()

if 6 <= current_time.hour < 14:
    batch_start = (current_time - datetime.timedelta(days=1)).replace(
        hour=22, minute=0, second=0, microsecond=0)
    batch_end = current_time.replace(
        hour=5, minute=59, second=59, microsecond=999999)
    batch = "6am"
elif 14 <= current_time.hour < 22:
    batch_start = current_time.replace(
        hour=6, minute=0, second=0, microsecond=0)
    batch_end = current_time.replace(
        hour=13, minute=59, second=59, microsecond=999999)
    batch = "2pm"
else:
    batch_start = current_time.replace(
        hour=14, minute=0, second=0, microsecond=0)
    batch_end = current_time.replace(
        hour=21, minute=59, second=59, microsecond=999999)
    batch = "10pm"

file_date = current_time.strftime('%d-%m-%Y')
file_name = f"{file_date}_increment_{batch}.csv"
print(f"Generated file name: {file_name}")

for table_name in table_names:
    try:
        query = f"""
            SELECT * FROM {table_name}
            WHERE UPDATEDATE >= '{batch_start.strftime(
                '%Y-%m-%d %H:%M:%S.%f')[:-3]}'
              AND UPDATEDATE <= '{batch_end.strftime(
                '%Y-%m-%d %H:%M:%S.%f')[:-3]}'
        """
        print(f"Executing query for {table_name}: {query}")

        df = pd.read_sql(query, connection)

        if df.empty:
            print(f"No data to export for {table_name} in {batch} batch.")
            continue

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0) 
        
        s3_object_key = f"{table_name}/{file_name}"

        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_object_key,
            Body=csv_buffer.getvalue()
        )

        print(f"Exported {table_name} to s3")
        
    except Exception as e:
        print(f"Failed to export {table_name}: {e}")

connection.close()