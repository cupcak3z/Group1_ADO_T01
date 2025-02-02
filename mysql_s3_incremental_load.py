import boto3
# import csv
import io
import datetime
import pymysql
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
AWS_ID = os.getenv('AWS_ID')
AWS_SECRET = os.getenv('AWS_SECRET')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ID,
    aws_secret_access_key=AWS_SECRET,
    region_name='us-west-2'
)

timeout = 10
connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="defaultdb",
    host="contoso-smallscale-production-contoso.g.aivencloud.com",
    password=MYSQL_PASSWORD,
    read_timeout=timeout,
    port=12096,
    user="avnadmin",
    write_timeout=timeout,
)

s3_bucket_name = 'contoso-blah'
non_loadup_tables = [
    'DATE', 'INVENTORY', 'ONLINESALES', 'SALESQUOTA',
    'SALES', 'STRATEGYPLAN'
]

loadup_tables = [
    'ACCOUNT', 'CHANNEL', 'CURRENCY', 'CUSTOMER', 'EMPLOYEE',
    'ENTITY', 'EXCHANGERATE', 'GEOGRAPHY', 'ITMACHINE', 'ITSLA',
    'MACHINE', 'OUTAGE', 'PRODUCT', 'PRODUCTCATEGORY',
    'PRODUCTSUBCATEGORY', 'PROMOTION', 'SALESTERRITORY',
    'SCENARIO', 'STORE']

non_loadup_tables = ['CURRENCY']
loadup_tables = []
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

for table_name in loadup_tables:
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

for table_name in non_loadup_tables:
    try:
        row_count_query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
        current_row_count = pd.read_sql(
            row_count_query, connection).iloc[0]['row_count']

        metadata_query = f"SELECT last_row_count FROM EXPORTMETADATA WHERE table_name = '{table_name}'"
        metadata_result = pd.read_sql(metadata_query, connection)
        last_row_count = metadata_result.iloc[0]['last_row_count'] if not metadata_result.empty else 0

        if current_row_count <= last_row_count:
            print(f"No data to export for {table_name}.")
            continue

        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connection)

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

        connection.execute(update_metadata_query=f"""
            INSERT INTO EXPORTMETADATA (table_name, last_row_count)
            VALUES ('{table_name}', {current_row_count})
            ON DUPLICATE KEY UPDATE last_row_count = {current_row_count}
        """)

    except Exception as e:
        print(f"Failed to export {table_name}: {e}")

connection.close()
