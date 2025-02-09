# Introduction
Amazon S3 (Simple Storage Service) is a scalable, high-performance object storage service offered by AWS. It allows users to store and retrieve any amount of data from anywhere on the web. S3 provides durability, security, and availability, making it ideal for backup, big data analytics, content distribution, and data lake solutions. It supports features like versioning, access control, encryption, and lifecycle policies for cost optimization.

# Set-Up
Reference steps can be found in the video link below:
[Load Data from Amazon AWS S3 Bucket to Snowflake Data Warehouse]([https://www.youtube.com/watch?v=woFc8Om1-kY])

## Prerequisites
- AWS Account with both a User and IAM Role with AmazonS3FullAccess
- Snowflake Account

## Steps
1. Go to the link below and create a free MySQL Database
[Aiven.io]([https://console.aiven.io/])
2. Connect the database to MySQL Workbench 8.0 CE by specifying the required information like password, name etc.
3. Create a table with the code below in the query(Example and Demo only for 1 table):
CREATE TABLE defaultdb.CURRENCY (
    CURRENCYKEY INT PRIMARY KEY,
    CURRENCYLABEL INT,
    CURRENCYNAME CHAR(3),
    CURRENCYDESCRIPTION VARCHAR(50),
    ETLLOADID INT,
    LOADDATE DATETIME(3),
    UPDATEDATE DATETIME(3)
);
4. Right click the CURRENCY table in the schema navigator, select Table Data Import Wizard, and load the data into the table with the CURRENCY.csv file
5. Insert incremental data such as the one below:
INSERT INTO defaultdb.CURRENCY (
    CURRENCYKEY,
    CURRENCYLABEL,
    CURRENCYNAME,
    CURRENCYDESCRIPTION,
    ETLLOADID,
    LOADDATE,
    UPDATEDATE
)
VALUES (
    29,
    29,
    'BTC',
    'Bitcoin',
    2,
    '2025-02-10 05:00:00.000',
    '2025-02-10 05:00:00.000'
);
6. Run the code below to create an EXPORTMETADATA table (This is for tables without "LOADDATE" and "UPDATEDATE" columns):
CREATE TABLE defaultdb.EXPORTMETADATA (
    table_name VARCHAR(100) PRIMARY KEY,
    last_row_count INT
);
7. Run 'mysql_s3_incremental_load.py'
