import snowflake.connector
import datetime

# Setting up snowflake connection
conn = snowflake.connector.connect(
    user='BLUEJAY',
    password='Tanhongkai123!',
    account='sfedu02-bab83824',
    warehouse='GROUP1_ADO_WAREHOUSE',
    database='ADO_GROUP1_DB_RAW',
    schema='CONTOSO'
)

# Creating a cursor object
cursor = conn.cursor()

# Clearing previous load
cursor.execute("""
TRUNCATE TABLE LOADACCOUNT_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADCHANNEL_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADCURRENCY_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADCUSTOMER_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADDATE_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADEMPLOYEE_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADENTITY_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADEXCHANGERATE_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADGEOGRAPHY_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADINVENTORY_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADITMACHINE_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADITSLA_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADMACHINE_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADONLINESALES_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADOUTAGE_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADPRODUCTCATEGORY_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADPRODUCTSUBCATEGORY_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADPRODUCT_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADPROMOTION_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADSALESQUOTA_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADSALESTERRITORY_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADSALES_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADSCENARIO_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADSTORE_STAGE
""")

cursor.execute("""
TRUNCATE TABLE LOADSTRATEGYPLAN_STAGE
""")

# cursor.execute("""
# CREATE OR REPLACE STORAGE INTEGRATION contoso_int
#  TYPE = EXTERNAL_STAGE
#  STORAGE_PROVIDER = 'S3'
#  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::590183919776:role/contoso'
#  ENABLED = TRUE
#  STORAGE_ALLOWED_LOCATIONS = ('s3://contoso-blah/');
# """)

# Creating a file format for the load

# cursor.execute("""
# CREATE OR REPLACE FILE FORMAT CSV_LOAD_FORMAT
#    TYPE = 'CSV'
#    COMPRESSION = 'AUTO'
#    FIELD_DELIMITER = ','
#    RECORD_DELIMITER = '\\n'
#    SKIP_HEADER = 1
#    FIELD_OPTIONALLY_ENCLOSED_BY = '\\042'
#    TRIM_SPACE = FALSE
#    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
#    ESCAPE = 'NONE'
#    ESCAPE_UNENCLOSED_FIELD = '\\134'
#    DATE_FORMAT = 'AUTO'
#    TIMESTAMP_FORMAT = 'AUTO';
# """)

# Creating stages for the storage integration
# cursor.execute("""
# CREATE OR REPLACE STAGE STG_ACCOUNT_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/ACCOUNT/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_CHANNEL_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/CHANNEL/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_CURRENCY_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/CURRENCY/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_CUSTOMER_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/CUSTOMER/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_DATE_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/DATE/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_EMPLOYEE_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/EMPLOYEE/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_ENTITY_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/ENTITY/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_GEOGRAPHY_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/GEOGRAPHY/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_MACHINE_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/MACHINE/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_OUTAGE_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/OUTAGE/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_PRODUCTCATEGORY_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/PRODUCTCATEGORY/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_PRODUCTSUBCATEGORY_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/PRODUCTSUBCATEGORY/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_PRODUCT_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/PRODUCT/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_PROMOTION_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/PROMOTION/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_SALESTERRITORY_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/SALESTERRITORY/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_SCENARIO_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/SCENARIO/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_STORE_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/STORE/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_EXCHANGERATE_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/EXCHANGERATE/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_INVENTORY_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/INVENTORY/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_ITMACHINE_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/ITMACHINE/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_ITSLA_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/ITSLA/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_ONLINESALES_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/ONLINESALES/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_SALESQUOTA_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/SALESQUOTA/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_SALES_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/SALES/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# cursor.execute("""
# CREATE OR REPLACE STAGE STG_STRATEGYPLAN_DEV
# STORAGE_INTEGRATION = CONTOSO_INT
# URL = 's3://contoso-blah/STRATEGYPLAN/'
# FILE_FORMAT = CSV_LOAD_FORMAT;
# """)

# Create stages for the table load

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADACCOUNT_STAGE (
# 	ACCOUNTKEY NUMBER(38,0),
# 	PARENTACCOUNTKEY VARCHAR(16777216),
# 	ACCOUNTLABEL NUMBER(38,0),
# 	ACCOUNTNAME VARCHAR(16777216),
# 	ACCOUNTDESCRIPTION VARCHAR(16777216),
# 	ACCOUNTTYPE VARCHAR(16777216),
# 	OPERATOR VARCHAR(16777216),
# 	CUSTOMMEMBERS VARCHAR(16777216),
# 	VALUETYPE VARCHAR(16777216),
# 	CUSTOMMEMBEROPTIONS VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADCHANNEL_STAGE (
# 	CHANNELKEY NUMBER(38,0),
# 	CHANNELLABEL NUMBER(38,0),
# 	CHANNELNAME VARCHAR(16777216),
# 	CHANNELDESCRIPTION VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADCURRENCY_STAGE (
# 	CURRENCYKEY NUMBER(38,0),
# 	CURRENCYLABEL NUMBER(38,0),
# 	CURRENCYNAME VARCHAR(16777216),
# 	CURRENCYDESCRIPTION VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADCUSTOMER_STAGE (
# 	CUSTOMERKEY NUMBER(38,0),
# 	GEOGRAPHYKEY NUMBER(38,0),
# 	CUSTOMERLABEL VARCHAR(16777216),
# 	TITLE VARCHAR(16777216),
# 	FIRSTNAME VARCHAR(16777216),
# 	MIDDLENAME VARCHAR(16777216),
# 	LASTNAME VARCHAR(16777216),
# 	NAMESTYLE VARCHAR(16777216),
# 	BIRTHDATE VARCHAR(16777216),
# 	MARITALSTATUS VARCHAR(16777216),
# 	SUFFIX VARCHAR(16777216),
# 	GENDER VARCHAR(16777216),
# 	EMAILADDRESS VARCHAR(16777216),
# 	YEARLYINCOME NUMBER(38,0),
# 	TOTALCHILDREN VARCHAR(16777216),
# 	NUMBERCHILDRENATHOME VARCHAR(16777216),
# 	EDUCATION VARCHAR(16777216),
# 	OCCUPATION VARCHAR(16777216),
# 	HOUSEOWNERFLAG VARCHAR(16777216),
# 	NUMBERCARSOWNED VARCHAR(16777216),
# 	ADDRESSLINE1 VARCHAR(16777216),
# 	ADDRESSLINE2 VARCHAR(16777216),
# 	PHONE VARCHAR(16777216),
# 	DATEFIRSTPURCHASE VARCHAR(16777216),
# 	CUSTOMERTYPE VARCHAR(16777216),
# 	COMPANYNAME VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADDATE_STAGE (
# 	DATEKEY DATE,
# 	FULLDATELABEL DATE,
# 	DATEDESCRIPTION VARCHAR(16777216),
# 	CALENDARYEAR NUMBER(38,0),
# 	CALENDARYEARLABEL VARCHAR(16777216),
# 	CALENDARHALFYEAR NUMBER(38,0),
# 	CALENDARHALFYEARLABEL VARCHAR(16777216),
# 	CALENDARQUARTER NUMBER(38,0),
# 	CALENDARQUARTERLABEL VARCHAR(16777216),
# 	CALENDARMONTH NUMBER(38,0),
# 	CALENDARMONTHLABEL VARCHAR(16777216),
# 	CALENDARWEEK NUMBER(38,0),
# 	CALENDARWEEKLABEL VARCHAR(16777216),
# 	CALENDARDAYOFWEEK NUMBER(38,0),
# 	CALENDARDAYOFWEEKLABEL VARCHAR(16777216),
# 	FISCALYEAR NUMBER(38,0),
# 	FISCALYEARLABEL VARCHAR(16777216),
# 	FISCALHALFYEAR NUMBER(38,0),
# 	FISCALHALFYEARLABEL VARCHAR(16777216),
# 	FISCALQUARTER NUMBER(38,0),
# 	FISCALQUARTERLABEL VARCHAR(16777216),
# 	FISCALMONTH NUMBER(38,0),
# 	FISCALMONTHLABEL VARCHAR(16777216),
# 	ISWORKDAY VARCHAR(16777216),
# 	ISHOLIDAY NUMBER(38,0),
# 	HOLIDAYNAME VARCHAR(16777216),
# 	EUROPESEASON VARCHAR(16777216),
# 	NORTHAMERICASEASON VARCHAR(16777216),
# 	ASIASEASON VARCHAR(16777216)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADEMPLOYEE_STAGE (
# 	EMPLOYEEKEY NUMBER(38,0),
# 	PARENTEMPLOYEEKEY VARCHAR(16777216),
# 	FIRSTNAME VARCHAR(16777216),
# 	LASTNAME VARCHAR(16777216),
# 	MIDDLENAME VARCHAR(16777216),
# 	TITLE VARCHAR(16777216),
# 	HIREDATE DATE,
# 	BIRTHDATE DATE,
# 	EMAILADDRESS VARCHAR(16777216),
# 	PHONE VARCHAR(16777216),
# 	MARITALSTATUS VARCHAR(16777216),
# 	EMERGENCYCONTACTNAME VARCHAR(16777216),
# 	EMERGENCYCONTACTPHONE VARCHAR(16777216),
# 	SALARIEDFLAG NUMBER(38,0),
# 	GENDER VARCHAR(16777216),
# 	PAYFREQUENCY NUMBER(38,0),
# 	BASERATE NUMBER(38,2),
# 	VACATIONHOURS NUMBER(38,0),
# 	CURRENTFLAG NUMBER(38,0),
# 	SALESPERSONFLAG NUMBER(38,0),
# 	DEPARTMENTNAME VARCHAR(16777216),
# 	STARTDATE DATE,
# 	ENDDATE VARCHAR(16777216),
# 	STATUS VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADENTITY_STAGE (
# 	ENTITYKEY NUMBER(38,0),
# 	ENTITYLABEL NUMBER(38,0),
# 	PARENTENTITYKEY VARCHAR(16777216),
# 	PARENTENTITYLABEL VARCHAR(16777216),
# 	ENTITYNAME VARCHAR(16777216),
# 	ENTITYDESCRIPTION VARCHAR(16777216),
# 	ENTITYTYPE VARCHAR(16777216),
# 	STARTDATE DATE,
# 	ENDDATE VARCHAR(16777216),
# 	STATUS VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADGEOGRAPHY_STAGE (
# 	GEOGRAPHYKEY NUMBER(38,0),
# 	GEOGRAPHYTYPE VARCHAR(16777216),
# 	CONTINENTNAME VARCHAR(16777216),
# 	CITYNAME VARCHAR(16777216),
# 	STATEPROVINCENAME VARCHAR(16777216),
# 	REGIONCOUNTRYNAME VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADMACHINE_STAGE (
# 	MACHINEKEY NUMBER(38,0),
# 	MACHINELABEL VARCHAR(16777216),
# 	STOREKEY NUMBER(38,0),
# 	MACHINETYPE VARCHAR(16777216),
# 	MACHINENAME VARCHAR(16777216),
# 	MACHINEDESCRIPTION VARCHAR(16777216),
# 	VENDORNAME VARCHAR(16777216),
# 	MACHINEOS VARCHAR(16777216),
# 	MACHINESOURCE VARCHAR(16777216),
# 	MACHINEHARDWARE VARCHAR(16777216),
# 	MACHINESOFTWARE VARCHAR(16777216),
# 	STATUS VARCHAR(16777216),
# 	SERVICESTARTDATE DATE,
# 	DECOMMISSIONDATE DATE,
# 	LASTMODIFIEDDATE DATE,
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADOUTAGE_STAGE (
# 	OUTAGEKEY NUMBER(38,0),
# 	OUTAGELABEL NUMBER(38,0),
# 	OUTAGENAME VARCHAR(16777216),
# 	OUTAGEDESCRIPTION VARCHAR(16777216),
# 	OUTAGETYPE VARCHAR(16777216),
# 	OUTAGETYPEDESCRIPTION VARCHAR(16777216),
# 	OUTAGESUBTYPE VARCHAR(16777216),
# 	OUTAGESUBTYPEDESCRIPTION VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADPRODUCTCATEGORY_STAGE (
# 	PRODUCTCATEGORYKEY NUMBER(38,0),
# 	PRODUCTCATEGORYLABEL NUMBER(38,0),
# 	PRODUCTCATEGORYNAME VARCHAR(16777216),
# 	PRODUCTCATEGORYDESCRIPTION VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADPRODUCTSUBCATEGORY_STAGE (
# 	PRODUCTSUBCATEGORYKEY NUMBER(38,0),
# 	PRODUCTSUBCATEGORYLABEL NUMBER(38,0),
# 	PRODUCTSUBCATEGORYNAME VARCHAR(16777216),
# 	PRODUCTSUBCATEGORYDESCRIPTION VARCHAR(16777216),
# 	PRODUCTCATEGORYKEY NUMBER(38,0),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADPRODUCT_STAGE (
# 	PRODUCTKEY NUMBER(38,0),
# 	PRODUCTLABEL NUMBER(38,0),
# 	PRODUCTNAME VARCHAR(16777216),
# 	PRODUCTDESCRIPTION VARCHAR(16777216),
# 	PRODUCTSUBCATEGORYKEY NUMBER(38,0),
# 	MANUFACTURER VARCHAR(16777216),
# 	BRANDNAME VARCHAR(16777216),
# 	CLASSID NUMBER(38,0),
# 	CLASSNAME VARCHAR(16777216),
# 	STYLEID VARCHAR(16777216),
# 	STYLENAME VARCHAR(16777216),
# 	COLORID NUMBER(38,0),
# 	COLORNAME VARCHAR(16777216),
# 	SIZE VARCHAR(16777216),
# 	SIZERANGE VARCHAR(16777216),
# 	SIZEUNITMEASUREID VARCHAR(16777216),
# 	WEIGHT VARCHAR(16777216),
# 	WEIGHTUNITMEASUREID VARCHAR(16777216),
# 	UNITOFMEASUREID NUMBER(38,0),
# 	UNITOFMEASURENAME VARCHAR(16777216),
# 	STOCKTYPEID NUMBER(38,0),
# 	STOCKTYPENAME VARCHAR(16777216),
# 	UNITCOST NUMBER(38,2),
# 	UNITPRICE NUMBER(38,3),
# 	AVAILABLEFORSALEDATE VARCHAR(16777216),
# 	STOPSALEDATE VARCHAR(16777216),
# 	STATUS VARCHAR(16777216),
# 	IMAGEURL VARCHAR(16777216),
# 	PRODUCTURL VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADPROMOTION_STAGE (
# 	PROMOTIONKEY NUMBER(38,0),
# 	PROMOTIONLABEL NUMBER(38,0),
# 	PROMOTIONNAME VARCHAR(16777216),
# 	PROMOTIONDESCRIPTION VARCHAR(16777216),
# 	DISCOUNTPERCENT NUMBER(38,2),
# 	PROMOTIONTYPE VARCHAR(16777216),
# 	PROMOTIONCATEGORY VARCHAR(16777216),
# 	STARTDATE DATE,
# 	ENDDATE DATE,
# 	MINQUANTITY VARCHAR(16777216),
# 	MAXQUANTITY VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADSALESTERRITORY_STAGE (
# 	SALESTERRITORYKEY NUMBER(38,0),
# 	GEOGRAPHYKEY NUMBER(38,0),
# 	SALESTERRITORYLABEL NUMBER(38,0),
# 	SALESTERRITORYNAME VARCHAR(16777216),
# 	SALESTERRITORYREGION VARCHAR(16777216),
# 	SALESTERRITORYCOUNTRY VARCHAR(16777216),
# 	SALESTERRITORYGROUP VARCHAR(16777216),
# 	SALESTERRITORYLEVEL NUMBER(38,0),
# 	SALESTERRITORYMANAGER NUMBER(38,0),
# 	STARTDATE DATE,
# 	ENDDATE VARCHAR(16777216),
# 	STATUS VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADSCENARIO_STAGE (
# 	SCENARIOKEY NUMBER(38,0),
# 	SCENARIOLABEL NUMBER(38,0),
# 	SCENARIONAME VARCHAR(16777216),
# 	SCENARIODESCRIPTION VARCHAR(16777216),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADSTORE_STAGE (
# 	STOREKEY NUMBER(38,0),
# 	GEOGRAPHYKEY NUMBER(38,0),
# 	STOREMANAGER NUMBER(38,0),
# 	STORETYPE VARCHAR(16777216),
# 	STORENAME VARCHAR(16777216),
# 	STOREDESCRIPTION VARCHAR(16777216),
# 	STATUS BOOLEAN,
# 	OPENDATE DATE,
# 	CLOSEDATE VARCHAR(16777216),
# 	ENTITYKEY NUMBER(38,0),
# 	ZIPCODE VARCHAR(16777216),
# 	ZIPCODEEXTENSION VARCHAR(16777216),
# 	STOREPHONE VARCHAR(16777216),
# 	STOREFAX VARCHAR(16777216),
# 	ADDRESSLINE1 VARCHAR(16777216),
# 	ADDRESSLINE2 VARCHAR(16777216),
# 	CLOSEREASON VARCHAR(16777216),
# 	EMPLOYEECOUNT VARCHAR(16777216),
# 	SELLINGAREASIZE NUMBER(38,0),
# 	LASTREMODELDATE DATE,
# 	GEOLOCATION FLOAT,
# 	GEOMETRY FLOAT,
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADEXCHANGERATE_STAGE (
# 	EXCHANGERATEKEY NUMBER(38,0),
# 	CURRENCYKEY NUMBER(38,0),
# 	DATEKEY DATE,
# 	AVERAGERATE NUMBER(38,5),
# 	ENDOFDAYRATE NUMBER(38,6),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADINVENTORY_STAGE (
# 	INVENTORYKEY NUMBER(38,0),
# 	DATEKEY DATE,
# 	STOREKEY NUMBER(38,0),
# 	PRODUCTKEY NUMBER(38,0),
# 	CURRENCYKEY NUMBER(38,0),
# 	ONHANDQUANTITY NUMBER(38,0),
# 	ONORDERQUANTITY NUMBER(38,0),
# 	SAFETYSTOCKQUANTITY NUMBER(38,0),
# 	UNITCOST NUMBER(38,2),
# 	DAYSINSTOCK NUMBER(38,0),
# 	MINDAYINSTOCK NUMBER(38,0),
# 	MAXDAYINSTOCK NUMBER(38,0),
# 	AGING NUMBER(38,0)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADITMACHINE_STAGE (
#     ITMACHINEKEY NUMBER(38,0),
#     MACHINEKEY NUMBER(38,0),
#     DATEKEY DATE,
#     COSTAMOUNT NUMBER(38,0),
#     COSTTYPE VARCHAR(16777216),
#     ETLLOADID NUMBER(38,0),
#     LOADDATE TIMESTAMP_NTZ(9),
#     UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADITSLA_STAGE (
# 	ITSLAKEY NUMBER(38,0),
# 	DATEKEY DATE,
# 	STOREKEY NUMBER(38,0),
# 	MACHINEKEY NUMBER(38,0),
# 	OUTAGEKEY NUMBER(38,0),
# 	OUTAGESTARTTIME TIMESTAMP_NTZ(9),
# 	OUTAGEENDTIME TIMESTAMP_NTZ(9),
# 	DOWNTIME NUMBER(38,0),
# 	ETLLOADID NUMBER(38,0),
# 	LOADDATE TIMESTAMP_NTZ(9),
# 	UPDATEDATE TIMESTAMP_NTZ(9)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADONLINESALES_STAGE (
# 	ONLINESALESKEY NUMBER(38,0),
# 	DATEKEY DATE,
# 	STOREKEY NUMBER(38,0),
# 	PRODUCTKEY NUMBER(38,0),
# 	PROMOTIONKEY NUMBER(38,0),
# 	CURRENCYKEY NUMBER(38,0),
# 	CUSTOMERKEY NUMBER(38,0),
# 	SALESORDERNUMBER VARCHAR(16777216),
# 	SALESORDERLINENUMBER NUMBER(38,0),
# 	SALESQUANTITY NUMBER(38,0),
# 	SALESAMOUNT NUMBER(38,4),
# 	RETURNQUANTITY NUMBER(38,0),
# 	RETURNAMOUNT NUMBER(38,4),
# 	DISCOUNTQUANTITY NUMBER(38,0),
# 	DISCOUNTAMOUNT NUMBER(38,4),
# 	TOTALCOST NUMBER(38,2),
# 	UNITCOST NUMBER(38,2),
# 	UNITPRICE NUMBER(38,2)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADSALESQUOTA_STAGE (
# 	SALESQUOTAKEY NUMBER(38,0),
# 	CHANNELKEY NUMBER(38,0),
# 	STOREKEY NUMBER(38,0),
# 	PRODUCTKEY NUMBER(38,0),
# 	DATEKEY DATE,
# 	CURRENCYKEY NUMBER(38,0),
# 	SCENARIOKEY NUMBER(38,0),
# 	SALESQUANTITYQUOTA NUMBER(38,1),
# 	SALESAMOUNTQUOTA NUMBER(38,4),
# 	GROSSMARGINQUOTA NUMBER(38,4)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADSALES_STAGE (
# 	SALESKEY NUMBER(38,0),
# 	DATEKEY DATE,
# 	CHANNELKEY NUMBER(38,0),
# 	STOREKEY NUMBER(38,0),
# 	PRODUCTKEY NUMBER(38,0),
# 	PROMOTIONKEY NUMBER(38,0),
# 	CURRENCYKEY NUMBER(38,0),
# 	UNITCOST NUMBER(38,2),
# 	UNITPRICE NUMBER(38,3),
# 	SALESQUANTITY NUMBER(38,0),
# 	RETURNQUANTITY NUMBER(38,0),
# 	RETURNAMOUNT NUMBER(38,2),
# 	DISCOUNTQUANTITY NUMBER(38,0),
# 	DISCOUNTAMOUNT NUMBER(38,4),
# 	TOTALCOST NUMBER(38,2),
# 	SALESAMOUNT NUMBER(38,4)
# );
# """)

# cursor.execute("""
# CREATE OR REPLACE TABLE LOADSTRATEGYPLAN_STAGE (
# 	STRATEGYPLANKEY NUMBER(38,0),
# 	DATEKEY DATE,
# 	ENTITYKEY NUMBER(38,0),
# 	SCENARIOKEY NUMBER(38,0),
# 	ACCOUNTKEY NUMBER(38,0),
# 	CURRENCYKEY NUMBER(38,0),
# 	PRODUCTCATEGORYKEY NUMBER(38,0),
# 	AMOUNT NUMBER(38,4)
# );
# """)

# Batch
current_time = datetime.datetime.now()
if 6 <= current_time.hour < 14:
    batch = "6am"
elif 14 <= current_time.hour < 22:
    batch = "2pm"
else:
    batch = "10pm"

file_date = current_time.strftime('%d-%m-%Y')
file_name = f"{file_date}_increment_{batch}.csv"
print(f"Generated file name: {file_name}")

# file_name = "10-02-2025_increment_6am.csv"  # hardcoded for testing

# Account
file_path = f"@STG_ACCOUNT_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for ACCOUNT")
else:
    cursor.execute(f"""
    COPY INTO LOADACCOUNT_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMACCOUNT_RAW AS target
    USING LOADACCOUNT_STAGE AS source
    ON target.ACCOUNTKEY = source.ACCOUNTKEY
    WHEN MATCHED THEN
        UPDATE SET
            ACCOUNTKEY = source.ACCOUNTKEY,
            PARENTACCOUNTKEY = source.PARENTACCOUNTKEY,
            ACCOUNTLABEL = source.ACCOUNTLABEL,
            ACCOUNTNAME = source.ACCOUNTNAME,
            ACCOUNTDESCRIPTION = source.ACCOUNTDESCRIPTION,
            ACCOUNTTYPE = source.ACCOUNTTYPE,
            OPERATOR = source.OPERATOR,
            CUSTOMMEMBERS = source.CUSTOMMEMBERS,
            VALUETYPE = source.VALUETYPE,
            CUSTOMMEMBEROPTIONS = source.CUSTOMMEMBEROPTIONS,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            ACCOUNTKEY,
            PARENTACCOUNTKEY,
            ACCOUNTLABEL,
            ACCOUNTNAME,
            ACCOUNTDESCRIPTION,
            ACCOUNTTYPE,
            OPERATOR,
            CUSTOMMEMBERS,
            VALUETYPE,
            CUSTOMMEMBEROPTIONS,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.ACCOUNTKEY,
            source.PARENTACCOUNTKEY,
            source.ACCOUNTLABEL,
            source.ACCOUNTNAME,
            source.ACCOUNTDESCRIPTION,
            source.ACCOUNTTYPE,
            source.OPERATOR,
            source.CUSTOMMEMBERS,
            source.VALUETYPE,
            source.CUSTOMMEMBEROPTIONS,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Channel
file_path = f"@STG_CHANNEL_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for CHANNEL")
else:
    cursor.execute(f"""
    COPY INTO LOADCHANNEL_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMCHANNEL_RAW AS target
    USING LOADCHANNEL_STAGE AS source
    ON target.CHANNELKEY = source.CHANNELKEY
    WHEN MATCHED THEN
        UPDATE SET
            CHANNELKEY = source.CHANNELKEY,
            CHANNELLABEL = source.CHANNELLABEL,
            CHANNELNAME = source.CHANNELNAME,
            CHANNELDESCRIPTION = source.CHANNELDESCRIPTION,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            CHANNELKEY,
            CHANNELLABEL,
            CHANNELNAME,
            CHANNELDESCRIPTION,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.CHANNELKEY,
            source.CHANNELLABEL,
            source.CHANNELNAME,
            source.CHANNELDESCRIPTION,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Currency
file_path = f"@STG_CURRENCY_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for CURRENCY")
else:
    cursor.execute(f"""
    COPY INTO LOADCURRENCY_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMCURRENCY_RAW AS target
    USING LOADCURRENCY_STAGE AS source
    ON target.CURRENCYKEY = source.CURRENCYKEY
    WHEN MATCHED THEN
        UPDATE SET
            CURRENCYKEY = source.CURRENCYKEY,
            CURRENCYLABEL = source.CURRENCYLABEL,
            CURRENCYNAME = source.CURRENCYNAME,
            CURRENCYDESCRIPTION = source.CURRENCYDESCRIPTION,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            CURRENCYKEY,
            CURRENCYLABEL,
            CURRENCYNAME,
            CURRENCYDESCRIPTION,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.CURRENCYKEY,
            source.CURRENCYLABEL,
            source.CURRENCYNAME,
            source.CURRENCYDESCRIPTION,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Customer
file_path = f"@STG_CUSTOMER_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for CUSTOMER")
else:
    cursor.execute(f"""
    COPY INTO LOADCUSTOMER_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMCUSTOMER_RAW AS target
    USING LOADCUSTOMER_STAGE AS source
    ON target.CUSTOMERKEY = source.CUSTOMERKEY
    WHEN MATCHED THEN
        UPDATE SET
            CUSTOMERKEY = source.CUSTOMERKEY,
            GEOGRAPHYKEY = source.GEOGRAPHYKEY,
            CUSTOMERLABEL = source.CUSTOMERLABEL,
            TITLE = source.TITLE,
            FIRSTNAME = source.FIRSTNAME,
            MIDDLENAME = source.MIDDLENAME,
            LASTNAME = source.LASTNAME,
            NAMESTYLE = source.NAMESTYLE,
            BIRTHDATE = source.BIRTHDATE,
            MARITALSTATUS = source.MARITALSTATUS,
            SUFFIX = source.SUFFIX,
            GENDER = source.GENDER,
            EMAILADDRESS = source.EMAILADDRESS,
            YEARLYINCOME = source.YEARLYINCOME,
            TOTALCHILDREN = source.TOTALCHILDREN,
            NUMBERCHILDRENATHOME = source.NUMBERCHILDRENATHOME,
            EDUCATION = source.EDUCATION,
            OCCUPATION = source.OCCUPATION,
            HOUSEOWNERFLAG = source.HOUSEOWNERFLAG,
            NUMBERCARSOWNED = source.NUMBERCARSOWNED,
            ADDRESSLINE1 = source.ADDRESSLINE1,
            ADDRESSLINE2 = source.ADDRESSLINE2,
            PHONE = source.PHONE,
            DATEFIRSTPURCHASE = source.DATEFIRSTPURCHASE,
            CUSTOMERTYPE = source.CUSTOMERTYPE,
            COMPANYNAME = source.COMPANYNAME,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            CUSTOMERKEY,
            GEOGRAPHYKEY,
            CUSTOMERLABEL,
            TITLE,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            NAMESTYLE,
            BIRTHDATE,
            MARITALSTATUS,
            SUFFIX,
            GENDER,
            EMAILADDRESS,
            YEARLYINCOME,
            TOTALCHILDREN,
            NUMBERCHILDRENATHOME,
            EDUCATION,
            OCCUPATION,
            HOUSEOWNERFLAG,
            NUMBERCARSOWNED,
            ADDRESSLINE1,
            ADDRESSLINE2,
            PHONE,
            DATEFIRSTPURCHASE,
            CUSTOMERTYPE,
            COMPANYNAME,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.CUSTOMERKEY,
            source.GEOGRAPHYKEY,
            source.CUSTOMERLABEL,
            source.TITLE,
            source.FIRSTNAME,
            source.MIDDLENAME,
            source.LASTNAME,
            source.NAMESTYLE,
            source.BIRTHDATE,
            source.MARITALSTATUS,
            source.SUFFIX,
            source.GENDER,
            source.EMAILADDRESS,
            source.YEARLYINCOME,
            source.TOTALCHILDREN,
            source.NUMBERCHILDRENATHOME,
            source.EDUCATION,
            source.OCCUPATION,
            source.HOUSEOWNERFLAG,
            source.NUMBERCARSOWNED,
            source.ADDRESSLINE1,
            source.ADDRESSLINE2,
            source.PHONE,
            source.DATEFIRSTPURCHASE,
            source.CUSTOMERTYPE,
            source.COMPANYNAME,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Date
file_path = f"@STG_DATE_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for DATE")
else:
    cursor.execute(f"""
    COPY INTO LOADDATE_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMDATE_RAW AS target
    USING LOADDATE_STAGE AS source
    ON target.DATEKEY = source.DATEKEY
    WHEN MATCHED THEN
        UPDATE SET
            DATEKEY = source.DATEKEY,
            FULLDATELABEL = source.FULLDATELABEL,
            DATEDESCRIPTION = source.DATEDESCRIPTION,
            CALENDARYEAR = source.CALENDARYEAR,
            CALENDARYEARLABEL = source.CALENDARYEARLABEL,
            CALENDARHALFYEAR = source.CALENDARHALFYEAR,
            CALENDARHALFYEARLABEL = source.CALENDARHALFYEARLABEL,
            CALENDARQUARTER = source.CALENDARQUARTER,
            CALENDARQUARTERLABEL = source.CALENDARQUARTERLABEL,
            CALENDARMONTH = source.CALENDARMONTH,
            CALENDARMONTHLABEL = source.CALENDARMONTHLABEL,
            CALENDARWEEK = source.CALENDARWEEK,
            CALENDARWEEKLABEL = source.CALENDARWEEKLABEL,
            CALENDARDAYOFWEEK = source.CALENDARDAYOFWEEK,
            CALENDARDAYOFWEEKLABEL = source.CALENDARDAYOFWEEKLABEL,
            FISCALYEAR = source.FISCALYEAR,
            FISCALYEARLABEL = source.FISCALYEARLABEL,
            FISCALHALFYEAR = source.FISCALHALFYEAR,
            FISCALHALFYEARLABEL = source.FISCALHALFYEARLABEL,
            FISCALQUARTER = source.FISCALQUARTER,
            FISCALQUARTERLABEL = source.FISCALQUARTERLABEL,
            FISCALMONTH = source.FISCALMONTH,
            FISCALMONTHLABEL = source.FISCALMONTHLABEL,
            ISWORKDAY = source.ISWORKDAY,
            ISHOLIDAY = source.ISHOLIDAY,
            HOLIDAYNAME = source.HOLIDAYNAME,
            EUROPESEASON = source.EUROPESEASON,
            NORTHAMERICASEASON = source.NORTHAMERICASEASON,
            ASIASEASON = source.ASIASEASON
    WHEN NOT MATCHED THEN
        INSERT (
            DATEKEY,
            FULLDATELABEL,
            DATEDESCRIPTION,
            CALENDARYEAR,
            CALENDARYEARLABEL,
            CALENDARHALFYEAR,
            CALENDARHALFYEARLABEL,
            CALENDARQUARTER,
            CALENDARQUARTERLABEL,
            CALENDARMONTH,
            CALENDARMONTHLABEL,
            CALENDARWEEK,
            CALENDARWEEKLABEL,
            CALENDARDAYOFWEEK,
            CALENDARDAYOFWEEKLABEL,
            FISCALYEAR,
            FISCALYEARLABEL,
            FISCALHALFYEAR,
            FISCALHALFYEARLABEL,
            FISCALQUARTER,
            FISCALQUARTERLABEL,
            FISCALMONTH,
            FISCALMONTHLABEL,
            ISWORKDAY,
            ISHOLIDAY,
            HOLIDAYNAME,
            EUROPESEASON,
            NORTHAMERICASEASON,
            ASIASEASON
        )
        VALUES (
            source.DATEKEY,
            source.FULLDATELABEL,
            source.DATEDESCRIPTION,
            source.CALENDARYEAR,
            source.CALENDARYEARLABEL,
            source.CALENDARHALFYEAR,
            source.CALENDARHALFYEARLABEL,
            source.CALENDARQUARTER,
            source.CALENDARQUARTERLABEL,
            source.CALENDARMONTH,
            source.CALENDARMONTHLABEL,
            source.CALENDARWEEK,
            source.CALENDARWEEKLABEL,
            source.CALENDARDAYOFWEEK,
            source.CALENDARDAYOFWEEKLABEL,
            source.FISCALYEAR,
            source.FISCALYEARLABEL,
            source.FISCALHALFYEAR,
            source.FISCALHALFYEARLABEL,
            source.FISCALQUARTER,
            source.FISCALQUARTERLABEL,
            source.FISCALMONTH,
            source.FISCALMONTHLABEL,
            source.ISWORKDAY,
            source.ISHOLIDAY,
            source.HOLIDAYNAME,
            source.EUROPESEASON,
            source.NORTHAMERICASEASON,
            source.ASIASEASON
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Employee
file_path = f"@STG_EMPLOYEE_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for EMPLOYEE")
else:
    cursor.execute(f"""
    COPY INTO LOADEMPLOYEE_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMEMPLOYEE_RAW AS target
    USING LOADEMPLOYEE_STAGE AS source
    ON target.EMPLOYEEKEY = source.EMPLOYEEKEY
    WHEN MATCHED THEN
        UPDATE SET
            EMPLOYEEKEY = source.EMPLOYEEKEY,
            PARENTEMPLOYEEKEY = source.PARENTEMPLOYEEKEY,
            FIRSTNAME = source.FIRSTNAME,
            LASTNAME = source.LASTNAME,
            MIDDLENAME = source.MIDDLENAME,
            TITLE = source.TITLE,
            HIREDATE = source.HIREDATE,
            BIRTHDATE = source.BIRTHDATE,
            EMAILADDRESS = source.EMAILADDRESS,
            PHONE = source.PHONE,
            MARITALSTATUS = source.MARITALSTATUS,
            EMERGENCYCONTACTNAME = source.EMERGENCYCONTACTNAME,
            EMERGENCYCONTACTPHONE = source.EMERGENCYCONTACTPHONE,
            SALARIEDFLAG = source.SALARIEDFLAG,
            GENDER = source.GENDER,
            PAYFREQUENCY = source.PAYFREQUENCY,
            BASERATE = source.BASERATE,
            VACATIONHOURS = source.VACATIONHOURS,
            CURRENTFLAG = source.CURRENTFLAG,
            SALESPERSONFLAG = source.SALESPERSONFLAG,
            DEPARTMENTNAME = source.DEPARTMENTNAME,
            STARTDATE = source.STARTDATE,
            ENDDATE = source.ENDDATE,
            STATUS = source.STATUS,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            EMPLOYEEKEY,
            PARENTEMPLOYEEKEY,
            FIRSTNAME,
            LASTNAME,
            MIDDLENAME,
            TITLE,
            HIREDATE,
            BIRTHDATE,
            EMAILADDRESS,
            PHONE,
            MARITALSTATUS,
            EMERGENCYCONTACTNAME,
            EMERGENCYCONTACTPHONE,
            SALARIEDFLAG,
            GENDER,
            PAYFREQUENCY,
            BASERATE,
            VACATIONHOURS,
            CURRENTFLAG,
            SALESPERSONFLAG,
            DEPARTMENTNAME,
            STARTDATE,
            ENDDATE,
            STATUS,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.EMPLOYEEKEY,
            source.PARENTEMPLOYEEKEY,
            source.FIRSTNAME,
            source.LASTNAME,
            source.MIDDLENAME,
            source.TITLE,
            source.HIREDATE,
            source.BIRTHDATE,
            source.EMAILADDRESS,
            source.PHONE,
            source.MARITALSTATUS,
            source.EMERGENCYCONTACTNAME,
            source.EMERGENCYCONTACTPHONE,
            source.SALARIEDFLAG,
            source.GENDER,
            source.PAYFREQUENCY,
            source.BASERATE,
            source.VACATIONHOURS,
            source.CURRENTFLAG,
            source.SALESPERSONFLAG,
            source.DEPARTMENTNAME,
            source.STARTDATE,
            source.ENDDATE,
            source.STATUS,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Entity
file_path = f"@STG_ENTITY_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for ENTITY")
else:
    cursor.execute(f"""
    COPY INTO LOADENTITY_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMENTITY_RAW AS target
    USING LOADENTITY_STAGE AS source
    ON target.ENTITYKEY = source.ENTITYKEY
    WHEN MATCHED THEN
        UPDATE SET
            ENTITYKEY = source.ENTITYKEY,
            ENTITYLABEL = source.ENTITYLABEL,
            PARENTENTITYKEY = source.PARENTENTITYKEY,
            PARENTENTITYLABEL = source.PARENTENTITYLABEL,
            ENTITYNAME = source.ENTITYNAME,
            ENTITYDESCRIPTION = source.ENTITYDESCRIPTION,
            ENTITYTYPE = source.ENTITYTYPE,
            STARTDATE = source.STARTDATE,
            ENDDATE = source.ENDDATE,
            STATUS = source.STATUS,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            ENTITYKEY,
            ENTITYLABEL,
            PARENTENTITYKEY,
            PARENTENTITYLABEL,
            ENTITYNAME,
            ENTITYDESCRIPTION,
            ENTITYTYPE,
            STARTDATE,
            ENDDATE,
            STATUS,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.ENTITYKEY,
            source.ENTITYLABEL,
            source.PARENTENTITYKEY,
            source.PARENTENTITYLABEL,
            source.ENTITYNAME,
            source.ENTITYDESCRIPTION,
            source.ENTITYTYPE,
            source.STARTDATE,
            source.ENDDATE,
            source.STATUS,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# ExchangeRate
file_path = f"@STG_EXCHANGERATE_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for EXCHANGERATE")
else:
    cursor.execute(f"""
    COPY INTO LOADEXCHANGERATE_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTEXCHANGERATE_RAW AS target
    USING LOADEXCHANGERATE_STAGE AS source
    ON target.EXCHANGERATEKEY = source.EXCHANGERATEKEY
    WHEN MATCHED THEN
        UPDATE SET
            EXCHANGERATEKEY = source.EXCHANGERATEKEY,
            CURRENCYKEY = source.CURRENCYKEY,
            DATEKEY = source.DATEKEY,
            AVERAGERATE = source.AVERAGERATE,
            ENDOFDAYRATE = source.ENDOFDAYRATE,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            EXCHANGERATEKEY,
            CURRENCYKEY,
            DATEKEY,
            AVERAGERATE,
            ENDOFDAYRATE,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.EXCHANGERATEKEY,
            source.CURRENCYKEY,
            source.DATEKEY,
            source.AVERAGERATE,
            source.ENDOFDAYRATE,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Geography
file_path = f"@STG_GEOGRAPHY_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for GEOGRAPHY")
else:
    cursor.execute(f"""
    COPY INTO LOADGEOGRAPHY_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMGEOGRAPHY_RAW AS target
    USING LOADGEOGRAPHY_STAGE AS source
    ON target.GEOGRAPHYKEY = source.GEOGRAPHYKEY
    WHEN MATCHED THEN
        UPDATE SET
            GEOGRAPHYKEY = source.GEOGRAPHYKEY,
            GEOGRAPHYTYPE = source.GEOGRAPHYTYPE,
            CONTINENTNAME = source.CONTINENTNAME,
            CITYNAME = source.CITYNAME,
            STATEPROVINCENAME = source.STATEPROVINCENAME,
            REGIONCOUNTRYNAME = source.REGIONCOUNTRYNAME,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            GEOGRAPHYKEY,
            GEOGRAPHYTYPE,
            CONTINENTNAME,
            CITYNAME,
            STATEPROVINCENAME,
            REGIONCOUNTRYNAME,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.GEOGRAPHYKEY,
            source.GEOGRAPHYTYPE,
            source.CONTINENTNAME,
            source.CITYNAME,
            source.STATEPROVINCENAME,
            source.REGIONCOUNTRYNAME,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Inventory
file_path = f"@STG_INVENTORY_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for INVENTORY")
else:
    cursor.execute(f"""
    COPY INTO LOADINVENTORY_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTINVENTORY_RAW AS target
    USING LOADINVENTORY_STAGE AS source
    ON target.INVENTORYKEY = source.INVENTORYKEY
    WHEN MATCHED THEN
        UPDATE SET
            INVENTORYKEY = source.INVENTORYKEY,
            DATEKEY = source.DATEKEY,
            STOREKEY = source.STOREKEY,
            PRODUCTKEY = source.PRODUCTKEY,
            CURRENCYKEY = source.CURRENCYKEY,
            ONHANDQUANTITY = source.ONHANDQUANTITY,
            ONORDERQUANTITY = source.ONORDERQUANTITY,
            SAFETYSTOCKQUANTITY = source.SAFETYSTOCKQUANTITY,
            UNITCOST = source.UNITCOST,
            DAYSINSTOCK = source.DAYSINSTOCK,
            MINDAYINSTOCK = source.MINDAYINSTOCK,
            MAXDAYINSTOCK = source.MAXDAYINSTOCK,
            AGING = source.AGING
    WHEN NOT MATCHED THEN
        INSERT (
            INVENTORYKEY,
            DATEKEY,
            STOREKEY,
            PRODUCTKEY,
            CURRENCYKEY,
            ONHANDQUANTITY,
            ONORDERQUANTITY,
            SAFETYSTOCKQUANTITY,
            UNITCOST,
            DAYSINSTOCK,
            MINDAYINSTOCK,
            MAXDAYINSTOCK,
            AGING
        )
        VALUES (
            source.INVENTORYKEY,
            source.DATEKEY,
            source.STOREKEY,
            source.PRODUCTKEY,
            source.CURRENCYKEY,
            source.ONHANDQUANTITY,
            source.ONORDERQUANTITY,
            source.SAFETYSTOCKQUANTITY,
            source.UNITCOST,
            source.DAYSINSTOCK,
            source.MINDAYINSTOCK,
            source.MAXDAYINSTOCK,
            source.AGING
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# ITMachine
file_path = f"@STG_ITMACHINE_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for ITMACHINE")
else:
    cursor.execute(f"""
    COPY INTO LOADITMACHINE_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTITMACHINE_RAW AS target
    USING LOADITMACHINE_STAGE AS source
    ON target.ITMACHINEKEY = source.ITMACHINEKEY
    WHEN MATCHED THEN
        UPDATE SET
            ITMACHINEKEY = source.ITMACHINEKEY,
            MACHINEKEY = source.MACHINEKEY,
            DATEKEY = source.DATEKEY,
            COSTAMOUNT = source.COSTAMOUNT,
            COSTTYPE = source.COSTTYPE,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            ITMACHINEKEY,
            MACHINEKEY,
            DATEKEY,
            COSTAMOUNT,
            COSTTYPE,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.ITMACHINEKEY,
            source.MACHINEKEY,
            source.DATEKEY,
            source.COSTAMOUNT,
            source.COSTTYPE,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# ITSLA
file_path = f"@STG_ITSLA_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for ITSLA")
else:
    cursor.execute(f"""
    COPY INTO LOADITSLA_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTITSLA_RAW AS target
    USING LOADITSLA_STAGE AS source
    ON target.ITSLAKEY = source.ITSLAKEY
    WHEN MATCHED THEN
        UPDATE SET
            ITSLAKEY = source.ITSLAKEY,
            DATEKEY = source.DATEKEY,
            STOREKEY = source.STOREKEY,
            MACHINEKEY = source.MACHINEKEY,
            OUTAGEKEY = source.OUTAGEKEY,
            OUTAGESTARTTIME = source.OUTAGESTARTTIME,
            OUTAGEENDTIME = source.OUTAGEENDTIME,
            DOWNTIME = source.DOWNTIME,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            ITSLAKEY,
            DATEKEY,
            STOREKEY,
            MACHINEKEY,
            OUTAGEKEY,
            OUTAGESTARTTIME,
            OUTAGEENDTIME,
            DOWNTIME,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.ITSLAKEY,
            source.DATEKEY,
            source.STOREKEY,
            source.MACHINEKEY,
            source.OUTAGEKEY,
            source.OUTAGESTARTTIME,
            source.OUTAGEENDTIME,
            source.DOWNTIME,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Machine
file_path = f"@STG_MACHINE_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for MACHINE")
else:
    cursor.execute(f"""
    COPY INTO LOADMACHINE_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMMACHINE_RAW AS target
    USING LOADMACHINE_STAGE AS source
    ON target.MACHINEKEY = source.MACHINEKEY
    WHEN MATCHED THEN
        UPDATE SET
            MACHINEKEY = source.MACHINEKEY,
            MACHINELABEL = source.MACHINELABEL,
            STOREKEY = source.STOREKEY,
            MACHINETYPE = source.MACHINETYPE,
            MACHINENAME = source.MACHINENAME,
            MACHINEDESCRIPTION = source.MACHINEDESCRIPTION,
            VENDORNAME = source.VENDORNAME,
            MACHINEOS = source.MACHINEOS,
            MACHINESOURCE = source.MACHINESOURCE,
            MACHINEHARDWARE = source.MACHINEHARDWARE,
            MACHINESOFTWARE = source.MACHINESOFTWARE,
            STATUS = source.STATUS,
            SERVICESTARTDATE = source.SERVICESTARTDATE,
            DECOMMISSIONDATE = source.DECOMMISSIONDATE,
            LASTMODIFIEDDATE = source.LASTMODIFIEDDATE,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            MACHINEKEY,
            MACHINELABEL,
            STOREKEY,
            MACHINETYPE,
            MACHINENAME,
            MACHINEDESCRIPTION,
            VENDORNAME,
            MACHINEOS,
            MACHINESOURCE,
            MACHINEHARDWARE,
            MACHINESOFTWARE,
            STATUS,
            SERVICESTARTDATE,
            DECOMMISSIONDATE,
            LASTMODIFIEDDATE,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.MACHINEKEY,
            source.MACHINELABEL,
            source.STOREKEY,
            source.MACHINETYPE,
            source.MACHINENAME,
            source.MACHINEDESCRIPTION,
            source.VENDORNAME,
            source.MACHINEOS,
            source.MACHINESOURCE,
            source.MACHINEHARDWARE,
            source.MACHINESOFTWARE,
            source.STATUS,
            source.SERVICESTARTDATE,
            source.DECOMMISSIONDATE,
            source.LASTMODIFIEDDATE,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# OnlineSales
file_path = f"@STG_ONLINESALES_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for ONLINESALES")
else:
    cursor.execute(f"""
    COPY INTO LOADONLINESALES_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTONLINESALES_RAW AS target
    USING LOADONLINESALES_STAGE AS source
    ON target.ONLINESALESKEY = source.ONLINESALESKEY
    WHEN MATCHED THEN
        UPDATE SET
            ONLINESALESKEY = source.ONLINESALESKEY,
            DATEKEY = source.DATEKEY,
            STOREKEY = source.STOREKEY,
            PRODUCTKEY = source.PRODUCTKEY,
            PROMOTIONKEY = source.PROMOTIONKEY,
            CURRENCYKEY = source.CURRENCYKEY,
            CUSTOMERKEY = source.CUSTOMERKEY,
            SALESORDERNUMBER = source.SALESORDERNUMBER,
            SALESORDERLINENUMBER = source.SALESORDERLINENUMBER,
            SALESQUANTITY = source.SALESQUANTITY,
            SALESAMOUNT = source.SALESAMOUNT,
            RETURNQUANTITY = source.RETURNQUANTITY,
            RETURNAMOUNT = source.RETURNAMOUNT,
            DISCOUNTQUANTITY = source.DISCOUNTQUANTITY,
            DISCOUNTAMOUNT = source.DISCOUNTAMOUNT,
            TOTALCOST = source.TOTALCOST,
            UNITCOST = source.UNITCOST,
            UNITPRICE = source.UNITPRICE
    WHEN NOT MATCHED THEN
        INSERT (
            ONLINESALESKEY,
            DATEKEY,
            STOREKEY,
            PRODUCTKEY,
            PROMOTIONKEY,
            CURRENCYKEY,
            CUSTOMERKEY,
            SALESORDERNUMBER,
            SALESORDERLINENUMBER,
            SALESQUANTITY,
            SALESAMOUNT,
            RETURNQUANTITY,
            RETURNAMOUNT,
            DISCOUNTQUANTITY,
            DISCOUNTAMOUNT,
            TOTALCOST,
            UNITCOST,
            UNITPRICE
        )
        VALUES (
            source.ONLINESALESKEY,
            source.DATEKEY,
            source.STOREKEY,
            source.PRODUCTKEY,
            source.PROMOTIONKEY,
            source.CURRENCYKEY,
            source.CUSTOMERKEY,
            source.SALESORDERNUMBER,
            source.SALESORDERLINENUMBER,
            source.SALESQUANTITY,
            source.SALESAMOUNT,
            source.RETURNQUANTITY,
            source.RETURNAMOUNT,
            source.DISCOUNTQUANTITY,
            source.DISCOUNTAMOUNT,
            source.TOTALCOST,
            source.UNITCOST,
            source.UNITPRICE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Outage
file_path = f"@STG_OUTAGE_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for OUTAGE")
else:
    cursor.execute(f"""
    COPY INTO LOADOUTAGE_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMOUTAGE_RAW AS target
    USING LOADOUTAGE_STAGE AS source
    ON target.OUTAGEKEY = source.OUTAGEKEY
    WHEN MATCHED THEN
        UPDATE SET
            OUTAGEKEY = source.OUTAGEKEY,
            OUTAGELABEL = source.OUTAGELABEL,
            OUTAGENAME = source.OUTAGENAME,
            OUTAGEDESCRIPTION = source.OUTAGEDESCRIPTION,
            OUTAGETYPE = source.OUTAGETYPE,
            OUTAGETYPEDESCRIPTION = source.OUTAGETYPEDESCRIPTION,
            OUTAGESUBTYPE = source.OUTAGESUBTYPE,
            OUTAGESUBTYPEDESCRIPTION = source.OUTAGESUBTYPEDESCRIPTION,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            OUTAGEKEY,
            OUTAGELABEL,
            OUTAGENAME,
            OUTAGEDESCRIPTION,
            OUTAGETYPE,
            OUTAGETYPEDESCRIPTION,
            OUTAGESUBTYPE,
            OUTAGESUBTYPEDESCRIPTION,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.OUTAGEKEY,
            source.OUTAGELABEL,
            source.OUTAGENAME,
            source.OUTAGEDESCRIPTION,
            source.OUTAGETYPE,
            source.OUTAGETYPEDESCRIPTION,
            source.OUTAGESUBTYPE,
            source.OUTAGESUBTYPEDESCRIPTION,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Product
file_path = f"@STG_PRODUCT_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for PRODUCT")
else:
    cursor.execute(f"""
    COPY INTO LOADPRODUCT_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMPRODUCT_RAW AS target
    USING LOADPRODUCT_STAGE AS source
    ON target.PRODUCTKEY = source.PRODUCTKEY
    WHEN MATCHED THEN
        UPDATE SET
            PRODUCTKEY = source.PRODUCTKEY,
            PRODUCTLABEL = source.PRODUCTLABEL,
            PRODUCTNAME = source.PRODUCTNAME,
            PRODUCTDESCRIPTION = source.PRODUCTDESCRIPTION,
            PRODUCTSUBCATEGORYKEY = source.PRODUCTSUBCATEGORYKEY,
            MANUFACTURER = source.MANUFACTURER,
            BRANDNAME = source.BRANDNAME,
            CLASSID = source.CLASSID,
            CLASSNAME = source.CLASSNAME,
            STYLEID = source.STYLEID,
            STYLENAME = source.STYLENAME,
            COLORID = source.COLORID,
            COLORNAME = source.COLORNAME,
            SIZE = source.SIZE,
            SIZERANGE = source.SIZERANGE,
            SIZEUNITMEASUREID = source.SIZEUNITMEASUREID,
            WEIGHT = source.WEIGHT,
            WEIGHTUNITMEASUREID = source.WEIGHTUNITMEASUREID,
            UNITOFMEASUREID = source.UNITOFMEASUREID,
            UNITOFMEASURENAME = source.UNITOFMEASURENAME,
            STOCKTYPEID = source.STOCKTYPEID,
            STOCKTYPENAME = source.STOCKTYPENAME,
            UNITCOST = source.UNITCOST,
            UNITPRICE = source.UNITPRICE,
            AVAILABLEFORSALEDATE = source.AVAILABLEFORSALEDATE,
            STOPSALEDATE = source.STOPSALEDATE,
            STATUS = source.STATUS,
            IMAGEURL = source.IMAGEURL,
            PRODUCTURL = source.PRODUCTURL,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            PRODUCTKEY,
            PRODUCTLABEL,
            PRODUCTNAME,
            PRODUCTDESCRIPTION,
            PRODUCTSUBCATEGORYKEY,
            MANUFACTURER,
            BRANDNAME,
            CLASSID,
            CLASSNAME,
            STYLEID,
            STYLENAME,
            COLORID,
            COLORNAME,
            SIZE,
            SIZERANGE,
            SIZEUNITMEASUREID,
            WEIGHT,
            WEIGHTUNITMEASUREID,
            UNITOFMEASUREID,
            UNITOFMEASURENAME,
            STOCKTYPEID,
            STOCKTYPENAME,
            UNITCOST,
            UNITPRICE,
            AVAILABLEFORSALEDATE,
            STOPSALEDATE,
            STATUS,
            IMAGEURL,
            PRODUCTURL,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.PRODUCTKEY,
            source.PRODUCTLABEL,
            source.PRODUCTNAME,
            source.PRODUCTDESCRIPTION,
            source.PRODUCTSUBCATEGORYKEY,
            source.MANUFACTURER,
            source.BRANDNAME,
            source.CLASSID,
            source.CLASSNAME,
            source.STYLEID,
            source.STYLENAME,
            source.COLORID,
            source.COLORNAME,
            source.SIZE,
            source.SIZERANGE,
            source.SIZEUNITMEASUREID,
            source.WEIGHT,
            source.WEIGHTUNITMEASUREID,
            source.UNITOFMEASUREID,
            source.UNITOFMEASURENAME,
            source.STOCKTYPEID,
            source.STOCKTYPENAME,
            source.UNITCOST,
            source.UNITPRICE,
            source.AVAILABLEFORSALEDATE,
            source.STOPSALEDATE,
            source.STATUS,
            source.IMAGEURL,
            source.PRODUCTURL,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# ProductCategory
file_path = f"@STG_PRODUCTCATEGORY_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for PRODUCTCATEGORY")
else:
    cursor.execute(f"""
    COPY INTO LOADPRODUCTCATEGORY_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMPRODUCTCATEGORY_RAW AS target
    USING LOADPRODUCTCATEGORY_STAGE AS source
    ON target.PRODUCTCATEGORYKEY = source.PRODUCTCATEGORYKEY
    WHEN MATCHED THEN
        UPDATE SET
            PRODUCTCATEGORYKEY = source.PRODUCTCATEGORYKEY,
            PRODUCTCATEGORYLABEL = source.PRODUCTCATEGORYLABEL,
            PRODUCTCATEGORYNAME = source.PRODUCTCATEGORYNAME,
            PRODUCTCATEGORYDESCRIPTION = source.PRODUCTCATEGORYDESCRIPTION,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            PRODUCTCATEGORYKEY,
            PRODUCTCATEGORYLABEL,
            PRODUCTCATEGORYNAME,
            PRODUCTCATEGORYDESCRIPTION,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.PRODUCTCATEGORYKEY,
            source.PRODUCTCATEGORYLABEL,
            source.PRODUCTCATEGORYNAME,
            source.PRODUCTCATEGORYDESCRIPTION,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# ProductSubCategory
file_path = f"@STG_PRODUCTSUBCATEGORY_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for PRODUCTSUBCATEGORY")
else:
    cursor.execute(f"""
    COPY INTO LOADPRODUCTSUBCATEGORY_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMPRODUCTSUBCATEGORY_RAW AS target
    USING LOADPRODUCTSUBCATEGORY_STAGE AS source
    ON target.PRODUCTSUBCATEGORYKEY = source.PRODUCTSUBCATEGORYKEY
    WHEN MATCHED THEN
        UPDATE SET
            PRODUCTSUBCATEGORYKEY = source.PRODUCTSUBCATEGORYKEY,
            PRODUCTSUBCATEGORYLABEL = source.PRODUCTSUBCATEGORYLABEL,
            PRODUCTSUBCATEGORYNAME = source.PRODUCTSUBCATEGORYNAME,
            PRODUCTSUBCATEGORYDESCRIPTION = source.PRODUCTSUBCATEGORYDESCRIPTION,
            PRODUCTCATEGORYKEY = source.PRODUCTCATEGORYKEY,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            PRODUCTSUBCATEGORYKEY,
            PRODUCTSUBCATEGORYLABEL,
            PRODUCTSUBCATEGORYNAME,
            PRODUCTSUBCATEGORYDESCRIPTION,
            PRODUCTCATEGORYKEY,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.PRODUCTSUBCATEGORYKEY,
            source.PRODUCTSUBCATEGORYLABEL,
            source.PRODUCTSUBCATEGORYNAME,
            source.PRODUCTSUBCATEGORYDESCRIPTION,
            source.PRODUCTCATEGORYKEY,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Promotion
file_path = f"@STG_PROMOTION_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for PROMOTION")
else:
    cursor.execute(f"""
    COPY INTO LOADPROMOTION_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMPROMOTION_RAW AS target
    USING LOADPROMOTION_STAGE AS source
    ON target.PROMOTIONKEY = source.PROMOTIONKEY
    WHEN MATCHED THEN
        UPDATE SET
            PROMOTIONKEY = source.PROMOTIONKEY,
            PROMOTIONLABEL = source.PROMOTIONLABEL,
            PROMOTIONNAME = source.PROMOTIONNAME,
            PROMOTIONDESCRIPTION = source.PROMOTIONDESCRIPTION,
            DISCOUNTPERCENT = source.DISCOUNTPERCENT,
            PROMOTIONTYPE = source.PROMOTIONTYPE,
            PROMOTIONCATEGORY = source.PROMOTIONCATEGORY,
            STARTDATE = source.STARTDATE,
            ENDDATE = source.ENDDATE,
            MINQUANTITY = source.MINQUANTITY,
            MAXQUANTITY = source.MAXQUANTITY,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            PROMOTIONKEY,
            PROMOTIONLABEL,
            PROMOTIONNAME,
            PROMOTIONDESCRIPTION,
            DISCOUNTPERCENT,
            PROMOTIONTYPE,
            PROMOTIONCATEGORY,
            STARTDATE,
            ENDDATE,
            MINQUANTITY,
            MAXQUANTITY,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.PROMOTIONKEY,
            source.PROMOTIONLABEL,
            source.PROMOTIONNAME,
            source.PROMOTIONDESCRIPTION,
            source.DISCOUNTPERCENT,
            source.PROMOTIONTYPE,
            source.PROMOTIONCATEGORY,
            source.STARTDATE,
            source.ENDDATE,
            source.MINQUANTITY,
            source.MAXQUANTITY,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Sales
file_path = f"@STG_SALES_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for SALES")
else:
    cursor.execute(f"""
    COPY INTO LOADSALES_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTSALES_RAW AS target
    USING LOADSALES_STAGE AS source
    ON target.SALESKEY = source.SALESKEY
    WHEN MATCHED THEN
        UPDATE SET
            SALESKEY = source.SALESKEY,
            DATEKEY = source.DATEKEY,
            CHANNELKEY = source.CHANNELKEY,
            STOREKEY = source.STOREKEY,
            PRODUCTKEY = source.PRODUCTKEY,
            PROMOTIONKEY = source.PROMOTIONKEY,
            CURRENCYKEY = source.CURRENCYKEY,
            UNITCOST = source.UNITCOST,
            UNITPRICE = source.UNITPRICE,
            SALESQUANTITY = source.SALESQUANTITY,
            RETURNQUANTITY = source.RETURNQUANTITY,
            RETURNAMOUNT = source.RETURNAMOUNT,
            DISCOUNTQUANTITY = source.DISCOUNTQUANTITY,
            DISCOUNTAMOUNT = source.DISCOUNTAMOUNT,
            TOTALCOST = source.TOTALCOST,
            SALESAMOUNT = source.SALESAMOUNT
    WHEN NOT MATCHED THEN
        INSERT (
            SALESKEY,
            DATEKEY,
            CHANNELKEY,
            STOREKEY,
            PRODUCTKEY,
            PROMOTIONKEY,
            CURRENCYKEY,
            UNITCOST,
            UNITPRICE,
            SALESQUANTITY,
            RETURNQUANTITY,
            RETURNAMOUNT,
            DISCOUNTQUANTITY,
            DISCOUNTAMOUNT,
            TOTALCOST,
            SALESAMOUNT
        )
        VALUES (
            source.SALESKEY,
            source.DATEKEY,
            source.CHANNELKEY,
            source.STOREKEY,
            source.PRODUCTKEY,
            source.PROMOTIONKEY,
            source.CURRENCYKEY,
            source.UNITCOST,
            source.UNITPRICE,
            source.SALESQUANTITY,
            source.RETURNQUANTITY,
            source.RETURNAMOUNT,
            source.DISCOUNTQUANTITY,
            source.DISCOUNTAMOUNT,
            source.TOTALCOST,
            source.SALESAMOUNT
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# SalesQuota
file_path = f"@STG_SALESQUOTA_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for SALESQUOTA")
else:
    cursor.execute(f"""
    COPY INTO LOADSALESQUOTA_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTSALESQUOTA_RAW AS target
    USING LOADSALESQUOTA_STAGE AS source
    ON target.SALESQUOTAKEY = source.SALESQUOTAKEY
    WHEN MATCHED THEN
        UPDATE SET
            SALESQUOTAKEY = source.SALESQUOTAKEY,
            CHANNELKEY = source.CHANNELKEY,
            STOREKEY = source.STOREKEY,
            PRODUCTKEY = source.PRODUCTKEY,
            DATEKEY = source.DATEKEY,
            CURRENCYKEY = source.CURRENCYKEY,
            SCENARIOKEY = source.SCENARIOKEY,
            SALESQUANTITYQUOTA = source.SALESQUANTITYQUOTA,
            SALESAMOUNTQUOTA = source.SALESAMOUNTQUOTA,
            GROSSMARGINQUOTA = source.GROSSMARGINQUOTA
    WHEN NOT MATCHED THEN
        INSERT (
            SALESQUOTAKEY,
            CHANNELKEY,
            STOREKEY,
            PRODUCTKEY,
            DATEKEY,
            CURRENCYKEY,
            SCENARIOKEY,
            SALESQUANTITYQUOTA,
            SALESAMOUNTQUOTA,
            GROSSMARGINQUOTA
        )
        VALUES (
            source.SALESQUOTAKEY,
            source.CHANNELKEY,
            source.STOREKEY,
            source.PRODUCTKEY,
            source.DATEKEY,
            source.CURRENCYKEY,
            source.SCENARIOKEY,
            source.SALESQUANTITYQUOTA,
            source.SALESAMOUNTQUOTA,
            source.GROSSMARGINQUOTA
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# SalesTerritory
file_path = f"@STG_SALESTERRITORY_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for SALESTERRITORY")
else:
    cursor.execute(f"""
    COPY INTO LOADSALESTERRITORY_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMSALESTERRITORY_RAW AS target
    USING LOADSALESTERRITORY_STAGE AS source
    ON target.SALESTERRITORYKEY = source.SALESTERRITORYKEY
    WHEN MATCHED THEN
        UPDATE SET
            SALESTERRITORYKEY = source.SALESTERRITORYKEY,
            GEOGRAPHYKEY = source.GEOGRAPHYKEY,
            SALESTERRITORYLABEL = source.SALESTERRITORYLABEL,
            SALESTERRITORYNAME = source.SALESTERRITORYNAME,
            SALESTERRITORYREGION = source.SALESTERRITORYREGION,
            SALESTERRITORYCOUNTRY = source.SALESTERRITORYCOUNTRY,
            SALESTERRITORYGROUP = source.SALESTERRITORYGROUP,
            SALESTERRITORYLEVEL = source.SALESTERRITORYLEVEL,
            SALESTERRITORYMANAGER = source.SALESTERRITORYMANAGER,
            STARTDATE = source.STARTDATE,
            ENDDATE = source.ENDDATE,
            STATUS = source.STATUS,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            SALESTERRITORYKEY,
            GEOGRAPHYKEY,
            SALESTERRITORYLABEL,
            SALESTERRITORYNAME,
            SALESTERRITORYREGION,
            SALESTERRITORYCOUNTRY,
            SALESTERRITORYGROUP,
            SALESTERRITORYLEVEL,
            SALESTERRITORYMANAGER,
            STARTDATE,
            ENDDATE,
            STATUS,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.SALESTERRITORYKEY,
            source.GEOGRAPHYKEY,
            source.SALESTERRITORYLABEL,
            source.SALESTERRITORYNAME,
            source.SALESTERRITORYREGION,
            source.SALESTERRITORYCOUNTRY,
            source.SALESTERRITORYGROUP,
            source.SALESTERRITORYLEVEL,
            source.SALESTERRITORYMANAGER,
            source.STARTDATE,
            source.ENDDATE,
            source.STATUS,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Scenario
file_path = f"@STG_SCENARIO_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for SCENARIO")
else:
    cursor.execute(f"""
    COPY INTO LOADSCENARIO_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMSCENARIO_RAW AS target
    USING LOADSCENARIO_STAGE AS source
    ON target.SCENARIOKEY = source.SCENARIOKEY
    WHEN MATCHED THEN
        UPDATE SET
            SCENARIOKEY = source.SCENARIOKEY,
            SCENARIOLABEL = source.SCENARIOLABEL,
            SCENARIONAME = source.SCENARIONAME,
            SCENARIODESCRIPTION = source.SCENARIODESCRIPTION,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            SCENARIOKEY,
            SCENARIOLABEL,
            SCENARIONAME,
            SCENARIODESCRIPTION,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.SCENARIOKEY,
            source.SCENARIOLABEL,
            source.SCENARIONAME,
            source.SCENARIODESCRIPTION,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# Store
file_path = f"@STG_STORE_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for STORE")
else:
    cursor.execute(f"""
    COPY INTO LOADSTORE_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO DIMSTORE_RAW AS target
    USING LOADSTORE_STAGE AS source
    ON target.STOREKEY = source.STOREKEY
    WHEN MATCHED THEN
        UPDATE SET
            STOREKEY = source.STOREKEY,
            GEOGRAPHYKEY = source.GEOGRAPHYKEY,
            STOREMANAGER = source.STOREMANAGER,
            STORETYPE = source.STORETYPE,
            STORENAME = source.STORENAME,
            STOREDESCRIPTION = source.STOREDESCRIPTION,
            STATUS = source.STATUS,
            OPENDATE = source.OPENDATE,
            CLOSEDATE = source.CLOSEDATE,
            ENTITYKEY = source.ENTITYKEY,
            ZIPCODE = source.ZIPCODE,
            ZIPCODEEXTENSION = source.ZIPCODEEXTENSION,
            STOREPHONE = source.STOREPHONE,
            STOREFAX = source.STOREFAX,
            ADDRESSLINE1 = source.ADDRESSLINE1,
            ADDRESSLINE2 = source.ADDRESSLINE2,
            CLOSEREASON = source.CLOSEREASON,
            EMPLOYEECOUNT = source.EMPLOYEECOUNT,
            SELLINGAREASIZE = source.SELLINGAREASIZE,
            LASTREMODELDATE = source.LASTREMODELDATE,
            GEOLOCATION = source.GEOLOCATION,
            GEOMETRY = source.GEOMETRY,
            ETLLOADID = source.ETLLOADID,
            LOADDATE = source.LOADDATE,
            UPDATEDATE = source.UPDATEDATE
    WHEN NOT MATCHED THEN
        INSERT (
            STOREKEY,
            GEOGRAPHYKEY,
            STOREMANAGER,
            STORETYPE,
            STORENAME,
            STOREDESCRIPTION,
            STATUS,
            OPENDATE,
            CLOSEDATE,
            ENTITYKEY,
            ZIPCODE,
            ZIPCODEEXTENSION,
            STOREPHONE,
            STOREFAX,
            ADDRESSLINE1,
            ADDRESSLINE2,
            CLOSEREASON,
            EMPLOYEECOUNT,
            SELLINGAREASIZE,
            LASTREMODELDATE,
            GEOLOCATION,
            GEOMETRY,
            ETLLOADID,
            LOADDATE,
            UPDATEDATE
        )
        VALUES (
            source.STOREKEY,
            source.GEOGRAPHYKEY,
            source.STOREMANAGER,
            source.STORETYPE,
            source.STORENAME,
            source.STOREDESCRIPTION,
            source.STATUS,
            source.OPENDATE,
            source.CLOSEDATE,
            source.ENTITYKEY,
            source.ZIPCODE,
            source.ZIPCODEEXTENSION,
            source.STOREPHONE,
            source.STOREFAX,
            source.ADDRESSLINE1,
            source.ADDRESSLINE2,
            source.CLOSEREASON,
            source.EMPLOYEECOUNT,
            source.SELLINGAREASIZE,
            source.LASTREMODELDATE,
            source.GEOLOCATION,
            source.GEOMETRY,
            source.ETLLOADID,
            source.LOADDATE,
            source.UPDATEDATE
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

# StrategyPlan
file_path = f"@STG_STRATEGYPLAN_DEV/{file_name}"

cursor.execute(f"LIST {file_path};")
file_list = cursor.fetchall()

if len(file_list) == 0:
    print("No incremental data file found for STRATEGYPLAN")
else:
    cursor.execute(f"""
    COPY INTO LOADSTRATEGYPLAN_STAGE
    FROM {file_path}
    FILE_FORMAT = (
        TYPE='CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        SKIP_HEADER=1
    )
    ON_ERROR = 'CONTINUE';
    """)

    print("""Legend: (file, status, rows_parsed, rows_loaded,
        error_limit, errors_seen, first_error_details)""")

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    copy_results = cursor.fetchall()

    for row in copy_results:
        print(f"COPY INTO Result: {row}")
        status = row[1] if len(row) > 1 else "N/A"
        rows_parsed = row[2] if len(row) > 2 else 0
        rows_loaded = row[3] if len(row) > 3 else 0
        errors_seen = row[5] if len(row) > 5 else 0
        first_error_details = row[6] if len(row) > 6 and row[6] else "None"

        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, STATUS, ROWS_PARSED, ROWS_LOADED,
            ERRORS_SEEN, FIRST_ERROR_DETAILS
        )
        VALUES (
            'COPY', '{file_path}', '{status}', {rows_parsed}, {rows_loaded},
            {errors_seen}, '{first_error_details}'
        );
        """)

    cursor.execute("""
    MERGE INTO FACTSTRATEGYPLAN_RAW AS target
    USING LOADSTRATEGYPLAN_STAGE AS source
    ON target.STRATEGYPLANKEY = source.STRATEGYPLANKEY
    WHEN MATCHED THEN
        UPDATE SET
            STRATEGYPLANKEY = source.STRATEGYPLANKEY,
            DATEKEY = source.DATEKEY,
            ENTITYKEY = source.ENTITYKEY,
            SCENARIOKEY = source.SCENARIOKEY,
            ACCOUNTKEY = source.ACCOUNTKEY,
            CURRENCYKEY = source.CURRENCYKEY,
            PRODUCTCATEGORYKEY = source.PRODUCTCATEGORYKEY,
            AMOUNT = source.AMOUNT
    WHEN NOT MATCHED THEN
        INSERT (
            STRATEGYPLANKEY,
            DATEKEY,
            ENTITYKEY,
            SCENARIOKEY,
            ACCOUNTKEY,
            CURRENCYKEY,
            PRODUCTCATEGORYKEY,
            AMOUNT
        )
        VALUES (
            source.STRATEGYPLANKEY,
            source.DATEKEY,
            source.ENTITYKEY,
            source.SCENARIOKEY,
            source.ACCOUNTKEY,
            source.CURRENCYKEY,
            source.PRODUCTCATEGORYKEY,
            source.AMOUNT
        );
    """)

    print()
    print('Legend: (Number of inserted records, Number of updated records)')

    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
    merge_results = cursor.fetchall()

    for row in merge_results:
        print(f"MERGE Result: {row}")
        cursor.execute(f"""
        INSERT INTO PROCESSLOG (
            OPERATION_TYPE, FILE_NAME, INSERTED_RECORDS, UPDATED_RECORDS
        )
        VALUES (
            'MERGE', '{file_path}', {row[0]}, {row[1]}
        );
        """)

    print()

cursor.close()
conn.close()
