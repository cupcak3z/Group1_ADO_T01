import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')

# Setting up snowflake connection
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account='sfedu02-bab83824',
    warehouse='GROUP1_ADO_WAREHOUSE',
    database='ADO_GROUP1_DB_RAW',
    schema='CONTOSO'
)

# Creating a cursor object
cursor = conn.cursor()

# Account
cursor.execute("""
DELETE FROM DIMACCOUNT_RAW WHERE ACCOUNTKEY = 25;
""")

cursor.execute("""
UPDATE DIMACCOUNT_RAW
SET ACCOUNTNAME = 'Other Expenses',
    ACCOUNTDESCRIPTION = 'Other Expenses',
    UPDATEDATE = '2009-08-17 20:45:00.000'
WHERE ACCOUNTKEY = 12;
""")

# Channel
cursor.execute("""
DELETE FROM DIMCHANNEL_RAW WHERE CHANNELKEY = 5;
""")

# Currency
cursor.execute("""
DELETE FROM DIMCURRENCY_RAW WHERE CURRENCYKEY = 29;
""")

# Customer
cursor.execute("""
DELETE FROM DIMCUSTOMER_RAW WHERE CUSTOMERKEY = 19146;
""")

# Date
cursor.execute("""
DELETE FROM DIMDATE_RAW WHERE DATEKEY = '2025-02-10';
""")

# Employee
cursor.execute("""
DELETE FROM DIMEMPLOYEE_RAW WHERE EMPLOYEEKEY = 297;
""")

# Entity
cursor.execute("""
DELETE FROM DIMENTITY_RAW WHERE ENTITYKEY = 950;
""")

# ExchangeRate
cursor.execute("""
DELETE FROM FACTEXCHANGERATE_RAW WHERE EXCHANGERATEKEY = 775;
""")

# Geography
cursor.execute("""
DELETE FROM DIMGEOGRAPHY_RAW WHERE GEOGRAPHYKEY = 953;
""")

# Inventory
cursor.execute("""
DELETE FROM FACTINVENTORY_RAW WHERE INVENTORYKEY = 6070273;
""")

# ITMachine
cursor.execute("""
DELETE FROM FACTITMACHINE_RAW WHERE ITMACHINEKEY = 23284;
""")

# ITSLA
cursor.execute("""
DELETE FROM FACTITSLA_RAW WHERE ITSLAKEY = 4926;
""")

# Machine
cursor.execute("""
DELETE FROM DIMMACHINE_RAW WHERE MACHINEKEY = 7817;
""")

# OnlineSales
cursor.execute("""
DELETE FROM FACTONLINESALES_RAW WHERE ONLINESALESKEY = 32188092;
""")

# Outage
cursor.execute("""
DELETE FROM DIMOUTAGE_RAW WHERE OUTAGEKEY = 596;
""")

# Product
cursor.execute("""
DELETE FROM DIMPRODUCT_RAW WHERE PRODUCTKEY = 2518;
""")

# ProductCategory
cursor.execute("""
DELETE FROM DIMPRODUCTCATEGORY_RAW WHERE PRODUCTCATEGORYKEY = 9;
""")

# ProductSubCategory
cursor.execute("""
DELETE FROM DIMPRODUCTSUBCATEGORY_RAW WHERE PRODUCTSUBCATEGORYKEY = 49;
""")

# Promotion
cursor.execute("""
DELETE FROM DIMPROMOTION_RAW WHERE PROMOTIONKEY = 29;
""")

# Sales
cursor.execute("""
DELETE FROM FACTSALES_RAW WHERE SALESKEY = 3406090;
""")

# SalesQuota
cursor.execute("""
DELETE FROM FACTSALESQUOTA_RAW WHERE SALESQUOTAKEY = 7465912;
""")

# SalesTerritory
cursor.execute("""
DELETE FROM DIMSALESTERRITORY_RAW WHERE SALESTERRITORYKEY = 275;
""")

# Scenario
cursor.execute("""
DELETE FROM DIMSCENARIO_RAW WHERE SCENARIOKEY = 4;
""")

# Store
cursor.execute("""
DELETE FROM DIMSTORE_RAW WHERE STOREKEY = 311;
""")

# StrategyPlan
cursor.execute("""
DELETE FROM FACTSTRATEGYPLAN_RAW WHERE STRATEGYPLANKEY = 7465912;
""")

cursor.close()
conn.close()
