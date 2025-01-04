with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALES_RAW') }}
),

sales as (
    select
        -- IDs
        CAST(SALESKEY AS NUMERIC) as SALESKEY_updated,
        CAST(STOREKEY AS NUMERIC) as STOREKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        CAST(PRODUCTKEY AS NUMERIC) as PRODUCTKEY_updated,
        CAST(PROMOTIONKEY AS NUMERIC) as PROMOTIONKEY_updated,
        CAST(CHANNELKEY AS NUMERIC) as CHANNELKEY_updated,
        CAST(CURRENCYKEY AS NUMERIC) as CURRENCYKEY_updated,

        -- Amounts
        CAST(SALESAMOUNT AS NUMERIC) as SALESAMOUNT_updated,
        CAST(DISCOUNTAMOUNT AS NUMERIC) as DISCOUNTAMOUNT_updated,
        CAST(RETURNAMOUNT AS NUMERIC) as RETURNAMOUNT_updated,
        CAST(SALESQUANTITY AS NUMERIC) as SALESQUANTITY_updated,
        CAST(DISCOUNTQUANTITY AS NUMERIC) as DISCOUNTQUANTITY_updated,
        CAST(RETURNQUANTITY AS NUMERIC) as RETURNQUANTITY_updated,
        CAST(UNITCOST AS NUMERIC) as UNITCOST_updated,
        CAST(UNITPRICE AS NUMERIC) as UNITPRICE_updated,
        CAST(TOTALCOST AS NUMERIC) as TOTALCOST_updated,
        
        -- Derived Columns
        CAST(SALESAMOUNT - DISCOUNTAMOUNT as numeric) as NET_SALES_AMOUNT,
        CAST((UNITPRICE - UNITCOST) * SALESQUANTITY as numeric) as TOTAL_PROFIT,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from sales