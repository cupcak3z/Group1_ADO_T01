with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALES_RAW') }}
),

sales as (
    select
        -- IDs
        CAST(SALESKEY AS NUMERIC(38,0)) as SALESKEY_updated,
        CAST(STOREKEY AS NUMERIC(38,0)) as STOREKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        CAST(PRODUCTKEY AS NUMERIC(38,0)) as PRODUCTKEY_updated,
        CAST(PROMOTIONKEY AS NUMERIC(38,0)) as PROMOTIONKEY_updated,
        CAST(CHANNELKEY AS NUMERIC(38,0)) as CHANNELKEY_updated,
        CAST(CURRENCYKEY AS NUMERIC(38,0)) as CURRENCYKEY_updated,

        -- Amounts
        CAST(SALESAMOUNT AS NUMERIC(38,4)) as SALESAMOUNT_updated,
        CAST(DISCOUNTAMOUNT AS NUMERIC(38,4)) as DISCOUNTAMOUNT_updated,
        CAST(RETURNAMOUNT AS NUMERIC(38,2)) as RETURNAMOUNT_updated,
        CAST(SALESQUANTITY AS NUMERIC(38,0)) as SALESQUANTITY_updated,
        CAST(DISCOUNTQUANTITY AS NUMERIC(38,0)) as DISCOUNTQUANTITY_updated,
        CAST(RETURNQUANTITY AS NUMERIC(38,0)) as RETURNQUANTITY_updated,
        CAST(UNITCOST AS NUMERIC(38,2)) as UNITCOST_updated,
        CAST(UNITPRICE AS NUMERIC(38,3)) as UNITPRICE_updated,
        CAST(TOTALCOST AS NUMERIC(38,2)) as TOTALCOST_updated,
        
        -- Derived Columns
        CAST(SALESAMOUNT - DISCOUNTAMOUNT AS NUMERIC(38,4)) as NET_SALES_AMOUNT,
        CAST((UNITPRICE - UNITCOST) * SALESQUANTITY AS NUMERIC(38,2)) as TOTAL_PROFIT,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from sales