with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALES_RAW') }}
),

sales as (
    select
        -- IDs
        SALESKEY,
        STOREKEY,
        to_char(DATEKEY, 'DD/MM/YYYY') as datekey_updated,
        PRODUCTKEY,
        PROMOTIONKEY,
        CHANNELKEY,
        CURRENCYKEY,

        -- Amounts and Numbers
        SALESAMOUNT,
        DISCOUNTAMOUNT,
        RETURNAMOUNT,
        SALESQUANTITY,
        DISCOUNTQUANTITY,
        RETURNQUANTITY,
        UNITCOST,
        UNITPRICE,
        TOTALCOST,

        -- Derived Columns
        SALESAMOUNT - DISCOUNTAMOUNT as NET_SALES_AMOUNT,
        UNITPRICE - UNITCOST * SALESQUANTITY as TOTAL_PROFIT,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from sales