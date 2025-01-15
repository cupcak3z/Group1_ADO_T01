with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTONLINESALES_RAW') }}
),

onlinesales as (
    select
    --ids
    cast(ONLINESALESKEY as numeric(38,0)) as ONLINESALESKEY_updated,
    cast(STOREKEY as numeric(38,0)) as STOREKEY_updated,
	cast(PRODUCTKEY as numeric(38,0)) as PRODUCTKEY_updated,
	cast(PROMOTIONKEY as numeric(38,0)) as PROMOTIONKEY_updated,
	cast(CURRENCYKEY as numeric(38,0)) as CURRENCYKEY_updated,
	cast(CUSTOMERKEY as numeric(38,0)) as CUSTOMERKEY_updated,

    --string
    SALESORDERNUMBER,

    --numbers
    cast(SALESORDERLINENUMBER as numeric(38,0)) as SALESORDERNUMBER_updated,
    cast(SALESQUANTITY as numeric(38,0)) as SALESQUANTITY_updated,
    cast(SALESAMOUNT as numeric(38,4)) as SALESAMOUNT_updated,
    cast(RETURNQUANTITY as numeric(38,0)) as RETURNQUANTITY_updated,
    cast(RETURNAMOUNT as numeric(38,4)) as RETURNAMOUNT_updated,
    cast(DISCOUNTQUANTITY as numeric(38,0)) as DISCOUNTQUANTITY_updated,
    cast(DISCOUNTAMOUNT as numeric(38,4)) as DISCOUNTAMOUNT_updated,
    cast(TOTALCOST as numeric(38,2)) as TOTALCOST_updated,
    cast(UNITCOST as numeric(38,2)) as UNITCOST_updated,
    cast(UNITPRICE as numeric(38,2)) as UNITPRICE_updated,

    --date
    cast(DATEKEY as date) as DATEKEY_updated,

    -- Derived Columns
    CAST(SALESAMOUNT - DISCOUNTAMOUNT AS NUMERIC(38,4)) as NET_SALES_AMOUNT,
    CAST((UNITPRICE - UNITCOST) * SALESQUANTITY AS NUMERIC(38,2)) as TOTAL_PROFIT,

    --creation date
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM onlinesales