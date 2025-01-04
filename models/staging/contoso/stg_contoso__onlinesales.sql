with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTONLINESALES_RAW') }}
),

onlinesales as (
    select
    --ids
    cast(ONLINESALESKEY as numeric) as ONLINESALESKEY_updated,
    cast(STOREKEY as numeric) as STOREKEY_updated,
	cast(PRODUCTKEY as numeric) as PRODUCTKEY_updated,
	cast(PROMOTIONKEY as numeric) as PROMOTIONKEY_updated,
	cast(CURRENCYKEY as numeric) as CURRENCYKEY_updated,
	cast(CUSTOMERKEY as numeric) as CUSTOMERKEY_updated,

    --string
    SALESORDERNUMBER,

    --numbers
    cast(SALESORDERLINENUMBER as numeric) as SALESORDERNUMBER_updated,
    cast(SALESQUANTITY as numeric) as SALESQUANTITY_updated,
    cast(SALESAMOUNT as numeric) as SALESAMOUNT_updated,
    cast(RETURNQUANTITY as numeric) as RETURNQUANTITY_updated,
    cast(RETURNAMOUNT as numeric) as RETURNAMOUNT_updated,
    cast(DISCOUNTQUANTITY as numeric) as DISCOUNTQUANTITY_updated,
    cast(DISCOUNTAMOUNT as numeric) as DISCOUNTAMOUNT_updated,
    cast(TOTALCOST as numeric) as TOTALCOST_updated,
    cast(UNITCOST as numeric) as UNITCOST_updated,
    cast(UNITPRICE as numeric) as UNITPRICE_updated,

    --date
    cast(DATEKEY as date) as DATEKEY_updated,

    --creation date
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM onlinesales