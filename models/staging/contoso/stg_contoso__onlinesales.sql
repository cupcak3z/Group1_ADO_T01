with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTONLINESALES_RAW') }}
),

onlinesales as (
    select
    --ids
    cast(ONLINESALESKEY as numeric) as ONLINESALESKEY_Updated,
    cast(STOREKEY as numeric) as STOREKEY_Updated,
	cast(PRODUCTKEY as numeric) as PRODUCTKEY_Updated,
	cast(PROMOTIONKEY as numeric) as PROMOTIONKEY_Updated,
	cast(CURRENCYKEY as numeric) as CURRENCYKEY_Updated,
	cast(CUSTOMERKEY as numeric) as CUSTOMERKEY_Updated,

    --string
    SALESORDERNUMBER,

    --numbers
    cast(SALESORDERLINENUMBER as numeric) as SALESORDERNUMBER_Updated,
    cast(SALESQUANTITY as numeric) as SALESQUANTITY_Updated,
    cast(SALESAMOUNT as numeric) as SALESAMOUNT_Updated,
    cast(RETURNQUANTITY as numeric) as RETURNQUANTITY_Updated,
    cast(RETURNAMOUNT as numeric) as RETURNAMOUNT_Updated,
    cast(DISCOUNTQUANTITY as numeric) as DISCOUNTQUANTITY_Updated,
    cast(DISCOUNTAMOUNT as numeric) as DISCOUNTAMOUNT_Updated,
    cast(TOTALCOST as numeric) as TOTALCOST_Updated,
    cast(UNITCOST as numeric) as UNITCOST_Updated,
    cast(UNITPRICE as numeric) as UNITPRICE_Updated,

    --date
    to_char(DATEKEY,'DD/MM/YYYY') as DATEKEY_updated,

    --creation date
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM onlinesales