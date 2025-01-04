with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTINVENTORY_RAW') }}
),

inventory as (
    select
    --ids
    cast(INVENTORYKEY as numeric) as INVENTORYKEY_Updated,
    cast(STOREKEY as numeric) as STOREKEY_Updated,
    cast(CURRENCYKEY as numeric) as CURRENCYKEY_Updated,

    --numbers
    cast(ONHANDQUANTITY as numeric) as ONHANDQUANTITY_Updated,
    cast(ONORDERQUANTITY as numeric) as ONORDERQUANTITY_Updated,
    cast(SAFETYSTOCKQUANTITY as numeric) as SAFETYSTOCKQUANTITY_Updated,
    cast(UNITCOST as numeric) as UNITCOST_Updated,
    cast(DAYSINSTOCK as numeric) as DAYSINSTOCK_Updated,
    cast(MINDAYINSTOCK as numeric) as MINDAYINSTOCK_Updated,
    cast(MAXDAYINSTOCK as numeric) as MAXDAYINSTOCK_Updated,
    cast(AGING as numeric) as AGING_Updated,

    --date
    to_char(DATEKEY,'DD/MM/YYYY') as DATEKEY_updated,

    --creation date
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM inventory