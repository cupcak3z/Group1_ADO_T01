with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTINVENTORY_RAW') }}
),

inventory as (
    select
    --ids
    cast(INVENTORYKEY as numeric) as INVENTORYKEY_updated,
    cast(STOREKEY as numeric) as STOREKEY_updated,
    cast(CURRENCYKEY as numeric) as CURRENCYKEY_updated,

    --numbers
    cast(ONHANDQUANTITY as numeric) as ONHANDQUANTITY_updated,
    cast(ONORDERQUANTITY as numeric) as ONORDERQUANTITY_updated,
    cast(SAFETYSTOCKQUANTITY as numeric) as SAFETYSTOCKQUANTITY_updated,
    cast(UNITCOST as numeric) as UNITCOST_updated,
    cast(DAYSINSTOCK as numeric) as DAYSINSTOCK_updated,
    cast(MINDAYINSTOCK as numeric) as MINDAYINSTOCK_updated,
    cast(MAXDAYINSTOCK as numeric) as MAXDAYINSTOCK_updated,
    cast(AGING as numeric) as AGING_updated,

    --date
    cast(DATEKEY as date) as DATEKEY_updated,

    --creation date
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM inventory