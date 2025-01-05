with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTINVENTORY_RAW') }}
),

inventory as (
    select
    --ids
    cast(INVENTORYKEY as numeric(38,0)) as INVENTORYKEY_updated,
    cast(STOREKEY as numeric(38,0)) as STOREKEY_updated,
    cast(CURRENCYKEY as numeric(38,0)) as CURRENCYKEY_updated,

    --numbers
    cast(ONHANDQUANTITY as numeric(38,0)) as ONHANDQUANTITY_updated,
    cast(ONORDERQUANTITY as numeric(38,0)) as ONORDERQUANTITY_updated,
    cast(SAFETYSTOCKQUANTITY as numeric(38,0)) as SAFETYSTOCKQUANTITY_updated,
    cast(UNITCOST as numeric(38,2)) as UNITCOST_updated,
    cast(DAYSINSTOCK as numeric(38,0)) as DAYSINSTOCK_updated,
    cast(MINDAYINSTOCK as numeric(38,0)) as MINDAYINSTOCK_updated,
    cast(MAXDAYINSTOCK as numeric(38,0)) as MAXDAYINSTOCK_updated,
    cast(AGING as numeric(38,0)) as AGING_updated,

    --date
    cast(DATEKEY as date) as DATEKEY_updated,

    --creation date
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM inventory