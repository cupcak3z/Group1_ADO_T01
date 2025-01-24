with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTINVENTORY_RAW') }}
),

inventory as (
    select
    --ids
        cast(inventorykey as numeric(38, 0)) as inventorykey_updated,
        cast(storekey as numeric(38, 0)) as storekey_updated,
        cast(currencykey as numeric(38, 0)) as currencykey_updated,

        --numbers
        cast(onhandquantity as numeric(38, 0)) as onhandquantity_updated,
        cast(onorderquantity as numeric(38, 0)) as onorderquantity_updated,
        cast(safetystockquantity as numeric(38, 0))
            as safetystockquantity_updated,
        cast(unitcost as numeric(38, 2)) as unitcost_updated,
        cast(daysinstock as numeric(38, 0)) as daysinstock_updated,
        cast(mindayinstock as numeric(38, 0)) as mindayinstock_updated,
        cast(maxdayinstock as numeric(38, 0)) as maxdayinstock_updated,
        cast(aging as numeric(38, 0)) as aging_updated,

        --date
        cast(datekey as date) as datekey_updated,

        --creation date
        to_timestamp(datekey) as created_at

    from source
)

select * from inventory
