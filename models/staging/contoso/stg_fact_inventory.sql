-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTINVENTORY_RAW') }}
),

inventory as (
    select
        --ids
        -- converting data type
        cast(inventorykey as numeric(38, 0)) as inventorykey_updated,
        -- converting data type
        cast(storekey as numeric(38, 0)) as storekey_updated,
        -- converting data type
        cast(currencykey as numeric(38, 0)) as currencykey_updated,
        -- converting data type
        cast(productkey as numeric(38, 0)) as productkey_updated,

        --numbers
        -- converting data type
        cast(onhandquantity as numeric(38, 0)) as onhandquantity_updated,
        -- converting data type
        cast(onorderquantity as numeric(38, 0)) as onorderquantity_updated,
        cast(safetystockquantity as numeric(38, 0))
            as safetystockquantity_updated, -- converting data type
        -- converting data type
        cast(unitcost as numeric(38, 2)) as unitcost_updated,
        -- converting data type
        cast(daysinstock as numeric(38, 0)) as daysinstock_updated,
        -- converting data type
        cast(mindayinstock as numeric(38, 0)) as mindayinstock_updated,
        -- converting data type
        cast(maxdayinstock as numeric(38, 0)) as maxdayinstock_updated,
        cast(aging as numeric(38, 0)) as aging_updated, -- converting data type

        --date
        cast(datekey as date) as datekey_updated, -- converting data type

        --creation date
        to_timestamp(datekey) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from inventory
