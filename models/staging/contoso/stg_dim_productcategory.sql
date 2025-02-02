-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCTCATEGORY_RAW') }}
),

productcategory as (
    select
        -- ids
        cast(productcategorykey as numeric(38, 0)) -- converting data type to ensure correct parsing
            as productcategorykey_updated,

        -- strings
        productcategoryname,

        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at -- converting data type to ensure correct parsing

    from source
)

-- putting the transformed data into a table
select * from productcategory
