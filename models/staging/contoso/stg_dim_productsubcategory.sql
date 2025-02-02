-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCTSUBCATEGORY_RAW') }}
),

productsubcategory as (
    select
        -- ids
        cast(productsubcategorykey as numeric(38, 0)) -- converting data type to ensure correct parsing
            as productsubcategorykey_updated,
        cast(productcategorykey as numeric(38, 0)) -- converting data type to ensure correct parsing
            as productcategorykey_updated,

        -- strings
        productsubcategoryname,

        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at -- converting data type to ensure correct parsing

    from source
)

-- putting the transformed data into a table
select * from productsubcategory
