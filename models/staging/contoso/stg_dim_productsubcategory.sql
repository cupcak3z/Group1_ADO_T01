-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCTSUBCATEGORY_RAW') }}
),

productsubcategory as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(productsubcategorykey as numeric(38, 0))
            as productsubcategorykey_updated,
        -- converting data type to ensure correct parsing
        cast(productcategorykey as numeric(38, 0))
            as productcategorykey_updated,

        -- strings
        productsubcategoryname,

        -- creation timing
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at

    from source
)

-- putting the transformed data into a table
select * from productsubcategory
