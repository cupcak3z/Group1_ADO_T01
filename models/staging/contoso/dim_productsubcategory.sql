with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCTSUBCATEGORY_RAW') }}
),

productsubcategory as (
    select
        -- ids
        cast(productsubcategorykey as numeric(38, 0))
            as productsubcategorykey_updated,
        cast(productcategorykey as numeric(38, 0))
            as productcategorykey_updated,

        -- strings
        productsubcategoryname,

        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at

    from source
)

select * from productsubcategory
