with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCTCATEGORY_RAW') }}
),

productcategory as (
    select
        -- ids
        cast(productcategorykey as numeric(38, 0))
            as productcategorykey_updated,

        -- strings
        productcategoryname,

        -- creation timing
        cast (loaddate as timestamp_ntz) as created_at

    from source
)

select * from productcategory
