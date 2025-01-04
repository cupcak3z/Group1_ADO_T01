with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCTCATEGORY_RAW')}}
),

productcategory as (
    select
        -- ids
        cast(PRODUCTCATEGORYKEY as numeric(38,0)) as PRODUCTCATEGORYKEY_updated,

        -- strings
        PRODUCTCATEGORYNAME,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from productcategory