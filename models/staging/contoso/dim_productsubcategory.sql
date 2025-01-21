with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCTSUBCATEGORY_RAW')}}
),

productsubcategory as (
    select
        -- ids
        cast(PRODUCTSUBCATEGORYKEY as numeric(38,0)) as PRODUCTSUBCATEGORYKEY_updated,
        cast(PRODUCTCATEGORYKEY as numeric(38,0)) as PRODUCTCATEGORYKEY_updated,

        -- strings
        PRODUCTSUBCATEGORYNAME,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from productsubcategory