with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCURRENCY_RAW')}}
),

currency as (
    select
        -- ids
        cast(CURRENCYKEY as numeric(38,0)) as CURRENCYKEY_updated,
        -- strings
        CURRENCYNAME,
        CURRENCYDESCRIPTION,
        -- creation timing
        LOADDATE::timestamp_ntz as created_at
    from source
)

select * from currency