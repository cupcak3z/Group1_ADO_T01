with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCURRENCY_RAW') }}
),

currency as (
    select
        -- ids
        cast(currencykey as numeric(38, 0)) as currencykey_updated,
        -- strings
        currencyname,
        currencydescription,
        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at
    from source
)

select * from currency
