-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCURRENCY_RAW') }}
),

currency as (
    select
        -- ids
        cast(currencykey as numeric(38, 0)) as currencykey_updated, -- converting data type to ensure correct parsing
        -- strings
        currencyname,
        currencydescription,
        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at -- converting data type to ensure correct parsing
    from source
)

-- putting the transformed data into a table
select * from currency
