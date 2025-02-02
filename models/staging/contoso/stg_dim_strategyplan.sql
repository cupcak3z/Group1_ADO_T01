-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSTRATEGYPLAN_RAW') }}
),

strategyplan as (
    select
        -- IDs
        CAST(strategyplankey as NUMERIC(38, 0)) as strategyplankey_updated, -- converting data type
        TO_CHAR(datekey) as datekey_updated, -- converting data type
        CAST(entitykey as NUMERIC(38, 0)) as entitykey_updated, -- converting data type
        CAST(scenariokey as NUMERIC(38, 0)) as scenariokey_updated, -- converting data type
        CAST(accountkey as NUMERIC(38, 0)) as accountkey_updated, -- converting data type
        CAST(currencykey as NUMERIC(38, 0)) as currencykey_updated, -- converting data type
        CAST(productcategorykey as NUMERIC(38, 0)) -- converting data type
            as productcategorykey_updated,

        -- Amounts 
        CAST(amount as NUMERIC(38, 4)) as amount_updated, -- converting data type

        -- Creation Timings
        TO_TIMESTAMP_NTZ(datekey) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from strategyplan
