-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSCENARIO_RAW') }}
),

scenario as (
    select
    --ids
        -- converting data type to ensure correct parsing
        cast(scenariokey as numeric(38, 0)) as scenariokey_updated,

        --strings
        scenarioname,

        --creation date
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at

    from source
)

-- putting the transformed data into a table
select * from scenario
