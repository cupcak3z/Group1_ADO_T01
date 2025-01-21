with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSCENARIO_RAW') }}
),

scenario as (
    select
    --ids
        cast(scenariokey as numeric(38, 0)) as scenariokey_updated,

        --strings
        scenarioname,

        --creation date
        cast (loaddate as timestamp_ntz) as created_at

    from source
)

select * from scenario
