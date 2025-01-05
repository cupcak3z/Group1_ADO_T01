with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSCENARIO_RAW') }}
),

scenario as (
    select
    --ids
    cast(SCENARIOKEY as numeric(38,0)) as SCENARIOKEY_updated,

    --strings
    SCENARIONAME,

    --creation date
    LOADDATE::timestamp_ntz as created_at

    from source
)

SELECT * FROM scenario