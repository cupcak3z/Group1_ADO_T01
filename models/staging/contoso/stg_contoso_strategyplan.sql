with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSTRATEGYPLAN_RAW') }}
),

strategyplan as (
    select
        -- IDs
        CAST(STRATEGYPLANKEY AS NUMERIC(38,0)) as STRATEGYPLANKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        CAST(ENTITYKEY AS NUMERIC(38,0)) as ENTITYKEY_updated,
        CAST(SCENARIOKEY AS NUMERIC(38,0)) as SCENARIOKEY_updated,
        CAST(ACCOUNTKEY AS NUMERIC(38,0)) as ACCOUNTKEY_updated,
        CAST(CURRENCYKEY AS NUMERIC(38,0)) as CURRENCYKEY_updated,
        CAST(PRODUCTCATEGORYKEY AS NUMERIC(38,0)) as PRODUCTCATEGORYKEY_updated,

        -- Amounts 
        CAST(AMOUNT AS NUMERIC(38,4)) as AMOUNT_updated,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from strategyplan