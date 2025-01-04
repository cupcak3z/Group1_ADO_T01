with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSTRATEGYPLAN_RAW') }}
),

strategyplan as (
    select
        -- IDs
        CAST(STRATEGYPLANKEY AS NUMERIC) as STRATEGYPLANKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        CAST(ENTITYKEY AS NUMERIC) as ENTITYKEY_updated,
        CAST(SCENARIOKEY AS NUMERIC) as SCENARIOKEY_updated,
        CAST(ACCOUNTKEY AS NUMERIC) as ACCOUNTKEY_updated,
        CAST(CURRENCYKEY AS NUMERIC) as CURRENCYKEY_updated,
        CAST(PRODUCTCATEGORYKEY AS NUMERIC) as PRODUCTCATEGORYKEY_updated,

        -- Amounts 
        CAST(AMOUNT AS NUMERIC) as AMOUNT_updated,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from strategyplan