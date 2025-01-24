with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSTRATEGYPLAN_RAW') }}
),

strategyplan as (
    select
        -- IDs
        CAST(strategyplankey as NUMERIC(38, 0)) as strategyplankey_updated,
        TO_CHAR(datekey) as datekey_updated,
        CAST(entitykey as NUMERIC(38, 0)) as entitykey_updated,
        CAST(scenariokey as NUMERIC(38, 0)) as scenariokey_updated,
        CAST(accountkey as NUMERIC(38, 0)) as accountkey_updated,
        CAST(currencykey as NUMERIC(38, 0)) as currencykey_updated,
        CAST(productcategorykey as NUMERIC(38, 0))
            as productcategorykey_updated,

        -- Amounts 
        CAST(amount as NUMERIC(38, 4)) as amount_updated,

        -- Creation Timings
        TO_TIMESTAMP_NTZ(datekey) as created_at

    from source
)

select * from strategyplan
