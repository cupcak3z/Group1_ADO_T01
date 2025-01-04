with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSTRATEGYPLAN_RAW') }}
),

strategyplan as (
    select
        -- IDs
        STRATEGYPLANKEY,
        to_char(DATEKEY, 'DD/MM/YYYY') as datekey_updated,
        ENTITYKEY,
        SCENARIOKEY,
        ACCOUNTKEY,
        CURRENCYKEY,
        PRODUCTCATEGORYKEY,

        -- Amounts and Numbers
        AMOUNT,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from strategyplan