with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALESQUOTA_RAW') }}
),

salesquota as (
    select
        -- IDs
        CAST(SALESQUOTAKEY AS NUMERIC(38,0)) as SALESQUOTAKEY_updated,
        CAST(CHANNELKEY AS NUMERIC(38,0)) as CHANNELKEY_updated,
        CAST(STOREKEY AS NUMERIC(38,0)) as STOREKEY_updated,
        CAST(PRODUCTKEY AS NUMERIC(38,0)) as PRODUCTKEY_updated,
        cast(DATEKEY as DATE) as DATEKEY_updated,
        CAST(CURRENCYKEY AS NUMERIC(38,0)) as CURRENCYKEY_updated,
        CAST(SCENARIOKEY AS NUMERIC(38,0)) as SCENARIOKEY_updated,

        -- Amounts 
        COALESCE(CAST(SALESQUANTITYQUOTA AS NUMERIC(38,1)), 0) as SALESQUANTITYQUOTA_updated,
        COALESCE(CAST(SALESAMOUNTQUOTA AS NUMERIC(38,4)), 0) as SALESAMOUNTQUOTA_updated,
        COALESCE(CAST(GROSSMARGINQUOTA AS NUMERIC(38,4)), 0) as GROSSMARGINQUOTA_updated,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from salesquota