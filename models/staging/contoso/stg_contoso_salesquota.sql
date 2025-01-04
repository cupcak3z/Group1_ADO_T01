with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALESQUOTA_RAW') }}
),

salesquota as (
    select
        -- IDs
        CAST(SALESQUOTAKEY AS NUMERIC) as SALESQUOTAKEY_updated,
        CAST(CHANNELKEY AS NUMERIC) as CHANNELKEY_updated,
        CAST(STOREKEY AS NUMERIC) as STOREKEY_updated,
        CAST(PRODUCTKEY AS NUMERIC) as PRODUCTKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        CAST(CURRENCYKEY AS NUMERIC) as CURRENCYKEY_updated,
        CAST(SCENARIOKEY AS NUMERIC) as SCENARIOKEY_updated,

        -- Amounts 
        COALESCE(CAST(SALESQUANTITYQUOTA AS NUMERIC), 0) as SALESQUANTITYQUOTA_updated,
        COALESCE(CAST(SALESAMOUNTQUOTA AS NUMERIC), 0) as SALESAMOUNTQUOTA_updated,
        COALESCE(CAST(GROSSMARGINQUOTA AS NUMERIC), 0) as GROSSMARGINQUOTA_updated,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from salesquota