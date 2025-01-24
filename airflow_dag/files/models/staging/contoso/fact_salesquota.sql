with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALESQUOTA_RAW') }}
),

salesquota as (
    select
        -- IDs
        CAST(salesquotakey as NUMERIC(38, 0)) as salesquotakey_updated,
        CAST(channelkey as NUMERIC(38, 0)) as channelkey_updated,
        CAST(storekey as NUMERIC(38, 0)) as storekey_updated,
        CAST(productkey as NUMERIC(38, 0)) as productkey_updated,
        CAST(datekey as DATE) as datekey_updated,
        CAST(currencykey as NUMERIC(38, 0)) as currencykey_updated,
        CAST(scenariokey as NUMERIC(38, 0)) as scenariokey_updated,

        -- Amounts 
        COALESCE(CAST(salesquantityquota as NUMERIC(38, 1)), 0)
            as salesquantityquota_updated,
        COALESCE(CAST(salesamountquota as NUMERIC(38, 4)), 0)
            as salesamountquota_updated,
        COALESCE(CAST(grossmarginquota as NUMERIC(38, 4)), 0)
            as grossmarginquota_updated,

        -- Creation Timings
        TO_TIMESTAMP_NTZ(datekey) as created_at

    from source
)

select * from salesquota
