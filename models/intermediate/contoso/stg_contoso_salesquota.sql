with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALESQUOTA_RAW') }}
),

salesquota as (
    select
        -- IDs
        SALESQUOTAKEY,
        CHANNELKEY,
        STOREKEY,
        PRODUCTKEY,
        to_char(DATEKEY, 'DD/MM/YYYY') as datekey_updated,
        CURRENCYKEY,
        SCENARIOKEY,

        -- Amounts and Numbers
        COALESCE(SALESQUANTITYQUOTA, 'No Sales Quota') as SALESQUANTITYQUOTA_updated,
        COALESCE(SALESAMOUNTQUOTA, 'No Amount Quota') as SALESAMOUNTQUOTA_updated,
        COALESCE(GROSSMARGINQUOTA, 'No Gross Margin Quota') as GROSSMARGINQUOTA_updated,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from salesquota