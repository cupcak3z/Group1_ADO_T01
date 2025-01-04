with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITSLA_RAW') }}
),

itsla as (
    select
        -- IDs
        CAST(ITSLAKEY AS NUMERIC) as ITSLAKEY_updated,
        CAST(STOREKEY AS NUMERIC) as STOREKEY_updated,
        to_timestamp(DATEKEY, 'DD/MM/YYYY HH24:MI') as DATEKEY_updated,
        -- CAST(DATEKEY AS DATE) as DATEKEY_updated,
        CAST(MACHINEKEY AS NUMERIC) as MACHINEKEY_updated,
        CAST(OUTAGEKEY AS NUMERIC) as OUTAGEKEY_updated,
        
        -- Amounts
        CAST(DOWNTIME AS NUMERIC) as DOWNTIME_updated,

        -- Dates
        to_timestamp(OUTAGESTARTTIME, 'DD/MM/YYYY HH24:MI') as OUTAGESTARTTIME_updated,
        to_timestamp(OUTAGEENDTIME, 'DD/MM/YYYY HH24:MI') as OUTAGEENDTIME_updated,

        -- Creation Timings
        to_timestamp(DATEKEY, 'DD/MM/YYYY HH24:MI') as created_at 
    

    from source
)

select * from itsla