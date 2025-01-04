with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITSLA_RAW') }}
),

itsla as (
    select
        -- IDs
        CAST(ITSLAKEY AS NUMERIC(38,0)) as ITSLAKEY_updated,
        CAST(STOREKEY AS NUMERIC(38,0)) as STOREKEY_updated,
        to_timestamp(DATEKEY, 'DD/MM/YYYY HH24:MI') as DATEKEY_updated,
        CAST(MACHINEKEY AS NUMERIC(38,0)) as MACHINEKEY_updated,
        CAST(OUTAGEKEY AS NUMERIC(38,0)) as OUTAGEKEY_updated,
        
        -- Amounts
        CAST(DOWNTIME AS NUMERIC(38,0)) as DOWNTIME_updated,

        -- Dates
        to_timestamp(OUTAGESTARTTIME, 'DD/MM/YYYY HH24:MI') as OUTAGESTARTTIME_updated,
        to_timestamp(OUTAGEENDTIME, 'DD/MM/YYYY HH24:MI') as OUTAGEENDTIME_updated,

        -- Creation Timings
        to_timestamp(DATEKEY, 'DD/MM/YYYY HH24:MI') as created_at 
    

    from source
)

select * from itsla