with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITSLA_RAW') }}
),

itsla as (
    select
        -- IDs
        CAST(ITSLAKEY AS NUMERIC) as ITSLAKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        CAST(STOREKEY AS NUMERIC) as STOREKEY_updated,
        CAST(MACHINEKEY AS NUMERIC) as MACHINEKEY_updated,
        CAST(OUTAGEKEY AS NUMERIC) as OUTAGEKEY_updated,
        
        -- Amounts
        CAST(DOWNTIME AS NUMERIC) as DOWNTIME_updated,

        -- Dates
        to_char(OUTAGESTARTTIME) as OUTAGESTARTTIME_updated,
        to_char(OUTAGEENDTIME) as OUTAGEENDTIME_updated,

        -- Creation Timings
        DATEKEY as created_at  
    

    from source
)

select * from itsla