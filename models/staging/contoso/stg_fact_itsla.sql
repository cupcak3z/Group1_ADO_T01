-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITSLA_RAW') }}
),

itsla as (
    select
        -- IDs
        CAST(itslakey as NUMERIC(38, 0)) as itslakey_updated, -- converting data type
        CAST(storekey as NUMERIC(38, 0)) as storekey_updated, -- converting data type
        CAST(datekey as DATE) as datekey_updated, -- converting data type
        CAST(machinekey as NUMERIC(38, 0)) as machinekey_updated, -- converting data type
        CAST(outagekey as NUMERIC(38, 0)) as outagekey_updated, -- converting data type

        -- Amounts
        CAST(downtime as NUMERIC(38, 0)) as downtime_updated, -- converting data type

        -- Dates
        TO_TIMESTAMP(outagestarttime) as outagestarttime_updated, -- converting data type
        TO_TIMESTAMP(outageendtime) as outageendtime_updated, -- converting data type

        -- Creation Timings
        CAST(loaddate as TIMESTAMP_NTZ) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from itsla
