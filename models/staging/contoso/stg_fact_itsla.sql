-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITSLA_RAW') }}
),

itsla as (
    select
        -- IDs
        -- converting data type
        CAST(itslakey as NUMERIC(38, 0)) as itslakey_updated,
        -- converting data type
        CAST(storekey as NUMERIC(38, 0)) as storekey_updated,
        CAST(datekey as DATE) as datekey_updated, -- converting data type
        -- converting data type
        CAST(machinekey as NUMERIC(38, 0)) as machinekey_updated,
        -- converting data type
        CAST(outagekey as NUMERIC(38, 0)) as outagekey_updated,

        -- Amounts
        -- converting data type
        CAST(downtime as NUMERIC(38, 0)) as downtime_updated,

        -- Dates
        -- converting data type
        TO_TIMESTAMP(outagestarttime) as outagestarttime_updated,
        -- converting data type
        TO_TIMESTAMP(outageendtime) as outageendtime_updated,

        -- Creation Timings
        CAST(loaddate as TIMESTAMP_NTZ) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from itsla
