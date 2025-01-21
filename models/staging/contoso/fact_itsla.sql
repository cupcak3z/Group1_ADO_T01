with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITSLA_RAW') }}
),

itsla as (
    select
        -- IDs
        CAST(itslakey as NUMERIC(38, 0)) as itslakey_updated,
        CAST(storekey as NUMERIC(38, 0)) as storekey_updated,
        CAST(datekey as DATE) as datekey_updated,
        CAST(machinekey as NUMERIC(38, 0)) as machinekey_updated,
        CAST(outagekey as NUMERIC(38, 0)) as outagekey_updated,

        -- Amounts
        CAST(downtime as NUMERIC(38, 0)) as downtime_updated,

        -- Dates
        TO_TIMESTAMP(outagestarttime) as outagestarttime_updated,
        TO_TIMESTAMP(outageendtime) as outageendtime_updated,

        -- Creation Timings
        CAST (loaddate as TIMESTAMP_NTZ) as created_at

    from source
)

select * from itsla
