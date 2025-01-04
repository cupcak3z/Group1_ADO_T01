with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMMACHINE_RAW')}}
),

machine as (
    select
        -- ids
        cast(MACHINEKEY as numeric) as MACHINEKEY_updated,
        cast(STOREKEY as numeric) as STOREKEY_updated,

        -- strings
        MACHINETYPE,
        MACHINENAME,
        MACHINEDESCRIPTION,
        VENDORNAME,
        MACHINEOS,
        MACHINESOURCE,
        MACHINEHARDWARE,
        MACHINESOFTWARE,
        STATUS,

        -- dates
        SERVICESTARTDATE,
        DECOMMISSIONDATE,
        LASTMODIFIEDDATE,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from machine