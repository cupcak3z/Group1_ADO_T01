with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMMACHINE_RAW')}}
),

machine as (
    select
        -- ids
        cast(MACHINEKEY as numeric(38,0)) as MACHINEKEY_updated,
        cast(STOREKEY as numeric(38,0)) as STOREKEY_updated,

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
        cast(SERVICESTARTDATE as date) as SERVICESTARTDATE_updated,
        cast(DECOMMISSIONDATE as date) as DECOMMISSIONDATE_updated,
        cast(LASTMODIFIEDDATE as date) as LASTMODIFIEDDATE_updated,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from machine