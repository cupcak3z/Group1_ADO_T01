with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMMACHINE_RAW') }}
),

machine as (
    select
        -- ids
        cast(machinekey as numeric(38, 0)) as machinekey_updated,
        cast(storekey as numeric(38, 0)) as storekey_updated,

        -- strings
        machinetype,
        machinename,
        machinedescription,
        vendorname,
        machineos,
        machinesource,
        machinehardware,
        machinesoftware,
        status,

        -- dates
        cast(servicestartdate as date) as servicestartdate_updated,
        cast(decommissiondate as date) as decommissiondate_updated,
        cast(lastmodifieddate as date) as lastmodifieddate_updated,

        -- additional
        cast(loaddate as timestamp_ntz) as created_at,
        datediff('year', servicestartdate_updated, decommissiondate_updated)
            as yearsservicelife,

        -- creation timing
        datediff('day', lastmodifieddate_updated, cast('2009-12-31' as date))
            as dayssincelastmodification

    from source
)

select * from machine
