with
machine as (
    select * from {{ ref('stg_contoso__machine') }}
),

machine_derivedmetrics as (
    select
        -- ids
        MACHINEKEY_UPDATED,
        STOREKEY_UPDATED,

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

        -- numbers
        datediff('year', SERVICESTARTDATE_UPDATED, DECOMMISSIONDATE_UPDATED) as YEARSSERVICELIFE,
        datediff('day', LASTMODIFIEDDATE_UPDATED, cast('2009-10-01' as date)) as DAYSSINCELASTMODIFICATION,
        
        -- creation timing
        CREATED_AT

    from machine
)

select * from machine_derivedmetrics