-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMMACHINE_RAW') }}
),

machine as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(machinekey as numeric(38, 0)) as machinekey_updated,
        -- converting data type to ensure correct parsing
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
        -- converting data type to ensure correct parsing
        cast(servicestartdate as date) as servicestartdate_updated,
        -- converting data type to ensure correct parsing
        cast(decommissiondate as date) as decommissiondate_updated,
        -- converting data type to ensure correct parsing
        cast(lastmodifieddate as date) as lastmodifieddate_updated,

        -- additional
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,
        datediff('year', servicestartdate_updated, decommissiondate_updated)
            as yearsservicelife, -- creating derived metrics

        -- creation timing
        datediff('day', lastmodifieddate_updated, getdate())
            as dayssincelastmodification -- creating derived metrics

    from source
)

-- putting the transformed data into a table
select * from machine
