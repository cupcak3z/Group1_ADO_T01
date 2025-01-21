with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSALESTERRITORY_RAW')}}
),

salesterritory as (
    select
        -- ids
        cast(SALESTERRITORYKEY as numeric(38,0)) as SALESTERRITORYKEY_updated,
        cast(GEOGRAPHYKEY as numeric(38,0)) as GEOGRAPHYKEY_updated,
        cast(SALESTERRITORYMANAGER as numeric(38,0)) as SALESTERRITORYMANAGER_updated,

        -- strings
        SALESTERRITORYNAME,
        SALESTERRITORYREGION,
        SALESTERRITORYCOUNTRY, 
        SALESTERRITORYGROUP,
        STATUS,
        
        -- numbers
        cast(SALESTERRITORYLEVEL as numeric(38,0)) as SALESTERRITORYLEVEL_updated,

        -- dates
        cast(STARTDATE as date) as STARTDATE_updated,
        case
            when ENDDATE = 'NULL' then null
            else cast(ENDDATE as date)
        end as ENDDATE_updated,
        
        -- additional
        case
            when cast('2009-12-31' as date) < STARTDATE_updated then 
                -datediff('day', cast('2009-12-31' as date), STARTDATE_updated)
        else
            datediff('day', STARTDATE_updated, cast('2009-12-31' as date))
        end as YEARSSINCESALESTERRITORYSTART,           

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from salesterritory