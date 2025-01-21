with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSALESTERRITORY_RAW') }}
),

salesterritory as (
    select
        -- ids
        cast(salesterritorykey as numeric(38, 0)) as salesterritorykey_updated,
        cast(geographykey as numeric(38, 0)) as geographykey_updated,
        cast(salesterritorymanager as numeric(38, 0))
            as salesterritorymanager_updated,

        -- strings
        salesterritoryname,
        salesterritoryregion,
        salesterritorycountry,
        salesterritorygroup,
        status,

        -- numbers
        cast(salesterritorylevel as numeric(38, 0))
            as salesterritorylevel_updated,

        -- dates
        cast(startdate as date) as startdate_updated,
        case
            when enddate = 'NULL' then null
            else cast(enddate as date)
        end as enddate_updated,

        -- additional
        case
            when cast('2009-12-31' as date) < startdate_updated
                then
                    -datediff(
                        'day', cast('2009-12-31' as date), startdate_updated
                    )
            else
                datediff('day', startdate_updated, cast('2009-12-31' as date))
        end as yearssincesalesterritorystart,

        -- creation timing
        cast (loaddate as timestamp_ntz) as created_at

    from source
)

select * from salesterritory
