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
        cast(loaddate as timestamp_ntz) as created_at,

        -- additional
        case
            when enddate = 'NULL' then null
            else cast(enddate as date)
        end as enddate_updated,

        -- creation timing
        case
            when getdate() < startdate_updated
                then
                    -datediff(
                        'day', getdate(), startdate_updated
                    )
            else
                datediff('day', startdate_updated, getdate())
        end as yearssincesalesterritorystart

    from source
)

select * from salesterritory
