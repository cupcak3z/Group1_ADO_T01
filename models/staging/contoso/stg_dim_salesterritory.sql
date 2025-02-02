-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSALESTERRITORY_RAW') }}
),

salesterritory as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(salesterritorykey as numeric(38, 0)) as salesterritorykey_updated,
        -- converting data type to ensure correct parsing
        cast(geographykey as numeric(38, 0)) as geographykey_updated,
        -- converting data type to ensure correct parsing
        cast(salesterritorymanager as numeric(38, 0))
            as salesterritorymanager_updated,

        -- strings
        salesterritoryname,
        salesterritoryregion,
        salesterritorycountry,
        salesterritorygroup,
        status,

        -- numbers
        -- converting data type to ensure correct parsing
        cast(salesterritorylevel as numeric(38, 0))
            as salesterritorylevel_updated,

        -- dates
        -- converting data type to ensure correct parsing
        cast(startdate as date) as startdate_updated,
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,

        -- additional
        case
            -- checking and replacing null values, converting data type
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
        end as yearssincesalesterritorystart -- creating derived metrics

    from source
)

-- putting the transformed data into a table
select * from salesterritory
