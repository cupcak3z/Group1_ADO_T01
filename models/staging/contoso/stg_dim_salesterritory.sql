-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSALESTERRITORY_RAW') }}
),

salesterritory as (
    select
        -- ids
        cast(salesterritorykey as numeric(38, 0)) as salesterritorykey_updated, -- converting data type to ensure correct parsing
        cast(geographykey as numeric(38, 0)) as geographykey_updated, -- converting data type to ensure correct parsing
        cast(salesterritorymanager as numeric(38, 0)) -- converting data type to ensure correct parsing
            as salesterritorymanager_updated,

        -- strings
        salesterritoryname,
        salesterritoryregion,
        salesterritorycountry,
        salesterritorygroup,
        status,

        -- numbers
        cast(salesterritorylevel as numeric(38, 0)) -- converting data type to ensure correct parsing
            as salesterritorylevel_updated,

        -- dates
        cast(startdate as date) as startdate_updated, -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at, -- converting data type to ensure correct parsing

        -- additional
        case
            when enddate = 'NULL' then null -- checking and replacing null values, converting data type
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
