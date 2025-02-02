-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSTORE_RAW') }}
),

store as (
    select
    --ids
        -- converting data type
        cast(storekey as numeric(38, 0)) as storekey_updated,
        -- converting data type
        cast(geographykey as numeric(38, 0)) as geographykey_updated,
        -- converting data type
        cast(entitykey as numeric(38, 0)) as entitykey_updated,

        --strings
        storetype,
        storename,
        storephone,
        storefax,
        status,
        -- converting data type
        cast(storemanager as numeric(38, 0)) as storemanager_updated,

        --numbers
        -- converting data type
        cast(sellingareasize as numeric(38, 0)) as sellingareasize_updated,
        cast(opendate as date) as opendate_updated, -- converting data type
        lastremodeldate,

        --date
        cast(loaddate as timestamp_ntz) as created_at, -- converting data type
        case
            -- checking and replacing null values
            when closereason = 'NULL' then 'Not Yet Closed'
            else closereason
        end as closereason_updated,

        --TIMESTAMP_NTZ
        case
            -- checking and replacing null values, converting data type
            when employeecount = 'NULL' then 18
            else cast(employeecount as numeric(38, 0))
        end as employeecount_updated,

        -- additional
        case
            when closedate = 'NULL' then to_date('2090-12-31', 'YYYY-MM-DD')
            -- checking and replacing null values, converting data type
            else to_date(closedate, 'YYYY-MM-DD')
        end as closedate_updated,
        datediff('year', opendate_updated, getdate())
            as yearssinceopen, -- creating derived metrics
        -- creating derived metrics
        datediff('year', opendate_updated, closedate_updated) as yearsleft,

        --creation date
        datediff('day', lastremodeldate, getdate())
            as dayssincelastremodel -- creating derived metrics

    from source
)

-- putting the transformed data into a table
select * from store
