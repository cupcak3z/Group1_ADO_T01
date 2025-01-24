with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSTORE_RAW') }}
),

store as (
    select
    --ids
        cast(storekey as numeric(38, 0)) as storekey_updated,
        cast(geographykey as numeric(38, 0)) as geographykey_updated,
        cast(entitykey as numeric(38, 0)) as entitykey_updated,

        --strings
        storetype,
        storename,
        storephone,
        storefax,
        status,
        cast(storemanager as numeric(38, 0)) as storemanager_updated,

        --numbers
        cast(sellingareasize as numeric(38, 0)) as sellingareasize_updated,
        cast(opendate as date) as opendate_updated,
        lastremodeldate,

        --date
        cast(loaddate as timestamp_ntz) as created_at,
        case
            when closereason = 'NULL' then 'Not Yet Closed'
            else closereason
        end as closereason_updated,

        --TIMESTAMP_NTZ
        case
            when employeecount = 'NULL' then 18
            else cast(employeecount as numeric(38, 0))
        end as employeecount_updated,

        -- additional
        case
            when closedate = 'NULL' then to_date('2090-12-31', 'YYYY-MM-DD')
            else to_date(closedate, 'YYYY-MM-DD')
        end as closedate_updated,
        datediff('year', opendate_updated, getdate())
            as yearssinceopen,
        datediff('year', opendate_updated, closedate_updated) as yearsleft,

        --creation date
        datediff('day', lastremodeldate, getdate())
            as dayssincelastremodel

    from source
)

select * from store
