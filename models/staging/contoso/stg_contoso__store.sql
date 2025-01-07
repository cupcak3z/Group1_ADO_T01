with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSTORE_RAW') }}
),

store as (
    select
    --ids
    cast(STOREKEY as numeric(38,0)) as STOREKEY_updated,
    cast(GEOGRAPHYKEY as numeric(38,0)) as GEOGRAPHYKEY_updated,
    cast(ENTITYKEY as numeric(38,0)) as ENTITYKEY_updated,

    --strings
    STORETYPE,
    STORENAME,
    STOREPHONE,
    STOREFAX,
    STATUS,
    case
      when CLOSEREASON = 'NULL' then 'Not Yet Closed'
      else CLOSEREASON
    end as CLOSEREASON_updated,

    --numbers
    cast(STOREMANAGER as numeric(38,0)) as STOREMANAGER_updated,
    case
      when EMPLOYEECOUNT = 'NULL' then 18
      else cast(EMPLOYEECOUNT as numeric(38,0))
    end as EMPLOYEECOUNT_updated,
    cast(SELLINGAREASIZE as numeric(38,0)) as SELLINGAREASIZE_updated,

    --date
    cast(OPENDATE as date) as OPENDATE_updated,
    case
      when CLOSEDATE = 'NULL' then to_date('2090-12-31', 'YYYY-MM-DD')
      else to_date(CLOSEDATE, 'YYYY-MM-DD')
    end as CLOSEDATE_updated,

    --TIMESTAMP_NTZ
    LASTREMODELDATE,

    -- additional
    datediff('year', OPENDATE_updated, cast('2009-10-01' as date)) as YEARSSINCEOPEN,
    datediff('year', OPENDATE_updated, CLOSEDATE_updated) as YEARSLEFT,
    datediff('day', LASTREMODELDATE, cast('2009-10-01' as date)) as DAYSSINCELASTREMODEL,

    --creation date
    LOADDATE::timestamp_ntz as created_at

    from source
)

SELECT * FROM store
