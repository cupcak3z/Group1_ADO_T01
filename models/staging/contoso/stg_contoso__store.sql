with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSTORE_RAW') }}
),

store as (
    select
    --ids
    cast(STOREKEY as numeric) as STOREKEY_updated,
    cast(GEOGRAPHYKEY as numeric) as GEOGRAPHYKEY_updated,
    cast(ENTITYKEY as numeric) as ENTITYKEY_updated,

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
    cast(STOREMANAGER as numeric) as STOREMANAGER_updated,
    case
      when EMPLOYEECOUNT = 'NULL' then 18
      else cast(EMPLOYEECOUNT as numeric)
    end as EMPLOYEECOUNT_Updated,
    cast(SELLINGAREASIZE as numeric) as SELLINGAREASIZE_updated,

    --date
    cast(OPENDATE as date) as OPENDATE_updated,
    case
      when CLOSEDATE = 'NULL' then to_date('2090-12-31', 'YYYY-MM-DD')
      else to_date(CLOSEDATE, 'YYYY-MM-DD')
    end as CLOSEDATE_updated,

    --TIMESTAMP_NTZ
    LASTREMODELDATE,

    --creation date
    LOADDATE::timestamp_ntz as created_at

    from source
)

SELECT * FROM store