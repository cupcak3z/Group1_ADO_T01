with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMSTORE_RAW') }}
),

store as (
    select
    --ids
    cast(STOREKEY as numeric) as STOREKEY_Updated,
    cast(GEOGRAPHYKEY as numeric) as GEOGRAPHYKEY_Updated,
    cast(ENTITYKEY as numeric) as ENTITYKEY_Updated,

    --strings
    STORETYPE,
    STORENAME,
    STOREPHONE,
    STOREFAX,
    case
      when CLOSEREASON = 'NULL' then 'Not Yet Closed'
      else CLOSEREASON
    end as CLOSEREASON_Updated,

    --bool
    STATUS,

    --numbers
    cast(STOREMANAGER as numeric) as STOREMANAGER_Updated,
    case
      when EMPLOYEECOUNT = 'NULL' then '18'
      else cast(EMPLOYEECOUNT as numeric)
    end as EMPLOYEECOUNT_Updated,
    cast(SELLINGAREASIZE as numeric) as SELLINGAREASIZE_Updated,

    --date
    OPENDATE,
    case
      when CLOSEDATE = 'NULL' then to_date('2090-12-31', 'YYYY-MM-DD')
      else to_date(CLOSEDATE, 'YYYY-MM-DD')
    end as CLOSEDATE_Updated,

    --TIMESTAMP_NTZ
    LASTREMODELDATE,

    --creation date
    LOADDATE::timestamp_ntz as created_at

    from source
)

SELECT * FROM store