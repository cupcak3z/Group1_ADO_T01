with
store as (
    select * from {{ ref('stg_contoso__store') }}
),

store_derivedmetrics as (
    select
        -- ids
        STOREKEY_UPDATED,
        GEOGRAPHYKEY_UPDATED,
        ENTITYKEY_UPDATED,
        STOREMANAGER_UPDATED,

        -- strings
        STORETYPE,
        STORENAME,
        STATUS,        
        CLOSEREASON_UPDATED,

        -- numbers
        EMPLOYEECOUNT_UPDATED,
        SELLINGAREASIZE_UPDATED,
        EMPLOYEECOUNT_UPDATED / SELLINGAREASIZE_UPDATED as EMPLOYEESPERSELLINGAREASIZE, 
        datediff('year', OPENDATE_UPDATED, cast('2009-10-01' as date)) as YEARSSINCEOPEN,
        datediff('year', OPENDATE_UPDATED, CLOSEDATE_UPDATED) as YEARSLEFT,
        datediff('day', LASTREMODELDATE, cast('2009-10-01' as date)) as DAYSSINCELASTREMODEL,

        -- creation timing
        CREATED_AT

    from store
)

select * from store_derivedmetrics