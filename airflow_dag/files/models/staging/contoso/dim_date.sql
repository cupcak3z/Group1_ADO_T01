with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMDATE_RAW') }}
),

date as (
    select
        -- ids
        cast(datekey as date) as datekey_updated,
        -- strings
        calendaryearlabel,
        calendarhalfyearlabel,
        calendarquarterlabel,
        calendarmonthlabel,
        calendarweeklabel,
        calendardayofweeklabel,
        fiscalyearlabel,
        fiscalhalfyearlabel,
        fiscalmonthlabel,
        fiscalquarterlabel,
        isworkday,
        cast(calendaryear as numeric(38, 0)) as calendaryear_updated,
        cast(fiscalyear as numeric(38, 0)) as fiscalyear_updated,
        cast(extract(month from datekey) as numeric(38, 0))
            as monthnumber_updated,
        cast(extract(dayofweek from datekey) as numeric(38, 0))
            as calendardayofweeknumber_updated,
        -- Numbers
        case
            when isholiday = 1 then 'Yes'
            else 'No'
        end as isholiday_updated,
        case
            when europeseason = 'NULL' then 'No Season'
            else europeseason
        end as europeseason_updated,
        case
            when northamericaseason = 'NULL' then 'No Season'
            else northamericaseason
        end as northamericaseason_updated,
        case
            when asiaseason = 'NULL' then 'No Season'
            else asiaseason
        end as asiaseason_updated,
        -- Creation timing
        to_timestamp(datekey) as created_at
    from source
)

select * from date
