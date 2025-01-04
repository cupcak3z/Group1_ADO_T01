with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMDATE_RAW')}}
),

date as (
    select
        -- ids
        cast(DATEKEY as date) as DATEKEY_updated ,
        -- strings
        CALENDARYEARLABEL,
        CALENDARHALFYEARLABEL,
        CALENDARQUARTERLABEL,
        CALENDARMONTHLABEL,
        CALENDARWEEKLABEL,
        CALENDARDAYOFWEEKLABEL,
        FISCALYEARLABEL
        FISCALHALFYEARLABEL,
        FISCALMONTHLABEL,
        FISCALQUARTERLABEL,
        case
           when ISHOLIDAY = 1 then 'Yes'
           else 'No'
        end as ISHOLIDAY_updated,
        ISWORKDAY,
        case 
           when EUROPESEASON = 'NULL' then 'No Season'
           else EUROPESEASON
        end as EUROPESEASON_UPDATED,
        case 
           when NORTHAMERICASEASON = 'NULL' then 'No Season'
           else NORTHAMERICASEASON
        end as NORTHAMERICASEASON_UPDATED,
        case 
           when ASIASEASON = 'NULL' then 'No Season'
           else ASIASEASON
        end as ASIASEASON_UPDATED,
        -- Numbers
        cast(CALENDARYEAR as numeric(38,0)) as CALENDARYEAR_UPDATED,
        cast(FISCALYEAR as numeric(38,0)) as FISCALYEAR_UPDATED,
        cast(extract(month from DATEKEY) as numeric(38,0)) as MONTHNUMBER_UPDATED,
        cast(extract(dayofweek from DATEKEY) as numeric(38,0)) as CALENDARDAYOFWEEKNUMBER_UPDATED,
        -- Creation timing
        TO_TIMESTAMP(DATEKEY) as created_at
    from source
)

select * from date
