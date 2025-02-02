-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMDATE_RAW') }}
),

date as (
    select
        -- ids
        cast(datekey as date) as datekey_updated, -- converting data type to ensure correct parsing
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
        cast(calendaryear as numeric(38, 0)) as calendaryear_updated, -- converting data type to ensure correct parsing
        cast(fiscalyear as numeric(38, 0)) as fiscalyear_updated, -- converting data type to ensure correct parsing
        cast(extract(month from datekey) as numeric(38, 0)) -- converting data type to ensure correct parsing
            as monthnumber_updated,
        cast(extract(dayofweek from datekey) as numeric(38, 0)) -- converting data type to ensure correct parsing
            as calendardayofweeknumber_updated,
        -- Numbers
        case
            when isholiday = 1 then 'Yes' -- convert values for easier understanding
            else 'No'
        end as isholiday_updated,
        case
            when europeseason = 'NULL' then 'No Season' -- checking and replacing null values
            else europeseason
        end as europeseason_updated,
        case
            when northamericaseason = 'NULL' then 'No Season' -- checking and replacing null values
            else northamericaseason
        end as northamericaseason_updated,
        case
            when asiaseason = 'NULL' then 'No Season' -- checking and replacing null values
            else asiaseason
        end as asiaseason_updated,
        -- Creation timing
        to_timestamp(datekey) as created_at -- converting data type to ensure correct parsing
    from source
)

-- putting the transformed data into a table
select * from date
