-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMDATE_RAW') }}
),

date as (
    select
        -- ids
        -- converting data type to ensure correct parsing
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
        -- converting data type to ensure correct parsing
        cast(calendaryear as numeric(38, 0)) as calendaryear_updated,
        -- converting data type to ensure correct parsing
        cast(fiscalyear as numeric(38, 0)) as fiscalyear_updated,
        -- converting data type to ensure correct parsing
        cast(extract(month from datekey) as numeric(38, 0))
            as monthnumber_updated,
        -- converting data type to ensure correct parsing
        cast(extract(dayofweek from datekey) as numeric(38, 0))
            as calendardayofweeknumber_updated,
        -- Numbers
        case
            -- convert values for easier understanding
            when isholiday = 1 then 'Yes'
            else 'No'
        end as isholiday_updated,
        case
            -- checking and replacing null values
            when europeseason = 'NULL' then 'No Season'
            else europeseason
        end as europeseason_updated,
        case
            -- checking and replacing null values
            when northamericaseason = 'NULL' then 'No Season'
            else northamericaseason
        end as northamericaseason_updated,
        case
            -- checking and replacing null values
            when asiaseason = 'NULL' then 'No Season'
            else asiaseason
        end as asiaseason_updated,
        -- Creation timing
        -- converting data type to ensure correct parsing
        to_timestamp(datekey) as created_at
    from source
)

-- putting the transformed data into a table
select * from date
