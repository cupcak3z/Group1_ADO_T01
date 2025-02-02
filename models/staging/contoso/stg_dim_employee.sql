-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMEMPLOYEE_RAW') }}
),

employee as (
    select
        -- ids
        cast(employeekey as numeric(38, 0)) as employeekey_updated, -- converting data type to ensure correct parsing
        title,

        -- strings
        emailaddress,
        phone,
        emergencycontactname,
        emergencycontactphone,
        departmentname,
        cast(hiredate as date) as hiredate_updated, -- converting data type to ensure correct parsing
        cast(birthdate as date) as birthdate_updated, -- converting data type to ensure correct parsing
        cast(startdate as date) as startdate_updated, -- converting data type to ensure correct parsing
        cast(payfrequency as numeric(38, 0)) as payfrequency_updated, -- converting data type to ensure correct parsing

        cast(baserate as numeric(38, 2)) as baserate_updated, -- converting data type to ensure correct parsing

        cast(vacationhours as numeric(38, 0)) as vacationhours_updated, -- converting data type to ensure correct parsing

        cast(loaddate as timestamp_ntz) as created_at, -- converting data type to ensure correct parsing

        -- dates
        case
            when
                parentemployeekey = 'NULL' -- checking and replacing null values, then converting data type
                then cast(employeekey as numeric(38, 0)) 
            else cast(parentemployeekey as numeric(38, 0))
        end as parentemployeekey_updated,
        cast(firstname as string) -- converting data type to ensure correct parsing
        || ' '
        || cast(lastname as string) as fullname, -- converting data type to ensure correct parsing
        case
            when gender = 'M' then 'Male' -- convert values for easier understanding
            else 'Female'
        end as gender_updated,

        -- numbers
        case
            when currentflag = 1 then 'Current'
            when currentflag = 0 then 'Not Current'
            else 'Not Current'
        end as status_updated, -- convert values for easier understanding
        case
            when salariedflag = '1' then 'Salaried'
            else 'Not Salaried'
        end as salarystatus, -- convert values for easier understanding
        case
            when salespersonflag = '1' then 'Yes'
            else 'No'
        end as issalesperson, -- creating derived metrics
        case
            when maritalstatus = 'M' then 'Yes'
            else 'No'
        end as ismarried, -- creating derived metrics
        datediff('year', hiredate_updated, getdate()) -- creating derived metrics
            as yearssincehired,
        datediff('year', birthdate_updated, getdate()) -- creating derived metrics
            as employeeage,

        -- creation timing
        datediff('day', hiredate_updated, startdate_updated) as daystoonboard -- creating derived metrics

    from source
)

-- putting the transformed data into a table
select * from employee
