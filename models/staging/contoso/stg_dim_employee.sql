-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMEMPLOYEE_RAW') }}
),

employee as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(employeekey as numeric(38, 0)) as employeekey_updated,
        title,

        -- strings
        emailaddress,
        phone,
        emergencycontactname,
        emergencycontactphone,
        departmentname,
        -- converting data type to ensure correct parsing
        cast(hiredate as date) as hiredate_updated,
        -- converting data type to ensure correct parsing
        cast(birthdate as date) as birthdate_updated,
        -- converting data type to ensure correct parsing
        cast(startdate as date) as startdate_updated,
        -- converting data type to ensure correct parsing
        cast(payfrequency as numeric(38, 0)) as payfrequency_updated,

        -- converting data type to ensure correct parsing
        cast(baserate as numeric(38, 2)) as baserate_updated,

        -- converting data type to ensure correct parsing
        cast(vacationhours as numeric(38, 0)) as vacationhours_updated,

        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,

        -- dates
        case
            when
                -- checking and replacing null values, then converting data type
                parentemployeekey = 'NULL'
                then cast(employeekey as numeric(38, 0))
            else cast(parentemployeekey as numeric(38, 0))
        end as parentemployeekey_updated,
        -- converting data type to ensure correct parsing
        cast(firstname as string)
        || ' '
        -- converting data type to ensure correct parsing
        || cast(lastname as string) as fullname,
        case
            -- convert values for easier understanding
            when gender = 'M' then 'Male'
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
        -- creating derived metrics
        datediff('year', hiredate_updated, getdate())
            as yearssincehired,
        -- creating derived metrics
        datediff('year', birthdate_updated, getdate())
            as employeeage,

        -- creation timing
        -- creating derived metrics
        datediff('day', hiredate_updated, startdate_updated) as daystoonboard

    from source
)

-- putting the transformed data into a table
select * from employee
