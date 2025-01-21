with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMEMPLOYEE_RAW') }}
),

employee as (
    select
        -- ids
        cast(employeekey as numeric(38, 0)) as employeekey_updated,
        title,

        -- strings
        emailaddress,
        phone,
        emergencycontactname,
        emergencycontactphone,
        departmentname,
        cast(hiredate as date) as hiredate_updated,
        cast(birthdate as date) as birthdate_updated,
        cast(startdate as date) as startdate_updated,
        cast(payfrequency as numeric(38, 0)) as payfrequency_updated,

        cast(baserate as numeric(38, 2)) as baserate_updated,

        cast(vacationhours as numeric(38, 0)) as vacationhours_updated,

        cast(loaddate as timestamp_ntz) as created_at,

        -- dates
        case
            when
                parentemployeekey = 'NULL'
                then cast(employeekey as numeric(38, 0))
            else cast(parentemployeekey as numeric(38, 0))
        end as parentemployeekey_updated,
        cast(firstname as string)
        || ' '
        || cast(lastname as string) as fullname,
        case
            when gender = 'M' then 'Male'
            else 'Female'
        end as gender_updated,

        -- numbers
        case
            when currentflag = 1 then 'Current'
            when currentflag = 0 then 'Not Current'
            else 'Not Current'
        end as status_updated,
        case
            when salariedflag = '1' then 'Salaried'
            else 'Not Salaried'
        end as salarystatus,
        case
            when salespersonflag = '1' then 'Yes'
            else 'No'
        end as issalesperson,
        case
            when maritalstatus = 'M' then 'Yes'
            else 'No'
        end as ismarried,
        datediff('year', hiredate_updated, cast('2009-12-31' as date))
            as yearssincehired,
        datediff('year', birthdate_updated, cast('2009-12-31' as date))
            as employeeage,

        -- creation timing
        datediff('day', hiredate_updated, startdate_updated) as daystoonboard

    from source
)

select * from employee
