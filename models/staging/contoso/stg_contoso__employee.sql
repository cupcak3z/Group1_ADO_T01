with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMEMPLOYEE_RAW')}}
),

employee as (
    select
        -- ids
        cast(EMPLOYEEKEY as numeric) as EMPLOYEEKEY_updated,
        case
            when PARENTEMPLOYEEKEY = 'NULL' then 1 
            else cast(PARENTEMPLOYEEKEY as numeric)
        end as PARENTEMPLOYEEKEY_updated,

        -- strings
        FIRSTNAME,
        LASTNAME,
        TITLE,
        EMAILADDRESS,
        PHONE,
        EMERGENCYCONTACTNAME,
        EMERGENCYCONTACTPHONE,
        GENDER,
        case
            when GENDER = 'M' then 'Male'
            else 'Female'
        end as GENDER_updated,
        DEPARTMENTNAME,
        case
            when CURRENTFLAG = 1 then 'Current'
            when CURRENTFLAG = 0 then 'Not Current'
            else 'Not Current'
        end as STATUS_updated,

        case
            when SALARIEDFLAG = '1' then 'Salaried'
            else 'Not Salaried'
        end as SALARYSTATUS,

        case
            when SALESPERSONFLAG = '1' then 'Yes'
            else 'No'
        end as ISSALESPERSON,

        case 
            when MARITALSTATUS = 'M' then 'Yes'
            else 'No'
        end as ISMARRIED,

        -- dates
        cast(HIREDATE as date) as HIREDATE_updated,
        cast(BIRTHDATE as date) as BIRTHDATE_updated,
        cast(STARTDATE as date) as STARTDATE_updated,

        -- numbers
        cast(PAYFREQUENCY as numeric) as PAYFREQUENCY_updated,
        cast(BASERATE as numeric) as BASERATE_updated,
        cast(VACATIONHOURS as numeric) as VACATIONHOURS_updated,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from employee