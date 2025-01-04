with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMEMPLOYEE_RAW')}}
),

employee as (
    select
        -- ids
        EMPLOYEEKEY,
        case
            when PARENTEMPLOYEEKEY = 'NULL' then '1'
            else PARENTEMPLOYEEKEY
        end as PARENTEMPLOYEEKEY_updated

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
        end as GENDER_updated
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
            when MARITIALSTATUS = 'M' then 'Yes'
            else 'No'
        end as ISMARRIED,

        -- dates
        to_char(HIREDDATE, 'DD/MM/YYYY') as HIREDDATE_formatted,
        to_char(BIRTHDATE, 'DD/MM/YYYY') as BIRTHDATE_formatted,
        to_char(STARTDATE, 'DD/MM/YYYY') as STARTDATE_formatted,

        -- numbers
        PAYFREQUENCY,
        BASERATE,
        VACATIONHOURS,

        -- creation timing
        date_trunc('day', LOADDATE) as created_date,
        LOADDATE::timestamp_ntz as created_date

    from source
)

select * from employee