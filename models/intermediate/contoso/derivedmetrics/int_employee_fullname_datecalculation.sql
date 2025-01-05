with
employee as (
    select * from {{ ref('stg_contoso__employee') }}
),

employee_derivedmetrics as (
    select
        -- ids
        EMPLOYEEKEY_UPDATED,
        PARENTEMPLOYEEKEY_UPDATED,

        -- strings
        cast(FIRSTNAME as string) || ' ' || cast(LASTNAME as string) as FULLNAME,        
        TITLE,
        GENDER_UPDATED,
        DEPARTMENTNAME,
        STATUS_UPDATED,
        SALARYSTATUS,
        ISSALESPERSON,
        ISMARRIED,

        -- numbers
        datediff('year', HIREDATE_UPDATED, cast('2009-10-01' as date)) as YEARSSINCEHIRED,
        datediff('year', BIRTHDATE_UPDATED, cast('2009-10-01' as date)) as EMPLOYEEAGE,
        datediff('day', HIREDATE_UPDATED, STARTDATE_UPDATED) as DAYSTOONBOARD,
        PAYFREQUENCY_UPDATED,
        BASERATE_UPDATED,
        VACATIONHOURS_UPDATED,
        
        -- creation timing
        CREATED_AT

    from employee
)

select * from employee_derivedmetrics