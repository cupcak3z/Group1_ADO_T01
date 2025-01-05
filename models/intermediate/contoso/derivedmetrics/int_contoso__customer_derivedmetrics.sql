with
customer as (
    select * from {{ ref('stg_contoso__customer') }}
),

customer_derivedmetrics as (
    select
        -- ids
        CUSTOMERKEY_UPDATED,
        GEOGRAPHYKEY_UPDATED,

        -- strings
        cast(FIRSTNAME as string) || ' ' || cast(LASTNAME as string) as FULLNAME,
        MARITALSTATUS_Updated,
        GENDER_updated
        EDUCATION,
        OCCUPATION,
        ADDRESSLINE1,
        CUSTOMERTYPE,
        COMPANYNAME_UPDATED,

        -- numbers
        YEARLYINCOME_UPDATED,
        TOTALCHILDREN_UPDATED,
        NUMBERCHILDRENATHOME_UPDATED,
        HOUSEOWNERFLAG_UPDATED,
        NUMBERCARSOWNED_UPDATED,
        datediff('year', BIRTHDATE_UPDATED, cast('2009-10-01' as date)) as CUSTOMERAGE,
        datediff('year', DATEFIRSTPURCHASE_UPDATED, cast('2009-10-01' as date)) as YEARSSINCEFIRSTPURCHASE,

        -- creation timing
        CREATED_AT        

    from customer 
)

select * from customer_derivedmetrics