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
        MARITALSTATUS_UPDATED,
        GENDER_UPDATED,
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
        case
            when BIRTHDATE_UPDATED is null then null
            else datediff('year', BIRTHDATE_UPDATED, cast('2009-10-01' as date)) 
        end as CUSTOMERAGE,
        case
            when DATEFIRSTPURCHASE_UPDATED is null then null
           else datediff('day', DATEFIRSTPURCHASE_UPDATED, cast('2009-10-01' as date)) 
        end as DAYSSINCEFIRSTPURCHASE, 

        -- creation timing
        CREATED_AT        

    from customer 
)

select * from customer_derivedmetrics