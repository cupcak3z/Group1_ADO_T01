with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCUSTOMER_RAW')}}
),

customer as (
    select
        -- ids
        cast(CUSTOMERKEY as numeric) as CUSTOMERKEY_updated,
        cast(GEOGRAPHYKEY as numeric) as GEOGRAPHYKEY_updated,
        -- strings
        FIRSTNAME,
        LASTNAME, 
        case 
           when MARITALSTATUS ='M' then 'Married'
           else 'Single'
        end as MARITALSTATUS_Updated,
        case
           when GENDER ='M' then 'Male'
           else 'Female'
        end as GENDER_updated,
        EMAILADDRESS,
        EDUCATION,
        OCCUPATION,
        ADDRESSLINE1,
        PHONE,
        CUSTOMERTYPE,
        case 
           when COMPANYNAME = 'NULL' then 'No Company Entered'
           else COMPANYNAME
        end as COMPANYNAME_updated,
        -- Numbers
        cast(yearlyincome as numeric) as yearlyincome_updated,
        cast(totalchildren as numeric) as totalchildren_updated,
        cast(numberchildrenathome as numeric) as numberchildrenathome_updated,
        cast(houseownerflag as numeric) as houseownerflag_updated,
        cast(numbercarsowned as numeric) as numbercarsowned_updated,
        --date
        BIRTHDATE,
        DATEFIRSTPURCHASE,
        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from customer