with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCUSTOMER_RAW')}}
),

customer as (
    select
        -- ids
        cast(CUSTOMERKEY as numeric(38,0)) as CUSTOMERKEY_updated,
        cast(GEOGRAPHYKEY as numeric(38,0)) as GEOGRAPHYKEY_updated,
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
        cast(yearlyincome as numeric(10,2)) as yearlyincome_updated,
        cast(totalchildren as numeric(38,0)) as totalchildren_updated,
        cast(numberchildrenathome as numeric(38,0)) as numberchildrenathome_updated,
        cast(houseownerflag as numeric(38,0)) as houseownerflag_updated,
        cast(numbercarsowned as numeric(38,0)) as numbercarsowned_updated,
        --date
        cast(BIRTHDATE as date) as BIRTHDATE_updated,
        cast(DATEFIRSTPURCHASE as date) as DATEFIRSTPURCHASE_updated,
        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from customer