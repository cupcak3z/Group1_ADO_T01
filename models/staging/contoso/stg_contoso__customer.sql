
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
        case
           when yearlyincome IS NULL then null
           else cast(yearlyincome as numeric(10,2))
        end as yearlyincome_updated,
        case
           when totalchildren = 'NULL' then null
           else cast(totalchildren as numeric(38,0))
        end as totalchildren_updated,
        case
           when numberchildrenathome = 'NULL' then null
           else cast(numberchildrenathome as numeric(38,0))
        end as numberchildrenathome_updated,
        case 
           when houseownerflag = 'NULL' then null
           else cast(houseownerflag as numeric(38,0))
        end as houseownerflag_updated,
        case
           when numbercarsowned = 'NULL' then null
           else cast(numbercarsowned as numeric(38,0))
        end as numbercarsowned_updated,
        -- date
        case
           when BIRTHDATE = 'NULL' or BIRTHDATE IS NULL then null
           else cast(BIRTHDATE as date)
        end as BIRTHDATE_updated,
        case
            when DATEFIRSTPURCHASE = 'NULL' or DATEFIRSTPURCHASE IS NULL then null
            else cast(DATEFIRSTPURCHASE as date)
        end as DATEFIRSTPURCHASE_updated,
        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from customer