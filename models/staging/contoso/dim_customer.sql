with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCUSTOMER_RAW') }}
),

customer as (
    select
        -- ids
        cast(customerkey as numeric(38, 0)) as customerkey_updated,
        cast(geographykey as numeric(38, 0)) as geographykey_updated,
        -- strings
        emailaddress,
        education,
        occupation,
        addressline1,
        phone,
        customertype,
        cast(loaddate as timestamp_ntz) as created_at,
        cast(firstname as string)
        || ' '
        || cast(lastname as string) as fullname,
        case
            when maritalstatus = 'M' then 'Married'
            else 'Single'
        end as maritalstatus_updated,
        case
            when gender = 'M' then 'Male'
            else 'Female'
        end as gender_updated,
        -- Numbers
        case
            when companyname = 'NULL' then 'No Company Entered'
            else companyname
        end as companyname_updated,
        case
            when yearlyincome is NULL then NULL
            else cast(yearlyincome as numeric(10, 2))
        end as yearlyincome_updated,
        case
            when totalchildren = 'NULL' then NULL
            else cast(totalchildren as numeric(38, 0))
        end as totalchildren_updated,
        case
            when numberchildrenathome = 'NULL' then NULL
            else cast(numberchildrenathome as numeric(38, 0))
        end as numberchildrenathome_updated,
        case
            when houseownerflag = 'NULL' then NULL
            else cast(houseownerflag as numeric(38, 0))
        end as houseownerflag_updated,
        -- date
        case
            when numbercarsowned = 'NULL' then NULL
            else cast(numbercarsowned as numeric(38, 0))
        end as numbercarsowned_updated,
        case
            when birthdate = 'NULL' or birthdate is NULL then NULL
            else cast(birthdate as date)
        end as birthdate_updated,
        case
            when
                datefirstpurchase = 'NULL' or datefirstpurchase is NULL
                then NULL
            else cast(datefirstpurchase as date)
        end as datefirstpurchase_updated,
        case
            when birthdate_updated is NULL then NULL
            else datediff('year', birthdate_updated, getdate())
        end as customerage,

        -- creation timing
        case
            when datefirstpurchase_updated is NULL then NULL
            else
                datediff(
                    'day', datefirstpurchase_updated, getdate()
                )
        end as dayssincefirstpurchase

    from source
)

select * from customer
