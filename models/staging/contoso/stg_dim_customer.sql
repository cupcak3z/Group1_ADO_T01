-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCUSTOMER_RAW') }}
),

customer as (
    select
        -- ids
        cast(customerkey as numeric(38, 0)) as customerkey_updated, -- converting data type to ensure correct parsing
        cast(geographykey as numeric(38, 0)) as geographykey_updated, -- converting data type to ensure correct parsing
        -- strings
        emailaddress,
        education,
        occupation,
        addressline1,
        phone,
        customertype,
        cast(loaddate as timestamp_ntz) as created_at, -- converting data type to ensure correct parsing
        cast(firstname as string) -- converting data type to ensure correct parsing
        || ' '
        || cast(lastname as string) as fullname, -- converting data type to ensure correct parsing
        case
            when maritalstatus = 'M' then 'Married'
            else 'Single'
        end as maritalstatus_updated, -- convert values for easier understanding
        case
            when gender = 'M' then 'Male'
            else 'Female'
        end as gender_updated, -- convert values for easier understanding
        -- Numbers
        case
            when companyname = 'NULL' then 'No Company Entered' -- checking and replacing null values
            else companyname
        end as companyname_updated,
        case
            when yearlyincome is NULL then NULL -- checking and replacing null values
            else cast(yearlyincome as numeric(10, 2))
        end as yearlyincome_updated,
        case
            when totalchildren = 'NULL' then NULL -- checking and replacing null values
            else cast(totalchildren as numeric(38, 0)) -- converting data type to ensure correct parsing
        end as totalchildren_updated,
        case
            when numberchildrenathome = 'NULL' then NULL -- checking and replacing null values
            else cast(numberchildrenathome as numeric(38, 0)) -- converting data type to ensure correct parsing
        end as numberchildrenathome_updated,
        case
            when houseownerflag = 'NULL' then NULL -- checking and replacing null values
            else cast(houseownerflag as numeric(38, 0)) -- converting data type to ensure correct parsing
        end as houseownerflag_updated,
        -- date
        case
            when numbercarsowned = 'NULL' then NULL -- checking and replacing null values
            else cast(numbercarsowned as numeric(38, 0)) -- converting data type to ensure correct parsing
        end as numbercarsowned_updated,
        case
            when birthdate = 'NULL' or birthdate is NULL then NULL -- checking and replacing null values
            else cast(birthdate as date) -- converting data type to ensure correct parsing
        end as birthdate_updated,
        case
            when
                datefirstpurchase = 'NULL' or datefirstpurchase is NULL -- checking and replacing null values
                then NULL
            else cast(datefirstpurchase as date) -- converting data type to ensure correct parsing
        end as datefirstpurchase_updated,
        case
            when birthdate_updated is NULL then NULL  -- checking and replacing null values
            else datediff('year', birthdate_updated, getdate()) -- creating derived metrics
        end as customerage,

        -- creation timing
        case
            when datefirstpurchase_updated is NULL then NULL  -- checking and replacing null values
            else
                datediff(
                    'day', datefirstpurchase_updated, getdate() -- creating derived metrics
                )
        end as dayssincefirstpurchase

    from source
)

-- putting the transformed data into a table
select * from customer
