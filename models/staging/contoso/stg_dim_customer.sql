-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCUSTOMER_RAW') }}
),

customer as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(customerkey as numeric(38, 0)) as customerkey_updated,
        -- converting data type to ensure correct parsing
        cast(geographykey as numeric(38, 0)) as geographykey_updated,
        -- strings
        emailaddress,
        education,
        occupation,
        addressline1,
        phone,
        customertype,
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,
        -- converting data type to ensure correct parsing
        cast(firstname as string)
        || ' '
        -- converting data type to ensure correct parsing
        || cast(lastname as string) as fullname,
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
            -- checking and replacing null values
            when companyname = 'NULL' then 'No Company Entered'
            else companyname
        end as companyname_updated,
        case
            -- checking and replacing null values
            when yearlyincome is NULL then NULL
            else cast(yearlyincome as numeric(10, 2))
        end as yearlyincome_updated,
        case
            -- checking and replacing null values
            when totalchildren = 'NULL' then NULL
            -- converting data type to ensure correct parsing
            else cast(totalchildren as numeric(38, 0))
        end as totalchildren_updated,
        case
            -- checking and replacing null values
            when numberchildrenathome = 'NULL' then NULL
            -- converting data type to ensure correct parsing
            else cast(numberchildrenathome as numeric(38, 0))
        end as numberchildrenathome_updated,
        case
            -- checking and replacing null values
            when houseownerflag = 'NULL' then NULL
            -- converting data type to ensure correct parsing
            else cast(houseownerflag as numeric(38, 0))
        end as houseownerflag_updated,
        -- date
        case
            -- checking and replacing null values
            when numbercarsowned = 'NULL' then NULL
            -- converting data type to ensure correct parsing
            else cast(numbercarsowned as numeric(38, 0))
        end as numbercarsowned_updated,
        case
            -- checking and replacing null values
            when birthdate = 'NULL' or birthdate is NULL then NULL
            -- converting data type to ensure correct parsing
            else cast(birthdate as date)
        end as birthdate_updated,
        case
            when
                -- checking and replacing null values
                datefirstpurchase = 'NULL' or datefirstpurchase is NULL
                then NULL
            -- converting data type to ensure correct parsing
            else cast(datefirstpurchase as date)
        end as datefirstpurchase_updated,
        case
            -- checking and replacing null values
            when birthdate_updated is NULL then NULL
            -- creating derived metrics
            else datediff('year', birthdate_updated, getdate())
        end as customerage,

        -- creation timing
        case
            -- checking and replacing null values
            when datefirstpurchase_updated is NULL then NULL
            else
                datediff(
                    -- creating derived metrics
                    'day', datefirstpurchase_updated, getdate()
                )
        end as dayssincefirstpurchase

    from source
)

-- putting the transformed data into a table
select * from customer
