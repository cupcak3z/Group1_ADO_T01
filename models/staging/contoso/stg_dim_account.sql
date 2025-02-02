-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMACCOUNT_RAW') }}
),

account as (
    select
        -- ids
        cast(accountkey as numeric(38, 0)) as accountkey_updated,-- converting data type to ensure correct parsing
        accountname,

        -- strings
        accountdescription,
        accounttype,
        valuetype,
        cast(loaddate as timestamp_ntz) as created_at,-- converting data type to ensure correct parsing
        case
            when parentaccountkey = 'NULL' then '1' -- checking and replacing null values
            else cast(parentaccountkey as numeric(38, 0))
        end as parentaccountkey_updated,

        case
            when accounttype = 'NULL' then 'Income'-- checking and replacing null values
            else accounttype
        end as accounttype_updated,

        -- creation timing
        case -- checking and replacing null values
            when valuetype = 'NULL' then 'Income'
            else valuetype
        end as valuetype_updated
    from source
)

-- putting the transformed data into a table
select * from account
