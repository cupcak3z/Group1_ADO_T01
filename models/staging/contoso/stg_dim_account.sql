-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMACCOUNT_RAW') }}
),
----TESTING SQL LINTER
account as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(accountkey as numeric(38, 0)) as accountkey_updated,
        accountname,

        -- strings
        accountdescription,
        accounttype,
        valuetype,
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,
        case
            -- checking and replacing null values
            when parentaccountkey = 'NULL' then '1'
            else cast(parentaccountkey as numeric(38, 0))
        end as parentaccountkey_updated,

        case
            -- checking and replacing null values
            when accounttype = 'NULL' then 'Income'
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
