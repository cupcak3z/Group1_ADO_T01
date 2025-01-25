-- this statement is to test the SQL linter
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMACCOUNT_RAW') }}
),

account as (
    select
        -- ids
        cast(accountkey as numeric(38, 0)) as accountkey_updated,
        accountname,

        -- strings
        accountdescription,
        accounttype,
        valuetype,
        cast(loaddate as timestamp_ntz) as created_at,
        case
            when parentaccountkey = 'NULL' then '1'
            else cast(parentaccountkey as numeric(38, 0))
        end as parentaccountkey_updated,

        case
            when accounttype = 'NULL' then 'Income'
            else accounttype
        end as accounttype_updated,

        -- creation timing
        case
            when valuetype = 'NULL' then 'Income'
            else valuetype
        end as valuetype_updated
    from source
)

select * from account
