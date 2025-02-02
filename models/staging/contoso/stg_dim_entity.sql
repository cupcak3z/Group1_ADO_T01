-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMENTITY_RAW') }}
),

entity as (
    select
        -- ids
        cast(entitykey as numeric(38, 0)) as entitykey_updated, -- converting data type to ensure correct parsing
        entityname,

        -- strings
        entitydescription,
        entitytype,
        status,
        cast(loaddate as timestamp_ntz) as created_at, -- converting data type to ensure correct parsing
        case
            when parententitykey = 'NULL' then cast(entitykey as numeric(38, 0)) -- checking and replacing null values, converting data type
            else cast(parententitykey as numeric(38, 0))
        end as parententitykey_updated,

        -- creation timing
        case
            when parententitykey_updated = 1 then 'North America' -- convert values for easier understanding
            when parententitykey_updated = 2 then 'Europe' -- convert values for easier understanding
            when parententitykey_updated = 3 then 'Asia' -- convert values for easier understanding
            else parententitylabel
        end as parententitylabel_updated

    from source
)

-- putting the transformed data into a table
select * from entity
