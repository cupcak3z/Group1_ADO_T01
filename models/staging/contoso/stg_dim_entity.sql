-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMENTITY_RAW') }}
),

entity as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(entitykey as numeric(38, 0)) as entitykey_updated,
        entityname,

        -- strings
        entitydescription,
        entitytype,
        status,
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,
        case
            -- checking and replacing null values, converting data type
            when parententitykey = 'NULL' then cast(entitykey as numeric(38, 0))
            else cast(parententitykey as numeric(38, 0))
        end as parententitykey_updated,

        -- creation timing
        case
            -- convert values for easier understanding
            when parententitykey_updated = 1 then 'North America'
            -- convert values for easier understanding
            when parententitykey_updated = 2 then 'Europe'
            -- convert values for easier understanding
            when parententitykey_updated = 3 then 'Asia'
            else parententitylabel
        end as parententitylabel_updated

    from source
)

-- putting the transformed data into a table
select * from entity
