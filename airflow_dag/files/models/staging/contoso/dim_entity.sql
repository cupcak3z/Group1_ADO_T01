with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMENTITY_RAW') }}
),

entity as (
    select
        -- ids
        cast(entitykey as numeric(38, 0)) as entitykey_updated,
        entityname,

        -- strings
        entitydescription,
        entitytype,
        status,
        cast(loaddate as timestamp_ntz) as created_at,
        case
            when parententitykey = 'NULL' then cast(entitykey as numeric(38, 0))
            else cast(parententitykey as numeric(38, 0))
        end as parententitykey_updated,

        -- creation timing
        case
            when parententitykey_updated = 1 then 'North America'
            when parententitykey_updated = 2 then 'Europe'
            when parententitykey_updated = 3 then 'Asia'
            else parententitylabel
        end as parententitylabel_updated

    from source
)

select * from entity
