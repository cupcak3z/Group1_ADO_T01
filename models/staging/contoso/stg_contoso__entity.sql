with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMENTITY_RAW')}}
),

entity as (
    select
        -- ids
        cast(ENTITYKEY as numeric(38,0)) as ENTITYKEY_updated,
        case
            when PARENTENTITYKEY = 'NULL' then cast(ENTITYKEY as numeric(38,0))
            else cast(PARENTENTITYKEY as numeric(38,0))
        end as PARENTENTITYKEY_updated,

        -- strings
        case
            when PARENTENTITYKEY_updated = 1 then 'North America'
            when PARENTENTITYKEY_updated = 2 then 'Europe'
            when PARENTENTITYKEY_updated = 3 then 'Asia'
            else PARENTENTITYLABEL
        end as PARENTENTITYLABEL_updated,
        ENTITYNAME,
        ENTITYDESCRIPTION,
        ENTITYTYPE,
        STATUS,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from entity