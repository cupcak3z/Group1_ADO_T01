-- get data from entity staging model
select * from {{ ref('stg_dim_entity') }}
