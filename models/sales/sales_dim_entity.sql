-- get data from entity staging models
select * from {{ ref('stg_dim_entity') }}
