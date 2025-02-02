-- get data from promotion staging models
select * from {{ ref('stg_dim_promotion') }}
