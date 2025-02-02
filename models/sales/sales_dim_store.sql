-- get data from store staging models
select * from {{ ref('stg_dim_store') }}
