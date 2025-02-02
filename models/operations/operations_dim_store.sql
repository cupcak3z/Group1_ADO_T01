-- get data from store staging model
select * from {{ ref('stg_dim_store') }}
