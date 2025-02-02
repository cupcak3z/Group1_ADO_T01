-- get data from product staging models
select * from {{ ref('stg_dim_product') }}
