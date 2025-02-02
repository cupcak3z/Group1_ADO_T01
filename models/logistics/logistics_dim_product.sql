-- get data from product staging model
select * from {{ ref('stg_dim_product') }}
