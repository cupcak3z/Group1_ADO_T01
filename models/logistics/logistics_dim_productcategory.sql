-- get data from product category staging model
select * from {{ ref('stg_dim_productcategory') }}
