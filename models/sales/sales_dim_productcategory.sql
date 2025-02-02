-- get data from product category staging models
select * from {{ ref('stg_dim_productcategory') }}
