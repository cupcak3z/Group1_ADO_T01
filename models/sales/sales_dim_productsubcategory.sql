-- get data from product subcategory staging models
select * from {{ ref('stg_dim_productsubcategory') }}
