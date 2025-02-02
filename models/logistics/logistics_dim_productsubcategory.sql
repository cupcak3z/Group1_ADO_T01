-- get data from product subcategory staging model
select * from {{ ref('stg_dim_productsubcategory') }}
