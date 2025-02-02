-- get data from sales territory staging model
select * from {{ ref('stg_dim_salesterritory') }}
