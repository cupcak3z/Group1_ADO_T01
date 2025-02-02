-- get data from sales territory staging models
select * from {{ ref('stg_dim_salesterritory') }}
