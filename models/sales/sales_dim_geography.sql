-- get data from geography staging models
select * from {{ ref('stg_dim_geography') }}
