-- get data from geography staging model
select * from {{ ref('stg_dim_geography') }}
