-- get data from date staging model
select * from {{ ref('stg_dim_date') }}
