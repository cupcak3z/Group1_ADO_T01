-- get data from date staging models
select * from {{ ref('stg_dim_date') }}
