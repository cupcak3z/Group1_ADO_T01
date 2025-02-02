-- get data from channel staging models
select * from {{ ref('stg_dim_channel') }}
