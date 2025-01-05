with 

aggregated_sales as (
    select * 
    from {{ ref('int_contoso_aggregated_sales') }}
),

lagged_features as (
    select
        *,
        lag(total_sales, 1) over (
            partition by STOREKEY_updated, PRODUCTKEY_updated 
            order by month
        ) as lag_sales,
        lag(total_quantity, 1) over (
            partition by STOREKEY_updated, PRODUCTKEY_updated 
            order by month
        ) as lag_quantity
    from aggregated_sales
)

select *
from lagged_features
order by PRODUCTKEY_updated, STOREKEY_updated
