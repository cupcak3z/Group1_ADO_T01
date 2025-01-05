with 
lagged_features as (
    select * 
    from {{ ref('int_contoso_lagged_features') }}  
),

moving_averages as (
    select
        *,
        avg(total_sales) over (
            partition by STOREKEY_updated, PRODUCTKEY_updated 
            order by month 
            rows between 3 preceding and current row
        ) as moving_avg_sales_3m, 
        avg(total_quantity) over (
            partition by STOREKEY_updated, PRODUCTKEY_updated 
            order by month 
            rows between 3 preceding and current row
        ) as moving_avg_quantity_3m 
    from lagged_features
)

select * 
from moving_averages
order by PRODUCTKEY_updated, STOREKEY_updated 