with 
sales as (
    select * 
    from {{ ref('stg_contoso__sales') }}
),

aggregated_sales as (
    select
        extract(month from created_at) as month_number, 
        CASE 
            WHEN extract(month from created_at) = 1 THEN 'January'
            WHEN extract(month from created_at) = 2 THEN 'February'
            WHEN extract(month from created_at) = 3 THEN 'March'
            WHEN extract(month from created_at) = 4 THEN 'April'
            WHEN extract(month from created_at) = 5 THEN 'May'
            WHEN extract(month from created_at) = 6 THEN 'June'
            WHEN extract(month from created_at) = 7 THEN 'July'
            WHEN extract(month from created_at) = 8 THEN 'August'
            WHEN extract(month from created_at) = 9 THEN 'September'
            WHEN extract(month from created_at) = 10 THEN 'October'
            WHEN extract(month from created_at) = 11 THEN 'November'
            WHEN extract(month from created_at) = 12 THEN 'December'
        END as month_name,
        sum(SALESAMOUNT_updated) as total_sales,
        avg(SALESAMOUNT_updated) as avg_sales,
        sum(SALESQUANTITY_updated) as total_quantity,
        avg(SALESQUANTITY_updated) as avg_quantity,
        sum(NET_SALES_AMOUNT) as total_net_sales,
        sum(TOTAL_PROFIT) as total_profit
    from sales
    group by 
        extract(month from created_at)
),

lagged_features as (
    select
        *,
        lag(total_sales) over (
            order by month_number
        ) as lag_total_sales,
        lag(total_quantity) over (
            order by month_number
        ) as lag_total_quantity,
        lag(total_profit) over (
            order by month_number
        ) as lag_total_profit,
        lag(avg_sales) over (
            order by month_number
        ) as lag_avg_sales,
        lag(avg_quantity) over (
            order by month_number
        ) as lag_avg_quantity
    from aggregated_sales
)

select 
    month_name,
    total_sales,
    avg_sales,
    total_quantity,
    avg_quantity,
    total_net_sales,
    total_profit,
    lag_total_sales,
    lag_total_quantity,
    lag_total_profit,
    lag_avg_sales,
    lag_avg_quantity
from lagged_features
order by month_number
