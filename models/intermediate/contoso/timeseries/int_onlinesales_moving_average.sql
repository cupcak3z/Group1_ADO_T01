with 
sales as (
    select * 
    from {{ ref('stg_contoso__onlinesales') }}
),

aggregated_sales as (
    select
        extract(year from created_at) as year_number,
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
        sum(SALESQUANTITY_updated) as total_quantity,
        sum(NET_SALES_AMOUNT) as total_net_sales,
        sum(TOTAL_PROFIT) as total_profit
    from sales
    group by 
        extract(year from created_at),
        extract(month from created_at)
),

moving_averages as (
    select
        *,
        -- Moving average of sales over 2 months (including current and previous)
        avg(total_sales) over (
            order by year_number, month_number
            rows between 1 preceding and current row
        ) as moving_avg_sales_2m,
        avg(total_quantity) over (
            order by year_number, month_number
            rows between 1 preceding and current row
        ) as moving_avg_quantity_2m,
        avg(total_net_sales) over (
            order by year_number, month_number
            rows between 1 preceding and current row
        ) as moving_avg_net_sales_2m,
        avg(total_profit) over (
            order by year_number, month_number
            rows between 1 preceding and current row
        ) as moving_avg_profit_2m
    from aggregated_sales
)

select 
    year_number,
    month_name,
    total_sales,
    total_quantity,
    total_net_sales,
    total_profit,
    moving_avg_sales_2m,
    moving_avg_quantity_2m,
    moving_avg_net_sales_2m,
    moving_avg_profit_2m
from moving_averages
order by year_number, month_number
