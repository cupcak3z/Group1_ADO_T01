with 
sales as (
    select * 
    from {{ ref('stg_contoso__sales') }}
),

aggregated_sales as (
    select
        date_trunc('month', created_at) as month,
        sum(SALESAMOUNT_updated) as total_sales,
        avg(SALESAMOUNT_updated) as avg_sales,
        sum(SALESQUANTITY_updated) as total_quantity,
        avg(SALESQUANTITY_updated) as avg_quantity,
        sum(NET_SALES_AMOUNT) as total_net_sales,
        sum(TOTAL_PROFIT) as total_profit
    from sales
    group by 
        date_trunc('month', created_at)
)

select *
from aggregated_sales
order by month
