with 

sales as (
    select * from {{ ref('stg_contoso_sales') }}  
),

aggregated_sales as (
    select
        PRODUCTKEY_updated,
        STOREKEY_updated,        
        date_trunc('month', created_at) as month,
        NET_SALES_AMOUNT,
        TOTAL_PROFIT,
        sum(SALESAMOUNT_updated) as total_sales,
        avg(SALESAMOUNT_updated) as avg_sales,
        sum(SALESQUANTITY_updated) as total_quantity,
        avg(SALESQUANTITY_updated) as avg_quantity
    from sales
    group by 1, 2, 3, 4, 5
)

select *
from aggregated_sales
order by PRODUCTKEY_updated, STOREKEY_updated
