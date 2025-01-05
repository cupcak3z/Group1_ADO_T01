with
salesproductgeography as (
    select * from {{ ref('int_sales_joinproductgeography') }}
)

-- Aggregate profit by Product Category and Region, and rank the results
select
    CLASSNAME, 
    REGIONCOUNTRYNAME,
    round(sum(SALESAMOUNT_UPDATED - PRODUCTCOST_UPDATED), 2) as total_profit,  -- Calculating Profit
    round(sum(SALESQUANTITY_UPDATED), 2) as total_sales_quantity,
    rank() over (order by sum(SALESAMOUNT_UPDATED - PRODUCTCOST_UPDATED) desc) as profit_rank  -- Ranking by Profit
from salesproductgeography
group by CLASSNAME, REGIONCOUNTRYNAME
