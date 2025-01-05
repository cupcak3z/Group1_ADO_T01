with
salesproductgeography as (
    select * from {{ ref('int_sales_joinproductgeography') }}
)

-- Aggregate profit by Product Category and Region, and rank the results
select
    PRODUCTNAME,
    CLASSNAME, 
    REGIONCOUNTRYNAME,
    round(sum(SALESQUANTITY_UPDATED), 2) as total_sales_quantity,
    round(sum(SALESAMOUNT_UPDATED - UNITCOST_UPDATED), 2) as total_profit,  -- Calculating Profit
    rank() over (order by sum(SALESAMOUNT_UPDATED - UNITCOST_UPDATED) desc) as profit_rank  -- Ranking by Profit
from salesproductgeography
group by PRODUCTNAME, CLASSNAME, REGIONCOUNTRYNAME

