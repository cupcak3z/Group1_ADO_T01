with
salesproductgeography as (
    select * from {{ ref('int_sales_joinproductgeography') }}
)

-- Aggregate profit by Product Category and Region, and rank the results
select
    PRODUCTNAME,
    CLASSNAME, 
    REGIONCOUNTRYNAME,
    round(sum(SALESQUANTITY_UPDATED), 2) as total_sales_quantity,  -- Total sales quantity
    round(sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED), 2) as total_cost,  -- Total cost
    round(sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED), 2) as total_profit,  -- Total profit
    round(((sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED)) / sum(SALESAMOUNT_UPDATED)) * 100, 2) as profit_margin,  -- Profit margin
    round(((sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED)) / sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED)) * 100, 2) as roi,  -- ROI
    row_number() over (order by sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED) desc) as profit_rank  -- Row number (no gaps)
from salesproductgeography
group by PRODUCTNAME, CLASSNAME, REGIONCOUNTRYNAME













