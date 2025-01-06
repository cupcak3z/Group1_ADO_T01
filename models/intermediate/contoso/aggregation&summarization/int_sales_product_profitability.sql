with salesprofitabilitydata as (
    select * from {{ ref('int_sales_joinproductgeography') }}
)

-- Aggregate profit by Product Category and Region, and rank the results
select
    PRODUCTNAME,
    CLASSNAME, 
    REGIONCOUNTRYNAME,
    round(UNITCOST_UPDATED, 2) as unit_cost,  -- Display unit cost for each product
    round(sum(SALESQUANTITY_UPDATED), 2) as total_sales_quantity,  -- Total sales quantity
    round(sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED), 2) as total_cost,  -- Total cost
    round(sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED), 2) as total_profit,  -- Total profit
    round((sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED)) / sum(SALESQUANTITY_UPDATED), 2) as avg_profit_per_unit,  -- Average profit per unit
    round(((sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED)) / sum(SALESAMOUNT_UPDATED)) * 100, 2) as profit_margin,  -- Profit margin
    round(((sum(SALESAMOUNT_UPDATED) - sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED)) / sum(UNITCOST_UPDATED * SALESQUANTITY_UPDATED)) * 100, 2) as roi  -- ROI
from salesprofitabilitydata
group by PRODUCTNAME, CLASSNAME, REGIONCOUNTRYNAME, UNITCOST_UPDATED

