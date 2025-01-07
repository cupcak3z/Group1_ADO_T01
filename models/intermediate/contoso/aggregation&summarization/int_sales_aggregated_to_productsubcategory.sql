--ProductSubCategory Sales Performance Analysis

with
salesproductsubcategorydata as (
    select * from {{ ref('int_product_joinsalesanddatedata') }}  -- Referencing the join model
),

productsubcategorysales as (
-- Aggregate sales by ProductSubCategory
    select
        PRODUCTSUBCATEGORYNAME,
        PRODUCTCATEGORYNAME,  
        round(sum(NET_SALES_AMOUNT), 2) as total_net_sales,
        round(sum(TOTAL_PROFIT), 2) as total_profit
    from salesproductsubcategorydata
    group by PRODUCTSUBCATEGORYNAME, PRODUCTCATEGORYNAME
    order by total_net_sales desc
)

select * from productsubcategorysales