--ProductSubCategory Onlinesales Performance Analysis

with
onlinesalesproductsubcategorydata as (
    select * from {{ ref('int_product_joinonlinesalesanddatedata') }}  -- Referencing the join model
),

productsubcategoryonlinesales as (
-- Aggregate Onlinesales by ProductSubCategory
    select
        PRODUCTSUBCATEGORYNAME,
        PRODUCTCATEGORYNAME, 
        round(sum(NET_SALES_AMOUNT), 2) as total_net_sales,
        round(sum(TOTAL_PROFIT), 2) as total_profit
    from onlinesalesproductsubcategorydata
    group by PRODUCTSUBCATEGORYNAME, PRODUCTCATEGORYNAME
    order by total_net_sales desc
)

select * from productsubcategoryonlinesales