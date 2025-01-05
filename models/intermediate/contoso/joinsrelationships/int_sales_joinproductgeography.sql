with
-- FactSales table (sales data)
sales as (
    select * from {{ ref('stg_contoso__sales') }}  -- Sales data from FactSales
),
-- DimProduct table (product details)
product as (
    select * from {{ ref('stg_contoso__product') }}  -- Product data from DimProduct
),
-- DimGeography table (geography details)
geography as (
    select * from {{ ref('stg_contoso__geography') }}  -- Geography data from DimGeography
),

-- Join sales, product, and geography data
int_sales_product_profitability as (
    select
        p.PRODUCTNAME,  -- Product name from DimProduct
        p.CLASSNAME,  -- Product class from DimProduct
        g.REGIONCOUNTRYNAME,  -- Region from DimGeography
        s.SALESAMOUNT_UPDATED,  -- Sales amount from FactSales
        s.SALESQUANTITY_UPDATED  -- Sales quantity from FactSales
    from sales s
    left join product p on s.PRODUCTKEY_UPDATED = p.PRODUCTKEY_UPDATED  -- Join to get product data
    left join geography g on s.STOREKEY_UPDATED = g.GEOGRAPHYKEY_UPDATED  -- Join to get geography data
)

select * from int_sales_product_profitability



