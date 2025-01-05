with

sales as (
    select * from {{ ref('stg_contoso__sales') }}
),

product as (
    select * from {{ ref('stg_contoso__product') }}
),

geography as (
    select * from {{ ref('stg_contoso__geography') }}
),

-- Join sales, product, and geography data
int_sales_product_profitability as (
    select
        p.PRODUCTNAME, 
        p.CLASSNAME, 
        g.REGIONCOUNTRYNAME,
        s.SALESAMOUNT_UPDATED, 
        p.UNITCOST_updated,
        s.SALESQUANTITY_UPDATED 
    from sales s
    left join product p on s.PRODUCTKEY_UPDATED = p.PRODUCTKEY_UPDATED 
    left join geography g on s.STOREKEY_UPDATED = g.GEOGRAPHYKEY_UPDATED
)

select * from int_sales_product_profitability


