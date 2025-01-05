with -- Load Sales Data
sales as (
    select
        SALESKEY_updated as SALESKEY,
        DATEKEY_updated as DATEKEY,
        PRODUCTKEY_updated as PRODUCTKEY,
        NET_SALES_AMOUNT,
        TOTAL_PROFIT,
    from {{ ref('stg_contoso__sales') }}
),

-- Load Product Data
product as (
    select
        PRODUCTKEY_updated as PRODUCTKEY,
        PRODUCTSUBCATEGORYKEY_updated as PRODUCTSUBCATEGORYKEY,
        PRODUCTCATEGORYNAME,
        PRODUCTSUBCATEGORYNAME,
    from {{ ref('int_product_jointogether') }}
),

-- Load Date Data
date as (
    select
        DATEKEY_updated as DATEKEY,
        CALENDARYEAR_UPDATED as CALENDARYEAR,
    from {{ ref('stg_contoso__date') }}
),

-- Join Online Sales with Product
sales_with_product as (
    select
        s.SALESKEY,
        s.DATEKEY,
        s.NET_SALES_AMOUNT,
        s.TOTAL_PROFIT,
        p.PRODUCTCATEGORYNAME,
        p.PRODUCTSUBCATEGORYNAME,
    from sales s
    left join product p
        on s.PRODUCTKEY = p.PRODUCTKEY
),

-- Join the Result with Date
sales_with_product_date as (
    select
        spc.PRODUCTCATEGORYNAME,
        spc.PRODUCTSUBCATEGORYNAME,
        spc.NET_SALES_AMOUNT,
        spc.TOTAL_PROFIT,
        d.CALENDARYEAR,
    from sales_with_product spc
    left join date d
        on spc.DATEKEY = d.DATEKEY
)
select * from sales_with_product_date