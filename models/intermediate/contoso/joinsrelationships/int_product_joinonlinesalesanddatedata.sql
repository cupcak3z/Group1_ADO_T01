with -- Load Sales Data
onlinesales as (
    select
        ONLINESALESKEY_updated as ONLINESALESKEY,
        DATEKEY_updated as DATEKEY,
        PRODUCTKEY_updated as PRODUCTKEY,
        NET_SALES_AMOUNT,
        TOTAL_PROFIT,
    from {{ ref('stg_contoso__onlinesales') }}
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
        CALENDARQUARTERLABEL,
        CALENDARMONTHLABEL
    from {{ ref('stg_contoso__date') }}
),

-- Join Online Sales with Product
onlinesales_with_product as (
    select
        s.ONLINESALESKEY,
        s.DATEKEY,
        s.NET_SALES_AMOUNT,
        s.TOTAL_PROFIT,
        p.PRODUCTCATEGORYNAME,
        p.PRODUCTSUBCATEGORYNAME,
    from onlinesales s
    left join product p
        on s.PRODUCTKEY = p.PRODUCTKEY
),

-- Join the Result with Date
onlinesales_with_product_date as (
    select
        spc.PRODUCTCATEGORYNAME,
        spc.PRODUCTSUBCATEGORYNAME,
        spc.NET_SALES_AMOUNT,
        spc.TOTAL_PROFIT,
        d.CALENDARYEAR,
        d.CALENDARQUARTERLABEL,
        d.CALENDARMONTHLABEL
    from onlinesales_with_product spc
    left join date d
        on spc.DATEKEY = d.DATEKEY
)
select * from onlinesales_with_product_date