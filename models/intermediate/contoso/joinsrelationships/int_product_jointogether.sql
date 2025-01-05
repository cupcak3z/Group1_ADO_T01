with
product as (
    select * from {{ ref('stg_contoso__product') }}
),
productcategory as (
    select * from {{ ref('stg_contoso__productcategory') }}
),
productsubcategory as (
    select * from {{ ref('stg_contoso__productsubcategory') }}
),
productinformation as (
    select
        p.*,
        c.PRODUCTCATEGORYNAME,
        s.PRODUCTSUBCATEGORYNAME
    from product p
    left join productsubcategory s on p.PRODUCTSUBCATEGORYKEY_updated = s.PRODUCTSUBCATEGORYKEY_updated
    left join productcategory c on s.PRODUCTCATEGORYKEY_updated = c.PRODUCTCATEGORYKEY_updated
)

select * from productinformation