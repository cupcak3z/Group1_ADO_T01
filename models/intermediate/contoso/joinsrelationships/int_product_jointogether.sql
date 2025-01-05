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
        products.*,
        category.PRODUCTCATEGORYNAME,
        subcategory.PRODUCTSUBCATEGORYNAME
    from product products
    left join productsubcategory subcategory on products.PRODUCTSUBCATEGORYKEY_updated = subcategory.PRODUCTSUBCATEGORYKEY_updated
    left join productcategory category on subcategory.PRODUCTCATEGORYKEY_updated = category.PRODUCTCATEGORYKEY_updated
)

select * from productinformation