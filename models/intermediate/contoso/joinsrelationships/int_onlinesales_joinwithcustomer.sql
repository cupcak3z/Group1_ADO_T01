with

online_sales as (

    select * from {{ ref('stg_contoso__onlinesales') }}

),

customers as (

    select FULLNAME from {{ ref('stg_contoso__customer') }}

),

online_sales_with_customer as (

    select
        os.*,
        c.FULLNAME
    from online_sales os
    left join customers c on os.CUSTOMERKEY_UPDATED = c.CUSTOMERKEY_UPDATED

)

select * from online_sales_with_customer