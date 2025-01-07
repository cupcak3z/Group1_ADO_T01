with

online_sales as (

    select * from {{ ref('stg_contoso__onlinesales') }}

),

customers as (

<<<<<<< HEAD
    select FULLNAME from {{ ref('int_customer_fullname_datecalculation') }}
=======
    select * from {{ ref('stg_contoso__onlinesales') }}
>>>>>>> 676447eecd7411a19acc56f94074c5f1bb5866f9

),

online_sales_with_customer as (

    select
        os.*,
        c.FULLNAME
    from online_sales os
    left join customers c on os.CUSTOMERKEY_UPDATED = c.CUSTOMERKEY_UPDATED

)

select * from online_sales_with_customer