with
salesdata as (
    select
        SALESKEY_updated,

    from {{ ref('stg_contoso__sales') }}
),

datedata as (
    select
        DATEKEY_updated,
        
),

geographydata as (
    select
        d
    from {{ ref('stg_contoso__geography') }}
),

storedata as (
    select
        d
    from {{ ref('stg_contoso__store') }}
),

productdata as (
    select
        d
    from {{ ref('int_product_jointogether') }}
),

customerdata as (
    select
        d
    from {{ ref('stg_contoso__customer')}}
),

promotiondata as (
    select
        d
    from {{ ref('stg_contoso__promotion')}}
),

channeldata as (
    select
        d
    from {{ ref('stg_contoso__channel') }}
),

currencydata as (
    select
        d
    from {{ ref('stg_contoso__currency') }}
),

completedsalesdata as (
    select
        d
    from 
)