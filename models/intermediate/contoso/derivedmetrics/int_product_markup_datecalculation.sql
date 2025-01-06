with
product as (
    select * from {{ ref('stg_contoso__product') }}
),

product_derivedmetrics as (
    select
        -- ids
        PRODUCTKEY_UPDATED,
        PRODUCTSUBCATEGORYKEY_UPDATED,

        -- strings
        PRODUCTNAME,
        PRODUCTDESCRIPTION,
        MANUFACTURER,
        BRANDNAME,
        CLASSNAME,
        STYLENAME,
        COLORNAME,
        WEIGHTUNITMEASURENAME,
        LENGTHUNITMEASURENAME,
        STOCKTYPENAME,
        STATUS_UPDATED,

        -- numbers
        WEIGHT_UPDATED,
        UNITCOST_UPDATED,
        UNITPRICE_UPDATED,
        cast(((UNITPRICE_UPDATED - UNITCOST_UPDATED) / UNITCOST_UPDATED) as numeric(3,2)) AS MARKUP,
        case
            when cast('2009-10-01' as date) < AVAILABLEFORSALEDATE_UPDATED then 
                -datediff('day', cast('2009-10-01' as date), AVAILABLEFORSALEDATE_UPDATED)
        else
            datediff('day', AVAILABLEFORSALEDATE_UPDATED, cast('2009-10-01' as date))
        end as DAYSSINCEAVAILABLEFORSALE,
        
        -- creation timing
        CREATED_AT

    from product
)

select * from product_derivedmetrics