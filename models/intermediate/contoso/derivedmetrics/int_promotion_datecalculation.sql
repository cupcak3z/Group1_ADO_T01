with
promotion as (
    select * from {{ ref('stg_contoso__promotion') }}
),

promotion_derivedmetrics as (
    select
        -- ids
        PROMOTIONKEY_UPDATED,

        -- strings
        PROMOTIONNAME,
        PROMOTIONTYPE,
        PROMOTIONCATEGORY,

        -- numbers
        DISCOUNTPERCENT_UPDATED,
        datediff('day', STARTDATE_UPDATED, ENDDATE_UPDATED) as DAYSPROMOTIONDURATION,
        case
            when cast('2009-10-01' as date) < STARTDATE_UPDATED then 
                -datediff('day', cast('2009-10-01' as date), STARTDATE_UPDATED)
        else
            datediff('day', STARTDATE_UPDATED, cast('2009-10-01' as date))
        end as DAYSSINCEPROMOTIONSTART,

        -- dates
        STARTDATE_UPDATED,
        ENDDATE_UPDATED,

        -- creation timing
        CREATED_AT

    from promotion
)

select * from promotion_derivedmetrics