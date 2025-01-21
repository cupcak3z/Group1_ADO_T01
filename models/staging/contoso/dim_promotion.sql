with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPROMOTION_RAW') }}
),

promotion as (
    select
        -- ids
        cast(promotionkey as numeric(38, 0)) as promotionkey_updated,

        -- strings
        promotionname,
        promotiontype,
        promotioncategory,

        -- numbers
        cast(discountpercent as numeric(3, 2)) as discountpercent_updated,

        -- dates
        cast(startdate as date) as startdate_updated,
        cast(enddate as date) as enddate_updated,

        -- additional
        datediff('day', startdate_updated, enddate_updated)
            as dayspromotionduration,
        case
            when cast('2009-12-31' as date) < startdate_updated
                then
                    -datediff(
                        'day', cast('2009-12-31' as date), startdate_updated
                    )
            else
                datediff('day', startdate_updated, cast('2009-12-31' as date))
        end as dayssincepromotionstart,

        -- creation timing
        cast (loaddate as timestamp_ntz) as created_at

    from source
)

select * from promotion
