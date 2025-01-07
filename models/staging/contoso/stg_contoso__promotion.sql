with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPROMOTION_RAW')}}
),

promotion as (
    select
        -- ids
        cast(PROMOTIONKEY as numeric(38,0)) as PROMOTIONKEY_updated,

        -- strings
        PROMOTIONNAME,
        PROMOTIONTYPE,
        PROMOTIONCATEGORY, 

        -- numbers
        cast(DISCOUNTPERCENT as numeric(3,2)) as DISCOUNTPERCENT_updated,

        -- dates
        cast(STARTDATE as date) as STARTDATE_updated,
        cast(ENDDATE as date) as ENDDATE_updated,

        -- additional
        datediff('day', STARTDATE_updated, ENDDATE_updated) as DAYSPROMOTIONDURATION,
        case
            when cast('2009-10-01' as date) < STARTDATE_updated then 
                -datediff('day', cast('2009-10-01' as date), STARTDATE_updated)
        else
            datediff('day', STARTDATE_updated, cast('2009-10-01' as date))
        end as DAYSSINCEPROMOTIONSTART,
        
        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from promotion