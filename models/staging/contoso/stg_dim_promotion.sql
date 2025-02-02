-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPROMOTION_RAW') }}
),

promotion as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(promotionkey as numeric(38, 0)) as promotionkey_updated,

        -- strings
        promotionname,
        promotiontype,
        promotioncategory,

        -- numbers
        -- converting data type to ensure correct parsing
        cast(discountpercent as numeric(3, 2)) as discountpercent_updated,

        -- dates
        -- converting data type to ensure correct parsing
        cast(startdate as date) as startdate_updated,
        -- converting data type to ensure correct parsing
        cast(enddate as date) as enddate_updated,

        -- additional
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,
        -- creating derived metrics
        datediff('day', startdate_updated, enddate_updated)
            as dayspromotionduration,

        -- creation timing
        case
            when getdate() < startdate_updated
                then
                    -datediff(
                        'day', getdate(), startdate_updated
                    )
            else
                datediff('day', startdate_updated, getdate())
        end as dayssincepromotionstart -- creating derived metrics

    from source
)

-- putting the transformed data into a table
select * from promotion
