with
salesterritory as (
    select * from {{ ref('stg_contoso__salesterritory') }}
),

salesterritory_derivedmetrics as (
    select
        -- ids
        SALESTERRITORYKEY_UPDATED,
        GEOGRAPHYKEY_UPDATED,
        SALESTERRITORYMANAGER_UPDATED,

        -- strings
        SALESTERRITORYNAME,
        SALESTERRITORYREGION,
        SALESTERRITORYCOUNTRY,
        SALESTERRITORYGROUP,
        STATUS,

        -- numbers
        SALESTERRITORYLEVEL_UPDATED,
        case
            when cast('2009-10-01' as date) < STARTDATE_UPDATED then 
                -datediff('day', cast('2009-10-01' as date), STARTDATE_UPDATED)
        else
            datediff('day', STARTDATE_UPDATED, cast('2009-10-01' as date))
        end as YEARSSINCESALESTERRITORYSTART,        

        -- dates
        STARTDATE_UPDATED,
        ENDDATE_UPDATED,

        -- creation timing
        CREATED_AT

    from salesterritory
)

select * from salesterritory_derivedmetrics