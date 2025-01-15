with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCT_RAW')}}
),

product as (
    select
        -- ids
        cast(PRODUCTKEY as numeric(38,0)) as PRODUCTKEY_updated,
        cast(PRODUCTSUBCATEGORYKEY as numeric(38,0)) as PRODUCTSUBCATEGORYKEY_updated,

        -- strings
        PRODUCTNAME,
        PRODUCTDESCRIPTION,
        MANUFACTURER,
        BRANDNAME,
        CLASSNAME,
        STYLENAME,
        COLORNAME,
        WEIGHTUNITMEASUREID as WEIGHTUNITMEASURENAME,
        UNITOFMEASUREID,
        UNITOFMEASURENAME as LENGTHUNITMEASURENAME,
        STOCKTYPENAME,
        case 
            when STATUS = 'NULL' then 'Off'
            else STATUS
        end as STATUS_updated,
        
        -- numbers
        cast(CLASSID as numeric(38,0)) as CLASSID_updated,
        case
            when STYLEID = 'NULL' then 1
            else cast(STYLEID as numeric(38,0))       
        end as STYLEID_updated,
        cast(COLORID as numeric(38,0)) as COLORID_updated,
        case
            when WEIGHT = 'NULL' then 0
            else cast(WEIGHT as numeric(10, 2)) 
        end as WEIGHT_updated,
        cast(STOCKTYPEID as numeric(38,0)) as STOCKTYPEID_updated,
        cast(UNITCOST as numeric(10, 2)) as UNITCOST_updated,
        cast(UNITPRICE as numeric(10, 2)) as UNITPRICE_updated,

        -- additional
        case
            when AVAILABLEFORSALEDATE = 'NULL' then cast('2005-05-03T00:00:00' as timestamp_ntz)
            else cast(AVAILABLEFORSALEDATE as timestamp_ntz)
        end as AVAILABLEFORSALEDATE_updated,
        cast(((UNITPRICE_updated - UNITCOST_updated) / UNITCOST_updated) as numeric(3,2)) AS MARKUP,
        case
            when cast('2009-12-31' as date) < AVAILABLEFORSALEDATE_updated then 
                -datediff('day', cast('2009-12-31' as date), AVAILABLEFORSALEDATE_updated)
        else
            datediff('day', AVAILABLEFORSALEDATE_updated, cast('2009-12-31' as date))
        end as DAYSSINCEAVAILABLEFORSALE,
        
        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from product