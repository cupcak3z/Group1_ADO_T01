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
        WEIGHTUNITMEASUREID,
        UNITOFMEASUREID,
        UNITOFMEASURENAME,
        STOCKTYPENAME,
        case 
            when STATUS = 'NULL' THEN 'Off'
            else STATUS
        end as STATUS_updated,
        
        -- numbers
        cast(CLASSID as numeric(38,0)) as CLASSID_updated,
        case
            when STYLEID = 'NULL' THEN 1
            else cast(STYLEID as numeric(38,0))       
        end as STYLEID_updated,
        cast(COLORID as numeric(38,0)) as COLORID_updated,
        case
            when WEIGHT = 'NULL' THEN 0
            else cast(WEIGHT as numeric(10, 2)) 
        end as WEIGHT_updated,
        cast(STOCKTYPEID as numeric(38,0)) as STOCKTYPEID_updated,
        cast(UNITCOST as numeric(10, 2)) as UNITCOST_updated,
        cast(UNITPRICE as numeric(10, 2)) as UNITPRICE_updated,

        -- dates
        case
            when AVAILABLEFORSALEDATE = 'NULL' then cast('2005-05-03T00:00:00' as timestamp_ntz)
            else cast(AVAILABLEFORSALEDATE as timestamp_ntz)
        end as AVAILABLEFORSALEDATE_updated,
        
        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from product