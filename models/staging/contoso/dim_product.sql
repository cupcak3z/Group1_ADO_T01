with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCT_RAW') }}
),

product as (
    select
        -- ids
        cast(productkey as numeric(38, 0)) as productkey_updated,
        cast(productsubcategorykey as numeric(38, 0))
            as productsubcategorykey_updated,

        -- strings
        productname,
        productdescription,
        manufacturer,
        brandname,
        classname,
        stylename,
        colorname,
        weightunitmeasureid as weightunitmeasurename,
        unitofmeasureid,
        unitofmeasurename as lengthunitmeasurename,
        stocktypename,
        cast(classid as numeric(38, 0)) as classid_updated,

        -- numbers
        cast(colorid as numeric(38, 0)) as colorid_updated,
        cast(stocktypeid as numeric(38, 0)) as stocktypeid_updated,
        cast(unitcost as numeric(10, 2)) as unitcost_updated,
        cast(unitprice as numeric(10, 2)) as unitprice_updated,
        cast(
            (
                (unitprice_updated - unitcost_updated) / unitcost_updated
            ) as numeric(3, 2)
        ) as markup,
        cast(loaddate as timestamp_ntz) as created_at,
        case
            when status = 'NULL' then 'Off'
            else status
        end as status_updated,

        -- additional
        case
            when styleid = 'NULL' then 1
            else cast(styleid as numeric(38, 0))
        end as styleid_updated,
        case
            when weight = 'NULL' then 0
            else cast(weight as numeric(10, 2))
        end as weight_updated,
        case
            when
                availableforsaledate = 'NULL'
                then cast('2005-05-03T00:00:00' as timestamp_ntz)
            else cast(availableforsaledate as timestamp_ntz)
        end as availableforsaledate_updated,

        -- creation timing
        case
            when cast('2009-12-31' as date) < availableforsaledate_updated
                then
                    -datediff(
                        'day',
                        cast('2009-12-31' as date),
                        availableforsaledate_updated
                    )
            else
                datediff(
                    'day',
                    availableforsaledate_updated,
                    cast('2009-12-31' as date)
                )
        end as dayssinceavailableforsale

    from source
)

select * from product
