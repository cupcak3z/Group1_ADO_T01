-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCT_RAW') }}
),

product as (
    select
        -- ids
        cast(productkey as numeric(38, 0)) as productkey_updated, -- converting data type to ensure correct parsing
        cast(productsubcategorykey as numeric(38, 0)) -- converting data type to ensure correct parsing
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
        cast(classid as numeric(38, 0)) as classid_updated, -- converting data type to ensure correct parsing

        -- numbers
        cast(colorid as numeric(38, 0)) as colorid_updated, -- converting data type to ensure correct parsing
        cast(stocktypeid as numeric(38, 0)) as stocktypeid_updated, -- converting data type to ensure correct parsing
        cast(unitcost as numeric(10, 2)) as unitcost_updated, -- converting data type to ensure correct parsing
        cast(unitprice as numeric(10, 2)) as unitprice_updated, -- converting data type to ensure correct parsing
        cast(
            (
                (unitprice_updated - unitcost_updated) / unitcost_updated -- creating derived metrics
            ) as numeric(3, 2) -- converting data type to ensure correct parsing
        ) as markup,
        cast(loaddate as timestamp_ntz) as created_at, -- converting data type to ensure correct parsing
        case
            when status = 'NULL' then 'Off' -- checking and replacing null values
            else status
        end as status_updated,

        -- additional
        case
            when styleid = 'NULL' then 1 -- checking and replacing null values, converting data type
            else cast(styleid as numeric(38, 0))
        end as styleid_updated,
        case
            when weight = 'NULL' then 0 -- checking and replacing null values, converting data type
            else cast(weight as numeric(10, 2))
        end as weight_updated,
        case
            when
                availableforsaledate = 'NULL' -- checking and replacing null values, converting data type
                then cast('2005-05-03T00:00:00' as timestamp_ntz)
            else cast(availableforsaledate as timestamp_ntz)
        end as availableforsaledate_updated,

        -- creation timing
        case
            when getdate() < availableforsaledate_updated
                then
                    -datediff(
                        'day',
                        getdate(),
                        availableforsaledate_updated
                    ) 
            else
                datediff(
                    'day',
                    availableforsaledate_updated,
                    getdate()
                )
        end as dayssinceavailableforsale -- creating derived metrics

    from source
)

-- putting the transformed data into a table
select * from product
