-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCT_RAW') }}
),

product as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(productkey as numeric(38, 0)) as productkey_updated,
        -- converting data type to ensure correct parsing
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
        -- converting data type to ensure correct parsing
        cast(classid as numeric(38, 0)) as classid_updated,

        -- numbers
        -- converting data type to ensure correct parsing
        cast(colorid as numeric(38, 0)) as colorid_updated,
        -- converting data type to ensure correct parsing
        cast(stocktypeid as numeric(38, 0)) as stocktypeid_updated,
        -- converting data type to ensure correct parsing
        cast(unitcost as numeric(10, 2)) as unitcost_updated,
        -- converting data type to ensure correct parsing
        cast(unitprice as numeric(10, 2)) as unitprice_updated,
        cast(
            (
                -- creating derived metrics
                (unitprice_updated - unitcost_updated) / unitcost_updated
            ) as numeric(3, 2) -- converting data type to ensure correct parsing
        ) as markup,
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at,
        case
            -- checking and replacing null values
            when status = 'NULL' then 'Off'
            else status
        end as status_updated,

        -- additional
        case
            -- checking and replacing null values, converting data type
            when styleid = 'NULL' then 1
            else cast(styleid as numeric(38, 0))
        end as styleid_updated,
        case
            -- checking and replacing null values, converting data type
            when weight = 'NULL' then 0
            else cast(weight as numeric(10, 2))
        end as weight_updated,
        case
            when
                -- checking and replacing null values, converting data type
                availableforsaledate = 'NULL'
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
