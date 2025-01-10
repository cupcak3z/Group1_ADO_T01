with

-- Inventory Data
inventorydata as (
    select
        INVENTORYKEY_UPDATED,
        STOREKEY_UPDATED,
        CURRENCYKEY_UPDATED,
        DATEKEY_UPDATED,
        ONHANDQUANTITY_UPDATED,
        ONORDERQUANTITY_UPDATED,
        UNITCOST_UPDATED,
        SAFETYSTOCKQUANTITY_UPDATED,
        DAYSINSTOCK_UPDATED
    from {{ ref('stg_contoso__inventory') }}
),

-- Date Data 
datedata as (
    select
        DATEKEY_UPDATED,
        CALENDARYEARLABEL,
        CALENDARMONTHLABEL,
        CALENDARQUARTERLABEL,
        ISHOLIDAY_UPDATED
    from {{ ref('stg_contoso__date') }}
),

-- Store Data
storedata as (
    select
        STOREKEY_UPDATED,
        STORETYPE,
        STORENAME
    from {{ ref('stg_contoso__store') }}
),

-- Currency Data
currencydata as (
    select
        CURRENCYKEY_UPDATED,
        CURRENCYNAME
    from {{ ref('stg_contoso__currency') }}
),

-- Centralized Inventory Table
centralizedinventorydata AS (
    SELECT
        -- Inventory Information
        i.INVENTORYKEY_UPDATED,
        i.STOREKEY_UPDATED,
        i.CURRENCYKEY_UPDATED,
        i.DATEKEY_UPDATED,
        i.ONHANDQUANTITY_UPDATED,
        i.ONORDERQUANTITY_UPDATED,
        i.UNITCOST_UPDATED,
        i.SAFETYSTOCKQUANTITY_UPDATED,
        i.DAYSINSTOCK_UPDATED,

        -- Store Information
        s.STORETYPE,
        s.STORENAME,

        -- Currency Information
        c.CURRENCYNAME,

        -- Date Information
        d.CALENDARYEARLABEL,
        d.CALENDARMONTHLABEL,
        d.CALENDARQUARTERLABEL,
        d.ISHOLIDAY_UPDATED

    FROM inventorydata i
    LEFT JOIN datedata d ON i.DATEKEY_UPDATED = d.DATEKEY_UPDATED
    LEFT JOIN storedata s ON i.STOREKEY_UPDATED = s.STOREKEY_UPDATED
    LEFT JOIN currencydata c ON i.CURRENCYKEY_UPDATED = c.CURRENCYKEY_UPDATED
)

select * from centralizedinventorydata

