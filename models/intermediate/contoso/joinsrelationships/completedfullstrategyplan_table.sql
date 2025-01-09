with

-- Strategy Plan Data
strategyplandata as (
    select
        STRATEGYPLANKEY_UPDATED,
        DATEKEY_UPDATED,
        ENTITYKEY_UPDATED,
        SCENARIOKEY_UPDATED,
        ACCOUNTKEY_UPDATED,
        CURRENCYKEY_UPDATED,
        PRODUCTCATEGORYKEY_UPDATED,
        AMOUNT_UPDATED
    from {{ ref('stg_contoso__strategyplan') }}
),

-- Date Data 
datedata as (
    select
        DATEKEY_UPDATED,
        CALENDARYEARLABEL,
        CALENDARMONTHLABEL,
        CALENDARQUARTERLABEL
    from {{ ref('stg_contoso__date') }}
),

-- Entity Data
entitydata as (
    select
        ENTITYKEY_UPDATED,
        ENTITYNAME,
        ENTITYDESCRIPTION,
        ENTITYTYPE
    from {{ ref('stg_contoso__entity') }}
),

-- Scenario Data
scenariodata as (
    select
        SCENARIOKEY_UPDATED,
        SCENARIONAME
    from {{ ref('stg_contoso__scenario') }}
),

-- Account Data
accountdata as (
    select
        ACCOUNTKEY_UPDATED,
        ACCOUNTNAME,
        ACCOUNTTYPE,
        VALUETYPE
    from {{ ref('stg_contoso__account') }}
),

-- Currency Data
currencydata as (
    select
        CURRENCYKEY_UPDATED,
        CURRENCYNAME
    from {{ ref('stg_contoso__currency') }}
),

-- Product Category Data
productcategorydata as (
    select
        PRODUCTCATEGORYKEY_UPDATED,
        PRODUCTCATEGORYNAME
    from {{ ref('stg_contoso__productcategory') }}
),

-- Centralized Strategy Plan Table
centralizedstrategyplandata AS (
    SELECT
        -- Strategy Plan Information
        sp.STRATEGYPLANKEY_UPDATED,
        sp.DATEKEY_UPDATED,
        sp.ENTITYKEY_UPDATED,
        sp.SCENARIOKEY_UPDATED,
        sp.ACCOUNTKEY_UPDATED,
        sp.CURRENCYKEY_UPDATED,
        sp.PRODUCTCATEGORYKEY_UPDATED,
        sp.AMOUNT_UPDATED,

        -- Date Information
        d.CALENDARYEARLABEL,
        d.CALENDARMONTHLABEL,
        d.CALENDARQUARTERLABEL,

        -- Entity Information
        e.ENTITYNAME,
        e.ENTITYDESCRIPTION,
        e.ENTITYTYPE,

        -- Scenario Information
        s.SCENARIONAME,

        -- Account Information
        a.ACCOUNTNAME,
        a.ACCOUNTTYPE,
        a.VALUETYPE,

        -- Currency Information
        c.CURRENCYNAME,

        -- Product Category Information
        p.PRODUCTCATEGORYNAME

    FROM strategyplandata sp
    LEFT JOIN datedata d ON sp.DATEKEY_UPDATED = d.DATEKEY_UPDATED
    LEFT JOIN entitydata e ON sp.ENTITYKEY_UPDATED = e.ENTITYKEY_UPDATED
    LEFT JOIN scenariodata s ON sp.SCENARIOKEY_UPDATED = s.SCENARIOKEY_UPDATED
    LEFT JOIN accountdata a ON sp.ACCOUNTKEY_UPDATED = a.ACCOUNTKEY_UPDATED
    LEFT JOIN currencydata c ON sp.CURRENCYKEY_UPDATED = c.CURRENCYKEY_UPDATED
    LEFT JOIN productcategorydata p ON sp.PRODUCTCATEGORYKEY_UPDATED = p.PRODUCTCATEGORYKEY_UPDATED
)

select * from centralizedstrategyplandata

