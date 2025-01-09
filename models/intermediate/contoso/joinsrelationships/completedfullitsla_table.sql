WITH
itsla AS (
    SELECT 
        ITSLAKEY_updated,
        STOREKEY_updated,
        DATEKEY_updated,
        MACHINEKEY_updated,
        OUTAGEKEY_updated
    FROM {{ ref('stg_contoso__itsla') }}
),
store AS (
    SELECT
        STOREKEY_UPDATED, 
        STORETYPE,
        STORENAME,
        STATUS as STORE_STATUS,
        CLOSEREASON_updated,
        EMPLOYEECOUNT_Updated,
        OPENDATE_updated,
        YEARSSINCEOPEN,
        YEARSLEFT,
        DAYSSINCELASTREMODEL
    FROM {{ ref('stg_contoso__store') }}
),
machine AS (
    SELECT
       MACHINEKEY_updated,
       STOREKEY_updated,
       MACHINETYPE,
       MACHINENAME,
       MACHINEDESCRIPTION,
       VENDORNAME,
       MACHINEOS,
       MACHINESOURCE,
       MACHINEHARDWARE,
       MACHINESOFTWARE,
       STATUS AS MACHINE_STATUS,
       SERVICESTARTDATE_updated,
       DECOMMISSIONDATE_updated,
       LASTMODIFIEDDATE_updated,
       YEARSSERVICELIFE,
       DAYSSINCELASTMODIFICATION
    FROM {{ ref('stg_contoso__machine') }} 
),
outage AS (
    SELECT 
        OUTAGEKEY_updated,
        OUTAGENAME,
        OUTAGESUBTYPE,
        OUTAGESUBTYPEDESCRIPTION
    FROM {{ ref('stg_contoso__outage') }} 
),

date as (
    select 
        DATEKEY_updated,
        CALENDARYEAR_updated,
        CALENDARMONTHLABEL,
        CALENDARHALFYEARLABEL,
        CALENDARQUARTERLABEL,
        CALENDARWEEKLABEL,
        CALENDARDAYOFWEEKLABEL,
        FISCALYEAR_updated,
        FISCALHALFYEARLABEL,
        FISCALQUARTERLABEL,
        FISCALMONTHLABEL,
        ISHOLIDAY_updated,
        ISWORKDAY,
        EUROPESEASON_updated,
        NORTHAMERICASEASON_updated,
        ASIASEASON_updated
    from {{ ref('stg_contoso__date') }}
) 

SELECT 
    -- itsla table
    i.ITSLAKEY_updated,
    i.STOREKEY_updated,
    i.DATEKEY_updated,
    i.MACHINEKEY_updated,
    i.OUTAGEKEY_updated,

    -- store table
    s.STORENAME,
    s.STORETYPE,
    s.STORE_STATUS,
    s.CLOSEREASON_updated,
    s.EMPLOYEECOUNT_Updated,
    s.OPENDATE_updated,
    s.YEARSSINCEOPEN,
    s.YEARSLEFT,
    s.DAYSSINCELASTREMODEL,

    -- machine table
    m.MACHINENAME,
    m.MACHINETYPE,
    m.MACHINEDESCRIPTION,
    m.VENDORNAME,
    m.MACHINEOS,
    m.MACHINESOURCE,
    m.MACHINEHARDWARE,
    m.MACHINESOFTWARE,
    m.MACHINE_STATUS,
    m.SERVICESTARTDATE_updated,
    m.DECOMMISSIONDATE_updated,
    m.LASTMODIFIEDDATE_updated,
    m.YEARSSERVICELIFE,
    m.DAYSSINCELASTMODIFICATION,

    -- outage table
    o.OUTAGENAME,
    o.OUTAGESUBTYPE,
    o.OUTAGESUBTYPEDESCRIPTION,

    -- date table
    d.CALENDARYEAR_updated,
    d.CALENDARMONTHLABEL,
    d.CALENDARHALFYEARLABEL,
    d.CALENDARQUARTERLABEL,
    d.CALENDARWEEKLABEL,
    d.CALENDARDAYOFWEEKLABEL,
    d.FISCALYEAR_updated,
    d.FISCALHALFYEARLABEL,
    d.FISCALQUARTERLABEL,
    d.FISCALMONTHLABEL,
    d.ISHOLIDAY_updated,
    d.ISWORKDAY,
    d.EUROPESEASON_updated,
    d.NORTHAMERICASEASON_updated,
    d.ASIASEASON_updated
FROM itsla i
LEFT JOIN store s ON i.STOREKEY_updated = s.STOREKEY_UPDATED
LEFT JOIN machine m ON i.MACHINEKEY_updated = m.MACHINEKEY_updated
LEFT JOIN outage o ON i.OUTAGEKEY_updated = o.OUTAGEKEY_updated
LEFT JOIN date d ON i.DATEKEY_updated = d.DATEKEY_updated



