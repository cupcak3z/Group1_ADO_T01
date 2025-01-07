WITH itmachine AS (
    SELECT 
        ITMACHINEKEY_updated,
        MACHINEKEY_updated,
        DATEKEY_updated,
        COSTTYPE,
        COSTAMOUNT_updated,
    FROM {{ ref('stg_contoso__itmachine') }}
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

date as (
    select 
        DATEKEY_updated,
        CALENDARYEAR_updated,
        CALENDARMONTHLABEL,
        CALENDARHALFYEARLABEL,
        CALENDARQUARTERLABEL,
        CALENDARWEEKLABEL,
        CALENDARDAYOFWEEKLABEL,
        ISHOLIDAY_updated,
        ISWORKDAY,
        EUROPESEASON_updated,
        NORTHAMERICASEASON_updated,
        ASIASEASON_updated
    from {{ ref('stg_contoso__date') }}
) 

SELECT 
    -- From itmachine
    itm.ITMACHINEKEY_updated,
    itm.MACHINEKEY_updated,
    itm.DATEKEY_updated,
    itm.COSTTYPE,
    itm.COSTAMOUNT_updated,

    -- From machine
    m.STOREKEY_updated,
    m.MACHINETYPE,
    m.MACHINENAME,
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

    -- From date
    d.CALENDARYEAR_updated,
    d.CALENDARMONTHLABEL,
    d.CALENDARHALFYEARLABEL,
    d.CALENDARQUARTERLABEL,
    d.CALENDARWEEKLABEL,
    d.CALENDARDAYOFWEEKLABEL,
    d.ISHOLIDAY_updated,
    d.ISWORKDAY,
    d.EUROPESEASON_updated,
    d.NORTHAMERICASEASON_updated,
    d.ASIASEASON_updated
FROM itmachine itm
LEFT JOIN machine m ON itm.MACHINEKEY_updated = m.MACHINEKEY_updated
LEFT JOIN date d ON itm.DATEKEY_updated = d.DATEKEY_updated