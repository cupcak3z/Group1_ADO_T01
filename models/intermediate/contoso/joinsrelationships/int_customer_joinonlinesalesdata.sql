WITH
onlinesales AS (
    SELECT * FROM {{ ref('stg_contoso__onlinesales') }}
),
customer AS (
    SELECT * FROM {{ ref('stg_contoso__customer') }}
),
geography AS (
    SELECT * FROM {{ ref('stg_contoso__geography') }}
),

-- Join onlinesales, customer, and geography data
int_onlinesales_customer AS (
    SELECT
        os.CUSTOMERKEY_UPDATED,
        c.FIRSTNAME,
        c.LASTNAME,
        c.GENDER_UPDATED,
        g.REGIONCOUNTRYNAME,
        c.MARITALSTATUS_UPDATED,
        c.EMAILADDRESS,
        c.OCCUPATION,
        c.ADDRESSLINE1,
        c.PHONE,
        c.CUSTOMERTYPE,
        c.COMPANYNAME_UPDATED,
        os.SALESAMOUNT_UPDATED,
        os.SALESQUANTITY_UPDATED
    FROM onlinesales os
    LEFT JOIN customer c ON os.CUSTOMERKEY_UPDATED = c.CUSTOMERKEY_UPDATED
    LEFT JOIN geography g ON c.GEOGRAPHYKEY_UPDATED = g.GEOGRAPHYKEY_UPDATED
)

SELECT * FROM int_onlinesales_customer



