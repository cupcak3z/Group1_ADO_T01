--Customer Segmentation

WITH
onlinesalescustomerdata AS (
    SELECT * 
    FROM {{ ref('int_customer_joinonlinesalesdata') }}
),

customer_sales_summary AS (
    SELECT
        CUSTOMERKEY_UPDATED,
        FIRSTNAME,
        LASTNAME,
        EMAILADDRESS,
        GENDER_UPDATED,
        REGIONCOUNTRYNAME,
        OCCUPATION,
        CUSTOMERTYPE,
        SUM(SALESAMOUNT_UPDATED) AS TotalSales,
        COUNT(*) AS transaction_count
    FROM onlinesalescustomerdata
    GROUP BY
        CUSTOMERKEY_UPDATED,
        FIRSTNAME,
        LASTNAME,
        EMAILADDRESS,
        GENDER_UPDATED,
        REGIONCOUNTRYNAME, 
        OCCUPATION,
        CUSTOMERTYPE
),

-- Segmentation Logic
segmented_customers AS (
    SELECT 
        CUSTOMERKEY_UPDATED,
        FIRSTNAME,
        LASTNAME,
        EMAILADDRESS,
        GENDER_UPDATED,
        REGIONCOUNTRYNAME,
        OCCUPATION,
        CUSTOMERTYPE,
        transaction_count,
        ROUND(TotalSales, 2) AS TotalSales,
        -- Frequency segmentation
        CASE
            WHEN transaction_count > 200 THEN 'High Frequency'
            WHEN transaction_count BETWEEN 100 AND 200 THEN 'Medium Frequency'
            ELSE 'Low Frequency'
        END AS frequency_segment,
        
        -- Value segmentation (based on TotalSales)
        CASE
            WHEN TotalSales > 50000 THEN 'High Value'
            WHEN TotalSales BETWEEN 20000 AND 50000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_segment
    FROM customer_sales_summary
)

SELECT * FROM segmented_customers
WHERE FIRSTNAME IS NOT NULL AND LASTNAME IS NOT NULL
  AND TRIM(FIRSTNAME) <> 'NULL' AND TRIM(LASTNAME) <> 'NULL'
