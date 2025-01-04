{{ config(materialized='table') }}

select *
from {{ source('ADO_GROUP1_DB_RAW', 'DIMPRODUCT_RAW') }}