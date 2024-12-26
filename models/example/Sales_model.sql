SELECT *
FROM {{ source('contoso', 'Sales') }};
