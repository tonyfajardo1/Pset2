{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Dimension de vendors (proveedores de tecnologia TPEP/LPEP)
SELECT
    vendor_id,
    vendor_name,
    vendor_description
FROM (
    VALUES
        (1, 'Creative Mobile Technologies', 'CMT - Technology provider for taxi meters'),
        (2, 'VeriFone Inc', 'VTS - Technology provider for taxi meters'),
        (0, 'Unknown', 'Unknown vendor')
) AS t(vendor_id, vendor_name, vendor_description)
