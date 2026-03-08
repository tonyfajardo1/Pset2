{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='payment_type_id',
        incremental_strategy='delete+insert',
        pre_hook="TRUNCATE TABLE gold.dim_payment_type"
    )
}}

-- Dimension de tipos de pago
-- Tabla particionada por LIST (payment_type): card, cash, other
SELECT
    payment_type_id,
    payment_type::VARCHAR(10) AS payment_type,
    payment_type_name,
    payment_type_description
FROM (
    VALUES
        (1, 'card', 'Credit Card', 'Payment made via credit card'),
        (2, 'cash', 'Cash', 'Payment made in cash'),
        (3, 'other', 'No Charge', 'No charge for the trip'),
        (4, 'other', 'Dispute', 'Disputed transaction'),
        (5, 'other', 'Unknown', 'Unknown payment method'),
        (6, 'other', 'Voided Trip', 'Trip was voided')
) AS t(payment_type_id, payment_type, payment_type_name, payment_type_description)
