{{
    config(
        materialized='table',
        schema='gold'
    )
}}

SELECT
    rate_code_id,
    rate_code_name,
    rate_code_description
FROM (
    VALUES
        (1, 'Standard Rate', 'Standard metered fare'),
        (2, 'JFK', 'JFK Airport flat fare'),
        (3, 'Newark', 'Newark Airport'),
        (4, 'Nassau/Westchester', 'Trips to Nassau or Westchester'),
        (5, 'Negotiated Fare', 'Negotiated fare between driver and passenger'),
        (6, 'Group Ride', 'Group ride shared fare')
) AS t(rate_code_id, rate_code_name, rate_code_description)
