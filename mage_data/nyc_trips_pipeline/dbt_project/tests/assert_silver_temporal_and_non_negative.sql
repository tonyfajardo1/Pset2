-- Reglas minimas de calidad en silver:
-- 1) pickup_datetime y dropoff_datetime no nulos
-- 2) pickup_datetime <= dropoff_datetime
-- 3) trip_distance >= 0
-- 4) total_amount >= 0

with unified as (
    select * from {{ ref('silver_trips_unified') }}
)
select *
from unified
where pickup_datetime is null
   or dropoff_datetime is null
   or pickup_datetime > dropoff_datetime
   or trip_distance < 0
   or total_amount < 0
