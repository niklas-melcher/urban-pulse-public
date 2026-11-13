select
    segment_id,
    cast(measured_at as date) as day,
    avg(uptime) as avg_uptime,
    sum(pedestrian_count) as total_pedestrians,
    sum(bike_count) as total_bikes,
    sum(car_count) as total_cars,
    sum(heavy_count) as total_heavy
from {{ source('silver', 'cycling') }}
group by 1, 2
