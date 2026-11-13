select
    station_number,
    pollutant,
    cast(measured_at as date) as day,
    avg(value) as avg_value,
    min(value) as min_value,
    max(value) as max_value,
    count(*) as reading_count
from {{ source('silver', 'air_quality') }}
group by 1, 2, 3
