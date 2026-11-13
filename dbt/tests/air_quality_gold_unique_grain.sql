select station_number, pollutant, day, count(*) as n
from {{ ref('air_quality_gold_dbt') }}
group by 1, 2, 3
having count(*) > 1
