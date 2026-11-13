select segment_id, day, count(*) as n
from {{ ref('cycling_gold_dbt') }}
group by 1, 2
having count(*) > 1
