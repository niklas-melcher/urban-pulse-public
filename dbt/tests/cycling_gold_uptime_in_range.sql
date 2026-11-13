select *
from {{ ref('cycling_gold_dbt') }}
where avg_uptime < 0 or avg_uptime > 1
