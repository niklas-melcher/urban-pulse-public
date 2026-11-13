select *
from {{ ref('air_quality_gold_dbt') }}
where min_value > max_value