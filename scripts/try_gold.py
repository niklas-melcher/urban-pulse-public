from urban_pulse.models.settings import Settings
from urban_pulse.storage.connection import get_connection
from urban_pulse.storage.gold import create_gold_views

# write a script that test the gold tables

settings = Settings()
conn = get_connection(settings)

create_gold_views(conn)

# query the gold tables and print the results
for view in [
    "gold_cycling_daily",
    "gold_air_quality_daily",
    "gold_cycling_latest",
    "gold_air_quality_latest",
]:
    print(f"\n=== {view} ===")
    print(conn.execute(f"SELECT * FROM {view} LIMIT 10").fetchdf().to_string())
