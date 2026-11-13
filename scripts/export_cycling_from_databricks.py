import json

from databricks import sql

from urban_pulse.models.settings import Settings

settings = Settings()

if not (
    settings.databricks_access_token and settings.databricks_host and settings.databricks_http_path
):
    raise RuntimeError("Databricks credentials not set in .env")

connection = sql.connect(
    server_hostname=settings.databricks_host,
    http_path=settings.databricks_http_path,
    access_token=settings.databricks_access_token,
)
cursor = connection.cursor()
cursor.execute("SELECT * FROM urban_pulse.silver.cycling ORDER BY measured_at LIMIT 20")
rows = cursor.fetchall()
cursor.close()
connection.close()

records = [
    {
        "segment_id": row.segment_id,
        "measured_at": row.measured_at.isoformat(),
        "bike_count": row.bike_count,
        "pedestrian_count": row.pedestrian_count,
    }
    for row in rows
]

with open("data/cycling_export.json", "w") as f:
    json.dump(records, f, indent=2)

print(f"exported {len(records)} records to data/cycling_export.json")
