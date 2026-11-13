# scripts/try_databricks_pull.py
from databricks import sql

from urban_pulse.models.settings import Settings

settings = Settings()

settings = Settings()

if not (
    settings.databricks_access_token and settings.databricks_host and settings.databricks_http_path
):
    raise RuntimeError("Databricks credentials not set in .env — required only for this script")

connection = sql.connect(
    server_hostname=settings.databricks_host,
    http_path=settings.databricks_http_path,
    access_token=settings.databricks_access_token,
)

cursor = connection.cursor()
cursor.execute("SELECT * FROM urban_pulse.silver.cycling ORDER BY measured_at LIMIT 5")
rows = cursor.fetchall()

print(f"pulled {len(rows)} rows")
for row in rows:
    print(row)

cursor.close()
connection.close()
