from urban_pulse.models.settings import Settings
from urban_pulse.quality.silver_checks import run_silver_cycling_checks
from urban_pulse.storage.connection import get_connection

settings = Settings()
conn = get_connection(settings)

all_passed = run_silver_cycling_checks(conn)

print(f"\noverall: {'ALL PASSED' if all_passed else 'SOME CHECKS FAILED'}")
