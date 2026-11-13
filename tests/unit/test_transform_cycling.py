from urban_pulse.transform.cycling import parse_cycling_reports


def _complete_report(**overrides: object) -> dict:
    """A full, valid raw Telraam report, with individual fields
    overridable per test case — avoids repeating all 7 fields in every
    test when only one is meant to be wrong."""
    base = {
        "segment_id": 1,
        "date": "2026-08-01T12:00:00.000Z",
        "uptime": 0.99,
        "pedestrian": 50.0,
        "bike": 20.0,
        "car": 100.0,
        "heavy": 5.0,
    }
    base.update(overrides)
    return base


def test_parse_cycling_reports_skips_invalid_keeps_valid():
    raw = {
        "1": [
            _complete_report(),  # valid
            {"segment_id": 1},  # missing almost everything -> KeyError, skipped
            _complete_report(pedestrian=-10.0),  # negative -> ValidationError, skipped
            _complete_report(uptime=0.5),  # valid, different uptime
        ]
    }

    result = parse_cycling_reports(raw)

    assert len(result) == 2
    assert result[0].pedestrian_count == 50.0
    assert result[1].uptime == 0.5
