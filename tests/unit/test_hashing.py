from urban_pulse.storage.hashing import content_hash, natural_key_hash


def test_content_hash_same_data_different_key_order_is_equal():
    a = {"segment_id": 123, "pedestrian": 99.0}
    b = {"pedestrian": 99.0, "segment_id": 123}
    assert content_hash(a) == content_hash(b)


def test_content_hash_different_data_is_different():
    a = {"segment_id": 123}
    b = {"segment_id": 456}
    assert content_hash(a) != content_hash(b)


def test_natural_key_hash_is_deterministic():
    assert natural_key_hash(123, "2026-08-01") == natural_key_hash(123, "2026-08-01")
