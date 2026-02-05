import json
import hashlib

from cheshbon.kernel.hash_utils import compute_canonical_json_sha256


def test_compute_canonical_json_sha256_pretty_printed(tmp_path):
    payload = {
        "b": 2,
        "a": {"z": 1, "y": 2},
        "list": [3, 2, 1],
    }

    path = tmp_path / "pretty.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    raw_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    canonical_str = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    canonical_hash = hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

    assert raw_hash != canonical_hash
    assert compute_canonical_json_sha256(path) == canonical_hash
