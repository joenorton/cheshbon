"""Report contract constants for all-details artifacts."""

ALL_DETAILS_SCHEMA_VERSION = "0.1"
VERIFIER_CONTRACT_VERSION = "1"
CANONICALIZATION_POLICY_ID = "cheshbon.canonical-json.v1"

# Default caps for all-details reports (can be overridden by callers).
DEFAULT_REPORT_CAPS = {
    "max_witnesses": 100000,
    "max_root_causes_per_node": 16,
    "max_trigger_events_per_node": 16,
    "max_top_roots": 50,
}
