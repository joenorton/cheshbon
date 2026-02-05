"""Test that JSON schemas don't change unintentionally (snapshot/checksum test)."""

import hashlib
import json
from pathlib import Path


def test_mapping_spec_schema_stable():
    """Test that mapping_spec.schema.json doesn't change unless intentionally updated.
    
    This prevents "one more optional field" from creeping in during refactors.
    """
    schema_path = Path(__file__).parent.parent / "schemas" / "mapping_spec.schema.json"
    assert schema_path.exists(), f"Schema file not found: {schema_path}"
    
    with open(schema_path, 'rb') as f:
        content = f.read()
    
    # Compute checksum
    checksum = hashlib.sha256(content).hexdigest()
    
    # Known checksum (update this when schema intentionally changes)
    # This is a snapshot test - if the schema changes, update this value
    # To update: run test, copy checksum from error, update EXPECTED_CHECKSUM below
    EXPECTED_CHECKSUM = "b27690c9188baa1463c790046966ea5d60b9ed4132199793889e8ce62aa3acda"
    
    assert checksum == EXPECTED_CHECKSUM, (
        f"Schema checksum changed! This indicates an unintentional schema change.\n"
        f"Expected: {EXPECTED_CHECKSUM}\n"
        f"Got:      {checksum}\n"
        f"If this change is intentional, update EXPECTED_CHECKSUM in this test."
    )


def test_transform_registry_schema_stable():
    """Test that transform_registry.schema.json doesn't change unless intentionally updated."""
    schema_path = Path(__file__).parent.parent / "schemas" / "transform_registry.schema.json"
    assert schema_path.exists(), f"Schema file not found: {schema_path}"
    
    with open(schema_path, 'rb') as f:
        content = f.read()
    
    # Compute checksum
    checksum = hashlib.sha256(content).hexdigest()
    
    # Known checksum (update this when schema intentionally changes)
    EXPECTED_CHECKSUM = "a2fe63e44fbb3daebc1a70b9cf69201cf5e30e7e196a23db13120f6a67ef331b"
    
    assert checksum == EXPECTED_CHECKSUM, (
        f"Schema checksum changed! This indicates an unintentional schema change.\n"
        f"Expected: {EXPECTED_CHECKSUM}\n"
        f"Got:      {checksum}\n"
        f"If this change is intentional, update EXPECTED_CHECKSUM in this test."
    )


def test_all_details_report_schema_stable():
    """Test that all_details_report.schema.json doesn't change unintentionally."""
    schema_path = Path(__file__).parent.parent / "schemas" / "all_details_report.schema.json"
    assert schema_path.exists(), f"Schema file not found: {schema_path}"

    with open(schema_path, 'rb') as f:
        content = f.read()

    checksum = hashlib.sha256(content).hexdigest()

    EXPECTED_CHECKSUM = "d6c65d92463a69271d391ff4e2f9934462308e79ea7c500978b1124435c05d91"

    assert checksum == EXPECTED_CHECKSUM, (
        f"Schema checksum changed! This indicates an unintentional schema change.\n"
        f"Expected: {EXPECTED_CHECKSUM}\n"
        f"Got:      {checksum}\n"
        f"If this change is intentional, update EXPECTED_CHECKSUM in this test."
    )
