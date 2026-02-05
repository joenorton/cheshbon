"""Generate JSON schemas from Pydantic models and save to schemas/ directory."""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cheshbon.kernel.spec import MappingSpec
from cheshbon.kernel.transform_registry import TransformRegistry


def generate_schemas():
    """Generate JSON schemas for all models."""
    schemas_dir = Path(__file__).parent.parent / "schemas"
    schemas_dir.mkdir(exist_ok=True)
    
    # Generate mapping spec schema
    mapping_schema = MappingSpec.model_json_schema()
    mapping_schema_path = schemas_dir / "mapping_spec.schema.json"
    with open(mapping_schema_path, 'w', encoding='utf-8') as f:
        json.dump(mapping_schema, f, indent=2, ensure_ascii=False)
    print(f"Generated: {mapping_schema_path}")
    
    # Generate transform registry schema
    registry_schema = TransformRegistry.model_json_schema()
    registry_schema_path = schemas_dir / "transform_registry.schema.json"
    with open(registry_schema_path, 'w', encoding='utf-8') as f:
        json.dump(registry_schema, f, indent=2, ensure_ascii=False)
    print(f"Generated: {registry_schema_path}")
    
    print("\nSchema generation complete!")


if __name__ == "__main__":
    generate_schemas()
