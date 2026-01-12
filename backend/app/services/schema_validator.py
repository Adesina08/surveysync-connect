from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SchemaValidationResult:
    is_valid: bool
    errors: list[str]


def validate_schema(schema_definition: dict) -> SchemaValidationResult:
    required_keys = {"tables"}
    missing = required_keys - set(schema_definition.keys())
    if missing:
        return SchemaValidationResult(is_valid=False, errors=[f"Missing keys: {', '.join(sorted(missing))}"])
    return SchemaValidationResult(is_valid=True, errors=[])
