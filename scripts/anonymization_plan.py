

"""
Data Anonymization Plan & helper for OULAD-derived tables.
Georgia State University Capstone – Module 3
Student: Gloria Ayesiga

OULAD is already anonymised by the data provider.
This script documents the additional safeguards we apply
and provides a simple utility that can be extended if
any residual quasi-identifiers need further protection.
"""

import pandas as pd
from pathlib import Path
import hashlib
import json
from datetime import datetime


def hash_identifier(value, salt: str = "gsu-capstone-2026") -> str:
    """One-way hash for any residual identifier columns."""
    if pd.isna(value):
        return None
    return hashlib.sha256(f"{salt}{value}".encode()).hexdigest()[:16]


def apply_additional_anonymization(
    analytic_path: str = "data/processed/analytic_table.parquet",
    output_path: str = "data/processed/analytic_table_anon.parquet",
) -> str:
    path = Path(analytic_path)
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    # OULAD id_student is already a synthetic identifier.
    # We optionally re-hash it for an extra layer of protection
    # when sharing derived tables outside the project team.
    if "id_student" in df.columns:
        df["id_student"] = df["id_student"].apply(hash_identifier)

    # Ensure no free-text fields that could contain residual PII
    # (OULAD does not contain free-text name/address fields)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.suffix == ".parquet":
        df.to_parquet(out, index=False)
    else:
        df.to_csv(out, index=False)

    # Log the anonymization action
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event": "additional_anonymization",
        "input": str(path),
        "output": str(out),
        "rows": len(df),
        "action": "re-hashed id_student",
    }
    log_file = Path("logs") / "privacy_audit.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")

    print(f"Anonymised table written → {out}")
    return str(out)


if __name__ == "__main__":
    apply_additional_anonymization()
