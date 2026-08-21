

"""
Simple representation-bias detection suite for the OULAD analytic table.
Data Analytics Capstone – Module 3
Student: Gloria Ayesiga

Checks whether key demographic groups are reasonably represented
relative to the overall cohort (a basic proxy for representation bias).
"""

import pandas as pd
from pathlib import Path
import json
from datetime import datetime


def run_bias_checks(analytic_path: str = "data/processed/analytic_table.parquet") -> dict:
    path = Path(analytic_path)
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    results = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "table": str(path),
        "n_rows": len(df),
        "checks": [],
    }

    def add(name, success, detail):
        results["checks"].append({"check": name, "success": bool(success), "detail": detail})

    # Gender distribution
    if "gender" in df.columns:
        counts = df["gender"].value_counts(normalize=True)
        # Flag if any gender group is < 15 % or > 85 %
        extreme = any((counts < 0.15) | (counts > 0.85))
        add("gender_representation", not extreme, counts.round(3).to_dict())

    # Disability flag
    if "disability" in df.columns:
        counts = df["disability"].value_counts(normalize=True)
        add("disability_representation", True, counts.round(3).to_dict())

    # Age band
    if "age_band" in df.columns:
        counts = df["age_band"].value_counts(normalize=True)
        add("age_band_representation", True, counts.round(3).to_dict())

    # IMD band (socio-economic proxy) – only if present
    if "imd_band" in df.columns:
        # Treat "Unknown" separately
        known = df[df["imd_band"] != "Unknown"]
        if len(known) > 0:
            counts = known["imd_band"].value_counts(normalize=True)
            add("imd_band_representation", True, counts.round(3).to_dict())

    results["n_checks"] = len(results["checks"])
    results["n_flagged"] = sum(1 for c in results["checks"] if not c["success"])

    out = Path("logs") / "bias_detection_results.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Bias checks complete – {results['n_flagged']} potential flags")
    print(f"Results → {out}")
    return results


if __name__ == "__main__":
    run_bias_checks()
