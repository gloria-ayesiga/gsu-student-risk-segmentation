
"""
Prefect ETL Flow for OULAD Student Risk Segmentation Pipeline
Data Analytics Capstone – Module 3
Student: Gloria Ayesiga

This flow implements the stages defined in the Module 3 pipeline:
Ingestion → Cleaning → Transformation → Integration → Validation
"""

from prefect import flow, task, get_run_logger
from pathlib import Path
import pandas as pd
import json
from datetime import datetime

# -------------------------------------------------
# Configuration
# -------------------------------------------------
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = Path("logs")

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------
# Tasks
# -------------------------------------------------
@task(name="ingest_oulad_tables", retries=2, retry_delay_seconds=10)
def ingest_oulad_tables(raw_path: str = "data/raw") -> dict:
    """Load the seven core OULAD CSV tables."""
    logger = get_run_logger()
    path = Path(raw_path)

    tables = {
        "courses": "courses.csv",
        "assessments": "assessments.csv",
        "vle": "vle.csv",
        "student_info": "studentInfo.csv",
        "student_registration": "studentRegistration.csv",
        "student_assessment": "studentAssessment.csv",
        "student_vle": "studentVle.csv",
    }

    loaded = {}
    for name, filename in tables.items():
        fp = path / filename
        if not fp.exists():
            logger.warning(f"{filename} not found – skipping")
            continue
        df = pd.read_csv(fp)
        loaded[name] = df
        logger.info(f"Loaded {name}: {df.shape[0]:,} rows × {df.shape[1]} columns")

    return loaded


@task(name="clean_student_info")
def clean_student_info(student_info: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning for the core student demographic table."""
    logger = get_run_logger()
    df = student_info.copy()

    # Standardise column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Handle missing values in key demographic fields
    for col in ["imd_band", "disability", "highest_education"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    # Remove exact duplicate student-module-presentation rows
    before = len(df)
    df = df.drop_duplicates(subset=["code_module", "code_presentation", "id_student"])
    logger.info(f"Removed {before - len(df)} duplicate rows from student_info")

    return df


@task(name="engineer_early_engagement")
def engineer_early_engagement(student_vle: pd.DataFrame, window_days: int = 30) -> pd.DataFrame:
    """Aggregate VLE clicks in the first N days of a presentation."""
    logger = get_run_logger()
    df = student_vle.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    early = (
        df[df["date"] <= window_days]
        .groupby(["code_module", "code_presentation", "id_student"])["sum_click"]
        .sum()
        .reset_index()
        .rename(columns={"sum_click": "early_clicks"})
    )
    logger.info(f"Early engagement features created for {len(early):,} student-module pairs")
    return early


@task(name="integrate_analytic_table")
def integrate_analytic_table(
    student_info: pd.DataFrame,
    student_registration: pd.DataFrame,
    early_engagement: pd.DataFrame,
) -> pd.DataFrame:
    """Join core tables into a single analysis-ready dataset."""
    logger = get_run_logger()

    # Normalise column names
    for frame in [student_info, student_registration, early_engagement]:
        frame.columns = [c.strip().lower() for c in frame.columns]

    # Base: student demographics + final result
    df = student_info.copy()

    # Registration timing
    if not student_registration.empty:
        reg = student_registration[
            ["code_module", "code_presentation", "id_student", "date_registration", "date_unregistration"]
        ]
        df = df.merge(reg, on=["code_module", "code_presentation", "id_student"], how="left")

    # Early VLE engagement
    if not early_engagement.empty:
        df = df.merge(early_engagement, on=["code_module", "code_presentation", "id_student"], how="left")
        df["early_clicks"] = df["early_clicks"].fillna(0)

    logger.info(f"Integrated analytic table shape: {df.shape}")
    return df


@task(name="write_processed_output")
def write_processed_output(df: pd.DataFrame, output_path: str = "data/processed/analytic_table.parquet") -> str:
    """Persist the analysis-ready table."""
    logger = get_run_logger()
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)

    logger.info(f"Wrote processed output → {path} ({len(df):,} rows)")
    return str(path)


@task(name="log_pipeline_run")
def log_pipeline_run(output_path: str, n_rows: int) -> None:
    """Simple privacy / audit log entry."""
    logger = get_run_logger()
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "event": "pipeline_complete",
        "output": output_path,
        "rows": n_rows,
        "dataset": "OULAD",
        "project": "GSU Student Risk Segmentation",
    }
    log_file = LOG_DIR / "privacy_audit.log"
    with open(log_file, "a") as f:
        f.write(json.dumps(entry) + "\n")
    logger.info(f"Audit log written → {log_file}")


# -------------------------------------------------
# Main Flow
# -------------------------------------------------
@flow(name="oulad_student_risk_etl", log_prints=True)
def oulad_etl_flow(raw_data_path: str = "data/raw"):
    """
    End-to-end Prefect flow for the OULAD risk-segmentation pipeline.
    Stages: Ingest → Clean → Transform → Integrate → Persist → Audit
    """
    logger = get_run_logger()
    logger.info("Starting OULAD Student Risk Segmentation ETL flow")

    # 1. Ingestion
    tables = ingest_oulad_tables(raw_data_path)

    if "student_info" not in tables:
        raise ValueError("studentInfo.csv is required but was not found")

    # 2. Cleaning
    clean_info = clean_student_info(tables["student_info"])

    # 3. Transformation (early engagement)
    early = (
        engineer_early_engagement(tables["student_vle"])
        if "student_vle" in tables
        else pd.DataFrame()
    )

    # 4. Integration
    analytic = integrate_analytic_table(
        clean_info,
        tables.get("student_registration", pd.DataFrame()),
        early,
    )

    # 5. Persist
    out_path = write_processed_output(analytic)

    # 6. Privacy / audit log
    log_pipeline_run(out_path, len(analytic))

    logger.info("ETL flow completed successfully")
    return out_path


if __name__ == "__main__":
    oulad_etl_flow()
