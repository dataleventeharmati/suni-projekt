import subprocess

from datetime import datetime
from pathlib import Path

RUN_ID = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
RUN_DIR = Path("runs") / RUN_ID
RUN_DIR.mkdir(parents=True, exist_ok=True)

import sys
from pathlib import Path
from datetime import datetime

LOG_FILE = Path("pipeline_build.log")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")

def run_step(cmd, step_name):
    log(f"START: {step_name}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        log(f"FAILED: {step_name}")
        sys.exit(1)
    log(f"OK: {step_name}")

def main():
    log("=== PIPELINE BUILD START ===")

    run_step("python3 src/ingest_bronze.py", "Bronze Ingest")
    run_step("python3 src/validate_and_clean.py", "Silver Clean & Validate")
    run_step("python3 src/data_profile_report.py", "Data Profile Report")
    run_step("python3 src/dq_gate.py", "Data Quality Gate")
    run_step("python3 src/build_gold_kpis.py", "Gold KPI Mart")
    run_step("python3 src/report_kpi_html.py", "KPI HTML Report")
    run_step("python3 src/artifacts_summary.py", "Artifacts Summary")


    
    # snapshot reports to versioned run folder
    import shutil
    run_reports_dir = RUN_DIR / "reports"
    if Path("reports").exists():
        shutil.copytree("reports", run_reports_dir, dirs_exist_ok=True)
        log(f"SNAPSHOT: copied reports -> {run_reports_dir}")

    log("=== PIPELINE BUILD COMPLETE ===")

if __name__ == "__main__":
    main()
