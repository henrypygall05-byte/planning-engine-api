import os
import json
import subprocess
from pathlib import Path
from fastapi import FastAPI
from pydantic import BaseModel

BASE = Path(__file__).resolve().parents[1]  # .../Newcastle
ENGINE = BASE / "plana" / "engine" / "run_engine.py"
RENDER = BASE / "scripts" / "render_council_report.py"
LOGS = BASE / "logs"

class AnalyzeRequest(BaseModel):
    proposal_text: str

app = FastAPI(title="Plana Newcastle C3 Pilot API")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/analyze")
def analyze(req: AnalyzeRequest):
    LOGS.mkdir(parents=True, exist_ok=True)

    db_path = os.environ.get("PLANA_DB_PATH", "")
    if not db_path:
        return {"ok": False, "error": "PLANA_DB_PATH is not set"}

    payload_path = LOGS / "payload_latest.json"
    report_path = LOGS / "report_latest.md"

    # Run your existing engine (writes JSON to stdout)
    p = subprocess.run(
        ["python3", str(ENGINE), req.proposal_text],
        cwd=str(BASE),
        capture_output=True,
        text=True,
    )

    if p.returncode != 0:
        return {
            "ok": False,
            "error": "Engine failed",
            "stderr": p.stderr[-4000:],
            "stdout": p.stdout[-4000:],
        }

    # Validate/parse JSON
    try:
        payload = json.loads(p.stdout)
    except Exception as e:
        return {
            "ok": False,
            "error": f"Engine output not valid JSON: {e}",
            "stdout_head": p.stdout[:500],
        }

    payload_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Render council-style report
    p2 = subprocess.run(
        ["python3", str(RENDER), str(payload_path), "-o", str(report_path)],
        cwd=str(BASE),
        capture_output=True,
        text=True,
    )
    if p2.returncode != 0:
        return {
            "ok": False,
            "error": "Report render failed",
            "stderr": p2.stderr[-4000:],
            "stdout": p2.stdout[-4000:],
        }

    report_md = report_path.read_text(encoding="utf-8", errors="ignore")

    return {
        "ok": True,
        "payload": payload,
        "report_markdown": report_md,
        "paths": {
            "payload": str(payload_path),
            "report": str(report_path),
        },
    }
