import os
import sys

# Ensure src/ is on path for local dev runs
ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from plana_engine.report.report_entrypoint import generate_report_payload

if __name__ == "__main__":
    proposal = " ".join(sys.argv[1:]).strip()
    if not proposal:
        raise SystemExit("Usage: python run_engine.py <proposal text>")
    out = generate_report_payload(
        proposal_text=proposal,
        authority="newcastle",
        doc_keys=["dap_2020", "csucp_2015", "nppf_2024"]
    )
    import json
    print(json.dumps(out, indent=2))
