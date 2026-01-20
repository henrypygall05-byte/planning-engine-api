import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Commands that are allowed to return exit code 1 (e.g. "no new weeks")
ALLOW_EXIT_1 = {
    "scripts/ingest_weekly_received_v5_1_resilient.py",
    "scripts/ingest_weekly_determined_v2.py",
}

def run(cmd: list[str]):
    print("\n$", " ".join(cmd))
    p = subprocess.run(cmd, cwd=str(ROOT))
    if p.returncode == 0:
        return
    if p.returncode == 1 and any(x in cmd for x in ALLOW_EXIT_1):
        print("ℹ️ Non-fatal: script returned exit code 1 (likely no new weeks). Continuing.")
        return
    raise SystemExit(f"❌ Command failed with exit code {p.returncode}: {' '.join(cmd)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--weeks", type=int, default=6, help="How many recent weeks to attempt")
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    # 1) received (new applications)
    run([
        "python", "scripts/ingest_weekly_received_v5_1_resilient.py",
        "--years", "1",
        "--max-weeks", str(args.weeks),
        "--sleep", str(args.sleep)
    ])

    # 2) determined (decisions)
    run([
        "python", "scripts/ingest_weekly_determined_v2.py",
        "--years", "1",
        "--max-weeks", str(args.weeks),
        "--sleep", str(args.sleep)
    ])

    # 3) enrich proposals from local stored weekly HTML
    run(["python", "scripts/enrich_fields_local.py"])

    # 4) embeddings (only decided)
    run(["python", "scripts/build_embeddings_local.py", "--only-decided"])

    print("\n✅ WEEKLY UPDATE COMPLETE")

if __name__ == "__main__":
    main()
