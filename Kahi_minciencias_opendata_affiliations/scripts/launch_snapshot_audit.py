#!/usr/bin/env python3
"""Launch the isolated audit with a persistent log, PID and worker status."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--activate-workflow", action="store_true")
    parser.add_argument("--check-yuku", action="store_true")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    pid_path = args.output_dir / "pid"
    status_path = args.output_dir / "status.json"
    if status_path.exists():
        previous = json.loads(status_path.read_text())
        if previous.get("status") in {"starting", "running"} and pid_path.exists():
            try:
                os.kill(int(pid_path.read_text()), 0)
            except ProcessLookupError:
                pass
            else:
                raise SystemExit("An audit is already running in this output directory")
    status_path.write_text(json.dumps({"status": "starting"}))
    worker = Path(__file__).with_name("audit_snapshot_migration.py")
    command = [sys.executable, "-u", str(worker), "--workflow", str(args.workflow.resolve()),
               "--output-dir", str(args.output_dir.resolve())]
    if args.activate_workflow:
        command.append("--activate-workflow")
    if args.check_yuku:
        command.append("--check-yuku")
    log_path = args.output_dir / "audit.log"
    with log_path.open("ab") as log:
        process = subprocess.Popen(command, start_new_session=True,
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
    pid_path.write_text(str(process.pid) + "\n")
    print(json.dumps({"pid": process.pid, "log": str(log_path), "status": str(status_path)}))


if __name__ == "__main__":
    main()
