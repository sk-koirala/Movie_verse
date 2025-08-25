import os
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG = str(ROOT / "config" / "config.yaml")

def run(cmd):
    print(f"\n=== Running: {' '.join(cmd)} ===\n")
    subprocess.check_call(cmd)

if __name__ == "__main__":
    run([sys.executable, str(ROOT/"extract"/"execute.py"), CONFIG])
    run([sys.executable, str(ROOT/"transform"/"execute.py"), CONFIG])
    run([sys.executable, str(ROOT/"load"/"execute.py"), CONFIG])
    print("\nAll steps complete ✅")
