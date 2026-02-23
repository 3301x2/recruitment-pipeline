"""run the full pipeline: bronze -> silver -> gold"""

import subprocess
import sys
import time


def run_step(name, script):
    print(f"\n{'='*50}")
    print(f"  {name}")
    print(f"{'='*50}")
    start = time.time()
    result = subprocess.run([sys.executable, script], capture_output=False)
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"FAILED: {name} (exit code {result.returncode})")
        sys.exit(1)
    print(f"  completed in {elapsed:.1f}s")


def main():
    print("RECRUITMENT PIPELINE — FULL RUN")
    run_step("BRONZE: ingest api + csv", "scripts/ingest.py")
    run_step("SILVER: clean + merge", "scripts/transform_silver.py")
    run_step("GOLD: build star schema", "scripts/transform_gold.py")
    print(f"\n{'='*50}")
    print("  ALL STEPS COMPLETE")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
