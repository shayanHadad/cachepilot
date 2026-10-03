"""
Runs the full pipeline: build_dataset -> labeling -> time_split.

Each stage is also runnable on its own (useful for debugging one
stage without re-running the others). This just chains them with
their default file paths.

Usage:
    python pipeline.py
"""

import subprocess
import sys


def run(script: str, *args: str) -> None:
    print(f"\n=== {script} {' '.join(args)} ===")
    result = subprocess.run([sys.executable, script, *args])
    if result.returncode != 0:
        sys.exit(result.returncode)


if __name__ == "__main__":
    run("build_dataset.py")
    run("labeling.py")
    run("time_split.py")
    print("\nPipeline complete.")