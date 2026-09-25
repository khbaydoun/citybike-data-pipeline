"""
Runs the provided event_generator.py without modifying it.

- Works around the generator reading `args.brokers` (the flag is `--broker`)
  by adding the missing attribute after its own argument parsing.
- Accepts several --file values and runs the generator once per file, one
  after another, so only one CSV is held in memory at a time.

All other flags are passed through unchanged:
    python tools/run_generator.py --file data/202606-citibike-tripdata_*.csv \
        --broker localhost:19092 --burst 1000 --interval 0.01
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import event_generator  # noqa: E402

_original_parse_args = event_generator.parse_args


def _parse_args_with_brokers():
    args = _original_parse_args()
    args.brokers = args.broker
    return args


event_generator.parse_args = _parse_args_with_brokers


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", nargs="+", required=True, help="One or more Citibike CSV files")
    args, passthrough = parser.parse_known_args()

    for i, path in enumerate(args.file, start=1):
        print(f"=== [{i}/{len(args.file)}] {path}", flush=True)
        sys.argv = ["event_generator.py", "--file", path, *passthrough]
        event_generator.main()


if __name__ == "__main__":
    main()
