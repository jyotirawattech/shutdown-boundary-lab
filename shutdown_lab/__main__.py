"""Run the local controlled comparison; never binds an external interface."""
import argparse
import asyncio
import json

from .experiment import run_comparison, stable_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", action="store_true", help="include actual elapsed observations")
    args = parser.parse_args()
    result = asyncio.run(run_comparison())
    print(json.dumps(result if args.observations else stable_result(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
