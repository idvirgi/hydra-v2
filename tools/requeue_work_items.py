import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "brain"))

from hydra_runtime import db_connect, ensure_schema, redis_client, requeue_item  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay blocked or dead-letter HYDRA work items.")
    parser.add_argument("item_ids", nargs="+")
    args = parser.parse_args()

    conn = db_connect()
    cache = redis_client()
    ensure_schema(conn)

    failures = 0
    for item_id in args.item_ids:
        ok = requeue_item(conn, cache, item_id)
        print(f"{'REQUEUED' if ok else 'SKIPPED'} {item_id}")
        if not ok:
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
