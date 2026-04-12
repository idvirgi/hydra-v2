import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "brain"))

from decision_engine import (  # noqa: E402
    decision_candidate_rows,
    decision_status_rows,
    ensure_intelligence_schema,
)
from hydra_runtime import (  # noqa: E402
    blocked_or_dlq_items,
    circuit_state,
    current_daily_cost,
    db_connect,
    ensure_schema,
    queue_metrics,
    recent_attempts,
    redis_client,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Show HYDRA autonomy/runtime status.")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    cache = redis_client()
    conn = db_connect()
    ensure_schema(conn)
    ensure_intelligence_schema(conn)

    print(f"DAILY_COST_USD={current_daily_cost(cache):.6f}")
    for provider in ("openrouter", "etsy"):
        state = circuit_state(cache, provider)
        print(f"CIRCUIT_{provider.upper()}={json.dumps(state or {'open': False})}")

    print("QUEUE_METRICS_BEGIN")
    for row in queue_metrics(conn):
        print(json.dumps(row, default=str))
    print("QUEUE_METRICS_END")

    print("TERMINAL_ITEMS_BEGIN")
    for row in blocked_or_dlq_items(conn):
        print(json.dumps(row, default=str))
    print("TERMINAL_ITEMS_END")

    print("RECENT_ATTEMPTS_BEGIN")
    for row in recent_attempts(conn, limit=20):
        print(json.dumps(row, default=str))
    print("RECENT_ATTEMPTS_END")

    print("DECISION_CYCLES_BEGIN")
    for row in decision_status_rows(conn, limit=10):
        print(json.dumps(row, default=str))
    print("DECISION_CYCLES_END")

    print("RECENT_CANDIDATES_BEGIN")
    for row in decision_candidate_rows(conn, limit=15):
        print(json.dumps(row, default=str))
    print("RECENT_CANDIDATES_END")

    if args.strict:
        terminal = blocked_or_dlq_items(conn)
        if terminal:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
