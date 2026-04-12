import argparse
import json
import logging
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "brain"))

from decision_engine import ensure_intelligence_schema, run_decision_cycle  # noqa: E402
from hydra_runtime import db_connect, ensure_schema, openrouter_client, redis_client  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run and verify one HYDRA intelligence cycle.")
    parser.add_argument("--enqueue", action="store_true", help="Allow build handoff for shortlisted candidates.")
    parser.add_argument("--force-refresh", action="store_true", help="Bypass scout cache for this verification run.")
    args = parser.parse_args()

    logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logger = logging.getLogger("verify_decision_engine")

    conn = db_connect()
    cache = redis_client()
    openrouter = openrouter_client()

    ensure_schema(conn)
    ensure_intelligence_schema(conn)
    summary = run_decision_cycle(
        conn,
        cache,
        openrouter,
        logger,
        enqueue=args.enqueue,
        force_refresh=args.force_refresh,
        wait_for_lock_seconds=600,
    )

    if summary["candidate_count"] < 3:
        print("FAIL insufficient_candidates")
        return 1
    if summary["shortlist_count"] < 1:
        print("FAIL no_shortlist")
        return 1

    winners = summary["top_candidates"][: summary["shortlist_count"]]
    if not all(candidate["reason_codes"] for candidate in winners):
        print("FAIL missing_reason_codes")
        return 1

    print("PASS decision_cycle")
    print(json.dumps(summary, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
