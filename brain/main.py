import argparse
import json
import logging
import os
import time

from decision_engine import ensure_intelligence_schema, run_decision_cycle
from hydra_runtime import (
    DAILY_BUDGET_USD,
    SCOUT_INTERVAL_SECONDS,
    current_daily_cost,
    db_connect,
    ensure_schema,
    openrouter_client,
    redis_client,
    redis_ping,
    verify_openrouter_auth,
)


APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(APP_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, "hydra_brain.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger("hydra_brain")

cache = redis_client()
conn = db_connect()
openrouter = openrouter_client()


def run_once(*, enqueue: bool, force_refresh: bool) -> dict:
    summary = run_decision_cycle(conn, cache, openrouter, logger, enqueue=enqueue, force_refresh=force_refresh)
    logger.info("Decision summary | %s", json.dumps(summary, ensure_ascii=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the HYDRA decision engine.")
    parser.add_argument("--once", action="store_true", help="Run a single decision cycle and exit.")
    parser.add_argument("--no-enqueue", action="store_true", help="Score candidates without pushing build work.")
    parser.add_argument("--force-refresh", action="store_true", help="Bypass scout cache for the current cycle.")
    args = parser.parse_args()

    ensure_schema(conn)
    ensure_intelligence_schema(conn)
    logger.info("HYDRA brain starting up.")
    redis_ping(cache)
    logger.info("Redis connectivity confirmed.")
    verify_openrouter_auth(openrouter, logger)

    while True:
        try:
            daily_cost = current_daily_cost(cache)
            if daily_cost >= DAILY_BUDGET_USD:
                logger.warning(
                    "Daily budget exceeded: $%.6f / $%.2f. Sleeping for 1 hour.",
                    daily_cost,
                    DAILY_BUDGET_USD,
                )
                time.sleep(3600)
                continue
            run_once(enqueue=not args.no_enqueue, force_refresh=args.force_refresh)
            if args.once:
                return
            logger.info("Sleeping for %s seconds before next decision cycle.", SCOUT_INTERVAL_SECONDS)
            time.sleep(SCOUT_INTERVAL_SECONDS)
        except Exception as exc:
            logger.exception("HYDRA loop error: %s", exc)
            if args.once:
                raise
            time.sleep(300)


if __name__ == "__main__":
    main()
