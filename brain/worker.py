import argparse
import json
import logging
import os
import time

from hydra_runtime import (
    DAILY_BUDGET_USD,
    HYDRA_CLAIM_TTL_SECONDS,
    MAX_TOKENS_BUILDER,
    MODELS,
    SCHEMA_VERSION,
    circuit_state,
    claim_next_work_item,
    classify_exception,
    create_etsy_draft_listing,
    current_daily_cost,
    db_connect,
    ensure_schema,
    enqueue_work_item,
    fail_work_item,
    iso_utc,
    lane_backlog,
    load_etsy_state,
    normalize_builder_output,
    open_circuit,
    openrouter_client,
    queue_signal_key,
    read_env,
    redis_client,
    refresh_etsy_token,
    store_draft,
    validate_build_payload,
    validate_publish_payload,
    verify_openrouter_auth,
    chat_json,
    ack_work_item,
    canonical_publish_payload,
    worker_sleep_seconds,
)


APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(APP_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logger(lane: str) -> logging.Logger:
    logger = logging.getLogger(f"hydra_{lane}_worker")
    if logger.handlers:
        return logger
    logger.setLevel(read_env("LOG_LEVEL", "INFO").upper())
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"hydra_{lane}_worker.log"), encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(stream)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def builder_prompt(payload: dict) -> str:
    return (
        "Using this canonical HYDRA build payload, generate an Etsy digital printable listing draft. "
        "Return JSON only with title, description, tags, materials, price_usd, and optional taxonomy_id. "
        f"Payload: {json.dumps(payload, ensure_ascii=True)}"
    )


def process_build_item(conn, cache, openrouter, logger, item: dict) -> None:
    payload = validate_build_payload(item["payload"])
    response = chat_json(
        openrouter,
        cache,
        MODELS["builder"],
        builder_prompt(payload),
        temperature=0.4,
        max_tokens=MAX_TOKENS_BUILDER,
        lane="build",
    )
    draft = normalize_builder_output(payload, response)
    store_draft(
        conn,
        cache,
        draft,
        lineage_id=item["lineage_id"],
        cycle_id=item["cycle_id"],
        source_item_id=item["item_id"],
    )
    publish_payload = canonical_publish_payload(
        draft,
        lineage_id=item["lineage_id"],
        cycle_id=item["cycle_id"],
        parent_item_id=item["item_id"],
    )
    enqueue_work_item(
        conn,
        cache,
        "publish",
        item["lineage_id"],
        item["cycle_id"],
        publish_payload,
        parent_item_id=item["item_id"],
        draft_id=draft["draft_id"],
    )
    ack_work_item(
        conn,
        item,
        response_payload={
            "draft_id": draft["draft_id"],
            "publish_signal_key": queue_signal_key("publish"),
            "schema_version": SCHEMA_VERSION,
        },
    )
    logger.info(
        "Build succeeded | item_id=%s lineage_id=%s draft_id=%s",
        item["item_id"],
        item["lineage_id"],
        draft["draft_id"],
    )


def process_publish_item(conn, cache, logger, item: dict) -> None:
    payload = validate_publish_payload(item["payload"])
    etsy_state = refresh_etsy_token(cache, load_etsy_state(cache))
    response = create_etsy_draft_listing(payload, etsy_state)
    cache.set(
        f"publish:done:{payload['fingerprint']}",
        json.dumps(
            {
                "draft_id": payload["draft_id"],
                "fingerprint": payload["fingerprint"],
                "listing_id": response.get("listing_id"),
                "listing_url": response.get("url") or response.get("listing_url"),
                "published_at": iso_utc(),
                "state": response.get("state", "draft"),
            }
        ),
    )
    ack_work_item(
        conn,
        item,
        response_payload={
            "listing_id": response.get("listing_id"),
            "listing_url": response.get("url") or response.get("listing_url"),
            "state": response.get("state", "draft"),
        },
    )
    logger.info(
        "Publish succeeded | item_id=%s lineage_id=%s draft_id=%s listing_id=%s",
        item["item_id"],
        item["lineage_id"],
        payload["draft_id"],
        response.get("listing_id"),
    )


def run_worker(lane: str) -> int:
    logger = setup_logger(lane)
    cache = redis_client()
    conn = db_connect()
    ensure_schema(conn)
    openrouter = openrouter_client() if lane == "build" else None
    if openrouter is not None:
        verify_openrouter_auth(openrouter, logger)
    logger.info("HYDRA worker starting | lane=%s", lane)
    while True:
        item = None
        try:
            if lane == "build":
                daily_cost = current_daily_cost(cache)
                if daily_cost >= DAILY_BUDGET_USD:
                    logger.warning(
                        "Build worker budget pause | daily_cost=$%.6f budget=$%.2f",
                        daily_cost,
                        DAILY_BUDGET_USD,
                    )
                    time.sleep(300)
                    continue
            provider = "openrouter" if lane == "build" else "etsy"
            provider_circuit = circuit_state(cache, provider)
            if provider_circuit:
                logger.warning("Circuit open | provider=%s reason=%s", provider, provider_circuit.get("reason"))
                time.sleep(60)
                continue
            item = claim_next_work_item(conn, cache, lane, f"{lane}-worker", HYDRA_CLAIM_TTL_SECONDS)
            if not item:
                time.sleep(worker_sleep_seconds())
                continue
            if lane == "build":
                process_build_item(conn, cache, openrouter, logger, item)
            else:
                process_publish_item(conn, cache, logger, item)
        except Exception as exc:
            if not item:
                classification, retriable, blocked, circuit = classify_exception(exc)
                if circuit:
                    open_circuit(cache, circuit, str(exc))
                logger.exception("Worker loop failure before item processing | lane=%s classification=%s", lane, classification)
                time.sleep(worker_sleep_seconds())
                continue
            classification, retriable, blocked, circuit = classify_exception(exc)
            if circuit:
                open_circuit(cache, circuit, str(exc))
            outcome = fail_work_item(
                conn,
                cache,
                item,
                classification=classification,
                message=str(exc),
                retriable=retriable,
                blocked=blocked,
                response_payload={"lane": lane},
            )
            logger.exception(
                "Worker item failure | lane=%s item_id=%s classification=%s outcome=%s",
                lane,
                item["item_id"],
                classification,
                outcome,
            )
            item = None
            time.sleep(5 if retriable else 15)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run HYDRA lane worker.")
    parser.add_argument("--lane", choices=["build", "publish"], required=True)
    parser.add_argument("--healthcheck", action="store_true")
    args = parser.parse_args()
    if args.healthcheck:
        cache = redis_client()
        conn = db_connect()
        ensure_schema(conn)
        cache.ping()
        backlog = lane_backlog(conn, args.lane)
        print(f"HEALTHCHECK_OK lane={args.lane} backlog={backlog}")
        return 0
    return run_worker(args.lane)


if __name__ == "__main__":
    raise SystemExit(main())
