import json
import logging
import os
import time

from hydra_runtime import (
    CACHE_TTL_SECONDS,
    DAILY_BUDGET_USD,
    DEBATE_THRESHOLD_SCORE,
    ENABLE_CACHE,
    HYDRA_MAX_BUILD_BACKLOG,
    MAX_TOKENS_DEBATE,
    MAX_TOKENS_SCOUT,
    MODELS,
    SCOUT_INTERVAL_SECONDS,
    TEMPERATURE_DEBATE,
    TEMPERATURE_SCOUT,
    build_cycle_record,
    canonical_build_payload,
    chat_json,
    current_daily_cost,
    db_connect,
    enqueue_work_item,
    ensure_schema,
    lane_backlog,
    openrouter_client,
    read_env,
    redis_client,
    redis_ping,
    store_cycle,
    update_cycle_status,
    verify_openrouter_auth,
)


APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(APP_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=read_env("LOG_LEVEL", "INFO").upper(),
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


def scout_etsy() -> dict:
    cache_key = "scout:etsy"
    if ENABLE_CACHE:
        cached = cache.get(cache_key)
        if cached:
            logger.info("Using cached scout payload from Redis.")
            return json.loads(cached)
    prompt = (
        "Find the top 5 Etsy niches for US digital printables with high demand and low competition. "
        "Prioritize niches that are realistic for a lean solo operator. "
        "Return JSON with this exact shape only: "
        "{\"generated_at\":\"ISO-8601\",\"niches\":["
        "{\"niche\":\"string\",\"score\":0,\"why_now\":\"string\",\"audience\":\"string\","
        "\"keywords\":[\"string\"],\"competition_notes\":\"string\",\"price_band_usd\":\"string\"}"
        "]}"
    )
    payload = chat_json(
        openrouter,
        cache,
        MODELS["scout"],
        prompt,
        temperature=TEMPERATURE_SCOUT,
        max_tokens=MAX_TOKENS_SCOUT,
        lane="scout",
    )
    niches = payload.get("niches", [])
    if not isinstance(niches, list) or not niches:
        raise ValueError("Scout response did not include any niches.")
    if ENABLE_CACHE:
        cache.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(payload))
        logger.info("Scout payload cached for %s seconds.", CACHE_TTL_SECONDS)
    return payload


def debate_niche(niche: str) -> dict:
    prompt = (
        f"Analyze the Etsy printable niche '{niche}' for the US market as a bull-vs-bear debate. "
        "Return JSON only with this shape: "
        "{\"niche\":\"string\",\"decision\":\"GO or KILL\",\"score\":0,"
        "\"bull_case\":[\"string\"],\"bear_case\":[\"string\"],\"final_reason\":\"string\"}"
    )
    payload = chat_json(
        openrouter,
        cache,
        MODELS["debate"],
        prompt,
        temperature=TEMPERATURE_DEBATE,
        max_tokens=MAX_TOKENS_DEBATE,
        lane="debate",
    )
    if "decision" not in payload or "score" not in payload:
        raise ValueError("Debate response was missing decision or score.")
    return payload


def top_niche_from_scout(payload: dict) -> dict:
    niches = payload.get("niches", [])
    if not niches:
        raise ValueError("Scout payload contained no niches.")

    def rank(item: dict) -> float:
        try:
            return float(item.get("score", 0))
        except (TypeError, ValueError):
            return 0.0

    return sorted(niches, key=rank, reverse=True)[0]


def main() -> None:
    ensure_schema(conn)
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
            scout_payload = scout_etsy()
            top_niche = top_niche_from_scout(scout_payload)
            niche_name = str(top_niche.get("niche", "")).strip()
            if not niche_name:
                raise ValueError("Top niche did not include a niche name.")
            debate_payload = debate_niche(niche_name)
            decision = str(debate_payload.get("decision", "KILL")).upper()
            score = int(float(debate_payload.get("score", 0)))
            cycle_status = "rejected"
            cycle = build_cycle_record(
                scout_payload=scout_payload,
                top_niche=top_niche,
                debate_payload=debate_payload,
                decision=decision,
                score=score,
                status=cycle_status,
            )
            store_cycle(conn, cycle)
            if decision == "GO" and score >= DEBATE_THRESHOLD_SCORE:
                build_backlog = lane_backlog(conn, "build")
                if build_backlog >= HYDRA_MAX_BUILD_BACKLOG:
                    update_cycle_status(conn, cycle["cycle_id"], "backpressure_skipped")
                    logger.warning(
                        "Build backlog limit reached | backlog=%s limit=%s lineage_id=%s",
                        build_backlog,
                        HYDRA_MAX_BUILD_BACKLOG,
                        cycle["lineage_id"],
                    )
                else:
                    payload = canonical_build_payload(cycle, top_niche, debate_payload)
                    item = enqueue_work_item(
                        conn,
                        cache,
                        "build",
                        cycle["lineage_id"],
                        cycle["cycle_id"],
                        payload,
                    )
                    update_cycle_status(conn, cycle["cycle_id"], "queued_for_build")
                    logger.info(
                        "Queued niche for build | lineage_id=%s item_id=%s niche=%s score=%s",
                        cycle["lineage_id"],
                        item["item_id"],
                        niche_name,
                        score,
                    )
            else:
                logger.info(
                    "Niche rejected | lineage_id=%s niche=%s decision=%s score=%s threshold=%s",
                    cycle["lineage_id"],
                    niche_name,
                    decision,
                    score,
                    DEBATE_THRESHOLD_SCORE,
                )
            logger.info("Sleeping for %s seconds before next scout cycle.", SCOUT_INTERVAL_SECONDS)
            time.sleep(SCOUT_INTERVAL_SECONDS)
        except Exception as exc:
            logger.exception("HYDRA loop error: %s", exc)
            time.sleep(300)


if __name__ == "__main__":
    main()
