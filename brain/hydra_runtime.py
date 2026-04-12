import datetime as dt
import json
import logging
import os
import time
import uuid
from typing import Any
from urllib import error, parse, request

import psycopg
import redis
from dotenv import load_dotenv
from openai import OpenAI
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


load_dotenv()

UTC = dt.timezone.utc
SCHEMA_VERSION = "hydra.autonomy.foundation.v1"
QUEUE_SIGNAL_PREFIX = "hydra:signal"
CIRCUIT_PREFIX = "hydra:circuit"
TELEMETRY_PREFIX = "hydra:telemetry"
ETSY_OAUTH_KEY = "hydra:etsy:oauth"


class HydraClassifiedError(Exception):
    def __init__(
        self,
        message: str,
        classification: str,
        *,
        retriable: bool = False,
        blocked: bool = False,
        circuit: str | None = None,
    ) -> None:
        super().__init__(message)
        self.classification = classification
        self.retriable = retriable
        self.blocked = blocked
        self.circuit = circuit


def read_env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.lstrip("\ufeff").strip() if isinstance(value, str) else default


def read_bool(name: str, default: bool) -> bool:
    raw = read_env(name, "true" if default else "false").lower()
    return raw in {"1", "true", "yes", "on"}


def now_utc() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso_utc() -> str:
    return now_utc().isoformat()


def redis_client() -> redis.Redis:
    return redis.Redis(
        host=read_env("REDIS_HOST", "redis"),
        port=int(read_env("REDIS_PORT", "6379")),
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=30,
    )


def db_connect() -> psycopg.Connection:
    return psycopg.connect(
        host=read_env("POSTGRES_HOST", "postgres"),
        port=int(read_env("POSTGRES_PORT", "5432")),
        dbname=read_env("POSTGRES_DB", "hydra_db"),
        user=read_env("POSTGRES_USER", "hydra"),
        password=read_env("POSTGRES_PASSWORD", "hydra_secure_2026"),
        row_factory=dict_row,
    )


def openrouter_client() -> OpenAI:
    return OpenAI(
        api_key=read_env("OPENROUTER_API_KEY"),
        base_url=read_env("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
        default_headers={
            "HTTP-Referer": "https://github.com/idvirgi/hydra-v2",
            "X-Title": "HYDRA v2",
        },
    )


MODELS = {
    "scout": read_env("MODEL_SCOUT", "deepseek/deepseek-v3.2-exp"),
    "builder": read_env("MODEL_BUILDER", "openai/gpt-4.1-nano"),
    "debate": read_env("MODEL_DEBATE", "qwen/qwen3.5-plus-02-15"),
    "backup": read_env("MODEL_BACKUP", "google/gemini-2.5-flash-lite"),
}

MODEL_RATES = {
    "deepseek": {"input": 0.028, "output": 0.42},
    "gpt-4.1-nano": {"input": 0.10, "output": 0.40},
    "qwen": {"input": 0.26, "output": 1.56},
}

ENABLE_CACHE = read_bool("ENABLE_CACHE", True)
ENABLE_COST_TRACKING = read_bool("ENABLE_COST_TRACKING", True)
CACHE_TTL_SECONDS = int(float(read_env("CACHE_TTL_HOURS", "48")) * 3600)
DAILY_BUDGET_USD = float(read_env("DAILY_BUDGET_USD", "3.33"))
SCOUT_INTERVAL_SECONDS = int(float(read_env("SCOUT_INTERVAL_HOURS", "48")) * 3600)
DEBATE_THRESHOLD_SCORE = int(read_env("DEBATE_THRESHOLD_SCORE", "75"))
MAX_TOKENS_SCOUT = int(read_env("MAX_TOKENS_SCOUT", "4000"))
MAX_TOKENS_BUILDER = int(read_env("MAX_TOKENS_BUILDER", "2000"))
MAX_TOKENS_DEBATE = int(read_env("MAX_TOKENS_DEBATE", "3000"))
TEMPERATURE_SCOUT = float(read_env("TEMPERATURE_SCOUT", "0.3"))
TEMPERATURE_DEBATE = float(read_env("TEMPERATURE_DEBATE", "0.7"))
HYDRA_CLAIM_TTL_SECONDS = int(read_env("HYDRA_CLAIM_TTL_SECONDS", "900"))
HYDRA_QUEUE_POLL_SECONDS = int(read_env("HYDRA_QUEUE_POLL_SECONDS", "15"))
HYDRA_BUILD_RETRY_LIMIT = int(read_env("HYDRA_BUILD_RETRY_LIMIT", "3"))
HYDRA_PUBLISH_RETRY_LIMIT = int(read_env("HYDRA_PUBLISH_RETRY_LIMIT", "4"))
HYDRA_BACKOFF_BASE_SECONDS = int(read_env("HYDRA_BACKOFF_BASE_SECONDS", "120"))
HYDRA_OPENROUTER_CIRCUIT_SECONDS = int(read_env("HYDRA_OPENROUTER_CIRCUIT_SECONDS", "300"))
HYDRA_ETSY_CIRCUIT_SECONDS = int(read_env("HYDRA_ETSY_CIRCUIT_SECONDS", "1800"))
HYDRA_MAX_BUILD_BACKLOG = int(read_env("HYDRA_MAX_BUILD_BACKLOG", "25"))
HYDRA_MAX_PUBLISH_BACKLOG = int(read_env("HYDRA_MAX_PUBLISH_BACKLOG", "25"))


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_cycles (
                cycle_id TEXT PRIMARY KEY,
                lineage_id TEXT NOT NULL UNIQUE,
                scout_payload JSONB NOT NULL,
                top_niche JSONB NOT NULL,
                debate_payload JSONB,
                decision TEXT NOT NULL,
                score INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_work_items (
                item_id TEXT PRIMARY KEY,
                lane TEXT NOT NULL,
                lineage_id TEXT NOT NULL,
                cycle_id TEXT,
                draft_id TEXT,
                parent_item_id TEXT,
                status TEXT NOT NULL,
                payload JSONB NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                last_error_class TEXT,
                last_error_message TEXT,
                blocked_reason TEXT,
                next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                claimed_by TEXT,
                claim_expires_at TIMESTAMPTZ,
                last_claimed_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS hydra_work_items_lane_status_idx
            ON hydra_work_items (lane, status, next_attempt_at, created_at);
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS hydra_work_items_lane_lineage_uidx
            ON hydra_work_items (lane, lineage_id);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_drafts (
                draft_id TEXT PRIMARY KEY,
                lineage_id TEXT NOT NULL,
                cycle_id TEXT,
                source_item_id TEXT,
                fingerprint TEXT NOT NULL UNIQUE,
                payload JSONB NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_attempts (
                attempt_id BIGSERIAL PRIMARY KEY,
                item_id TEXT NOT NULL,
                lane TEXT NOT NULL,
                lineage_id TEXT NOT NULL,
                cycle_id TEXT,
                draft_id TEXT,
                attempt_number INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                classification TEXT,
                error_message TEXT,
                response_payload JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    conn.commit()


def redis_ping(client: redis.Redis) -> None:
    client.ping()


def verify_openrouter_auth(client: OpenAI, logger: logging.Logger) -> None:
    models = client.models.list()
    visible_models = len(getattr(models, "data", []) or [])
    logger.info("OpenRouter authenticated | models_visible=%s", visible_models)


def queue_signal_key(lane: str) -> str:
    return f"{QUEUE_SIGNAL_PREFIX}:{lane}"


def daily_cost_key() -> str:
    return f"cost:daily:{now_utc().date().isoformat()}"


def resolve_model_rates(model_name: str) -> dict[str, float]:
    lowered = model_name.lower()
    if "deepseek" in lowered:
        return MODEL_RATES["deepseek"]
    if "gpt-4.1-nano" in lowered:
        return MODEL_RATES["gpt-4.1-nano"]
    if "qwen" in lowered:
        return MODEL_RATES["qwen"]
    return {"input": 0.0, "output": 0.0}


def cost_tracker(
    client: redis.Redis,
    model_name: str,
    prompt_tokens: int,
    completion_tokens: int,
    lane: str,
) -> float:
    rates = resolve_model_rates(model_name)
    input_cost = (prompt_tokens / 1_000_000) * rates["input"]
    output_cost = (completion_tokens / 1_000_000) * rates["output"]
    total_cost = round(input_cost + output_cost, 8)
    if not ENABLE_COST_TRACKING:
        return total_cost
    cost_key = daily_cost_key()
    client.incrbyfloat(cost_key, total_cost)
    client.expire(cost_key, 7 * 24 * 3600)
    client.hincrbyfloat(f"{cost_key}:models", model_name, total_cost)
    client.expire(f"{cost_key}:models", 7 * 24 * 3600)
    client.hincrbyfloat(f"{cost_key}:lanes", lane, total_cost)
    client.expire(f"{cost_key}:lanes", 7 * 24 * 3600)
    return total_cost


def current_daily_cost(client: redis.Redis) -> float:
    raw_value = client.get(daily_cost_key())
    return float(raw_value) if raw_value else 0.0


def safe_json_load(text: str) -> dict[str, Any]:
    payload = (text or "").strip()
    if payload.startswith("```"):
        payload = "\n".join(
            line for line in payload.splitlines() if not line.strip().startswith("```")
        ).strip()
    candidates = [payload]
    object_start = payload.find("{")
    object_end = payload.rfind("}")
    if object_start != -1 and object_end != -1 and object_end > object_start:
        candidates.append(payload[object_start : object_end + 1])
    for candidate in candidates:
        try:
            loaded = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            return loaded
    raise HydraClassifiedError(
        f"Model did not return valid JSON object: {payload[:400]}",
        "provider_invalid_json",
        retriable=True,
        circuit="openrouter",
    )


def extract_usage(response: Any) -> tuple[int, int]:
    usage = getattr(response, "usage", None)
    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    return prompt_tokens, completion_tokens


def chat_json(
    openrouter: OpenAI,
    cache: redis.Redis,
    model: str,
    prompt: str,
    temperature: float,
    max_tokens: int,
    lane: str,
) -> dict[str, Any]:
    messages = [
        {
            "role": "system",
            "content": (
                "You are the HYDRA market intelligence engine. "
                "Return valid JSON only and never wrap it in markdown."
            ),
        },
        {"role": "user", "content": prompt},
    ]
    for attempt in range(1, 3):
        try:
            response = openrouter.chat.completions.create(
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
                messages=messages,
            )
        except Exception as exc:
            message = str(exc)
            lowered = message.lower()
            if "429" in lowered or "rate limit" in lowered:
                raise HydraClassifiedError(
                    message,
                    "rate_limit",
                    retriable=True,
                    circuit="openrouter",
                ) from exc
            if "timed out" in lowered or "timeout" in lowered or "connection" in lowered:
                raise HydraClassifiedError(
                    message,
                    "provider_timeout",
                    retriable=True,
                    circuit="openrouter",
                ) from exc
            raise HydraClassifiedError(
                message,
                "provider_error",
                retriable=True,
                circuit="openrouter",
            ) from exc
        prompt_tokens, completion_tokens = extract_usage(response)
        cost_tracker(cache, model, prompt_tokens, completion_tokens, lane)
        content = response.choices[0].message.content or "{}"
        try:
            return safe_json_load(content)
        except HydraClassifiedError:
            if attempt == 2:
                raise
            time.sleep(2)
    raise HydraClassifiedError(
        "chat_json exhausted retry attempts unexpectedly",
        "provider_error",
        retriable=True,
        circuit="openrouter",
    )


def build_cycle_record(
    scout_payload: dict[str, Any],
    top_niche: dict[str, Any],
    debate_payload: dict[str, Any],
    decision: str,
    score: int,
    status: str,
) -> dict[str, Any]:
    return {
        "cycle_id": uuid.uuid4().hex,
        "lineage_id": uuid.uuid4().hex,
        "scout_payload": scout_payload,
        "top_niche": top_niche,
        "debate_payload": debate_payload,
        "decision": decision,
        "score": score,
        "status": status,
    }


def store_cycle(conn: psycopg.Connection, cycle: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hydra_cycles (
                cycle_id, lineage_id, scout_payload, top_niche, debate_payload,
                decision, score, status, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """,
            (
                cycle["cycle_id"],
                cycle["lineage_id"],
                Jsonb(cycle["scout_payload"]),
                Jsonb(cycle["top_niche"]),
                Jsonb(cycle["debate_payload"]),
                cycle["decision"],
                cycle["score"],
                cycle["status"],
            ),
        )
    conn.commit()


def update_cycle_status(conn: psycopg.Connection, cycle_id: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE hydra_cycles SET status=%s, updated_at=NOW() WHERE cycle_id=%s",
            (status, cycle_id),
        )
    conn.commit()


def canonical_build_payload(
    cycle: dict[str, Any],
    top_niche: dict[str, Any],
    debate_payload: dict[str, Any],
) -> dict[str, Any]:
    niche_name = str(top_niche.get("niche", "")).strip()
    if not niche_name:
        raise HydraClassifiedError("Top niche is missing 'niche'.", "invalid_payload")
    return {
        "schema_version": SCHEMA_VERSION,
        "lineage_id": cycle["lineage_id"],
        "cycle_id": cycle["cycle_id"],
        "lane": "build",
        "queued_at": iso_utc(),
        "niche": {
            "name": niche_name,
            "score": top_niche.get("score"),
            "audience": top_niche.get("audience"),
            "why_now": top_niche.get("why_now"),
            "keywords": top_niche.get("keywords", []),
            "competition_notes": top_niche.get("competition_notes"),
            "price_band_usd": top_niche.get("price_band_usd"),
        },
        "scout": top_niche,
        "debate": debate_payload,
    }


def canonical_publish_payload(
    draft: dict[str, Any],
    lineage_id: str,
    cycle_id: str,
    parent_item_id: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "lineage_id": lineage_id,
        "cycle_id": cycle_id,
        "lane": "publish",
        "parent_item_id": parent_item_id,
        "draft_id": draft["draft_id"],
        "fingerprint": draft["fingerprint"],
        "listing_title": draft["listing_title"],
        "listing_description": draft["listing_description"],
        "listing_tags": draft["listing_tags"],
        "materials": draft["materials"],
        "price_usd": draft["price_usd"],
        "quantity": draft.get("quantity", 999),
        "taxonomy_id": draft.get("taxonomy_id", 1),
        "source_payload": draft["source_payload"],
        "generated_at": draft["generated_at"],
    }


def lane_retry_limit(lane: str) -> int:
    if lane == "build":
        return HYDRA_BUILD_RETRY_LIMIT
    if lane == "publish":
        return HYDRA_PUBLISH_RETRY_LIMIT
    return 3


def enqueue_work_item(
    conn: psycopg.Connection,
    cache: redis.Redis,
    lane: str,
    lineage_id: str,
    cycle_id: str,
    payload: dict[str, Any],
    *,
    parent_item_id: str | None = None,
    draft_id: str | None = None,
    max_attempts: int | None = None,
) -> dict[str, Any]:
    row = {
        "item_id": uuid.uuid4().hex,
        "lane": lane,
        "lineage_id": lineage_id,
        "cycle_id": cycle_id,
        "draft_id": draft_id,
        "parent_item_id": parent_item_id,
        "status": "queued",
        "payload": payload,
        "attempts": 0,
        "max_attempts": max_attempts or lane_retry_limit(lane),
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hydra_work_items (
                item_id, lane, lineage_id, cycle_id, draft_id, parent_item_id, status,
                payload, attempts, max_attempts, next_attempt_at, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s, NOW(), NOW(), NOW())
            ON CONFLICT (lane, lineage_id)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                draft_id = COALESCE(hydra_work_items.draft_id, EXCLUDED.draft_id),
                parent_item_id = COALESCE(hydra_work_items.parent_item_id, EXCLUDED.parent_item_id),
                updated_at = NOW()
            RETURNING item_id, lane, lineage_id, cycle_id, draft_id, parent_item_id, status, attempts, max_attempts, payload
            """,
            (
                row["item_id"],
                row["lane"],
                row["lineage_id"],
                row["cycle_id"],
                row["draft_id"],
                row["parent_item_id"],
                row["status"],
                Jsonb(row["payload"]),
                row["max_attempts"],
            ),
        )
        returned = cur.fetchone()
    conn.commit()
    if returned and returned["status"] == "queued":
        cache.rpush(queue_signal_key(lane), returned["item_id"])
        cache.hincrby(f"{TELEMETRY_PREFIX}:signals", lane, 1)
    return returned or row


def lane_backlog(conn: psycopg.Connection, lane: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS count
            FROM hydra_work_items
            WHERE lane=%s
              AND status IN ('queued', 'processing', 'blocked_external')
            """,
            (lane,),
        )
        row = cur.fetchone()
    return int(row["count"]) if row else 0


def _claim_candidate(
    conn: psycopg.Connection,
    lane: str,
    worker_id: str,
    lease_seconds: int,
    candidate_id: str | None,
) -> dict[str, Any] | None:
    extra = "AND item_id = %(candidate_id)s" if candidate_id else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH candidate AS (
                SELECT item_id
                FROM hydra_work_items
                WHERE lane = %(lane)s
                  AND status = 'queued'
                  AND next_attempt_at <= NOW()
                  {extra}
                ORDER BY next_attempt_at ASC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE hydra_work_items AS item
            SET status = 'processing',
                attempts = item.attempts + 1,
                claimed_by = %(worker_id)s,
                claim_expires_at = NOW() + (%(lease_seconds)s || ' seconds')::interval,
                last_claimed_at = NOW(),
                updated_at = NOW()
            FROM candidate
            WHERE item.item_id = candidate.item_id
            RETURNING item.*;
            """,
            {
                "lane": lane,
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
                "candidate_id": candidate_id,
            },
        )
        row = cur.fetchone()
    conn.commit()
    return row


def recover_stale_claims(
    conn: psycopg.Connection,
    cache: redis.Redis,
    lane: str,
) -> list[str]:
    with conn.cursor() as cur:
        if lane == "publish":
            cur.execute(
                """
                UPDATE hydra_work_items
                SET status = 'blocked_external',
                    blocked_reason = 'ambiguous_publish_recovery',
                    claimed_by = NULL,
                    claim_expires_at = NULL,
                    last_error_class = 'claim_expired',
                    last_error_message = 'Publish claim expired after external side-effect boundary; manual replay required',
                    updated_at = NOW()
                WHERE lane = %s
                  AND status = 'processing'
                  AND claim_expires_at IS NOT NULL
                  AND claim_expires_at < NOW()
                RETURNING item_id;
                """,
                (lane,),
            )
        else:
            cur.execute(
                """
                UPDATE hydra_work_items
                SET status = 'queued',
                    claimed_by = NULL,
                    claim_expires_at = NULL,
                    next_attempt_at = NOW(),
                    last_error_class = COALESCE(last_error_class, 'claim_expired'),
                    last_error_message = COALESCE(last_error_message, 'Claim lease expired before ack'),
                    updated_at = NOW()
                WHERE lane = %s
                  AND status = 'processing'
                  AND claim_expires_at IS NOT NULL
                  AND claim_expires_at < NOW()
                RETURNING item_id;
                """,
                (lane,),
            )
        rows = cur.fetchall() or []
    conn.commit()
    recovered = [row["item_id"] for row in rows]
    if lane != "publish":
        for item_id in recovered:
            cache.rpush(queue_signal_key(lane), item_id)
    return recovered


def claim_next_work_item(
    conn: psycopg.Connection,
    cache: redis.Redis,
    lane: str,
    worker_id: str,
    lease_seconds: int = HYDRA_CLAIM_TTL_SECONDS,
) -> dict[str, Any] | None:
    recover_stale_claims(conn, cache, lane)
    candidate_id = cache.lpop(queue_signal_key(lane))
    claimed = _claim_candidate(conn, lane, worker_id, lease_seconds, candidate_id)
    if claimed:
        return claimed
    return _claim_candidate(conn, lane, worker_id, lease_seconds, None)


def record_attempt(
    conn: psycopg.Connection,
    item: dict[str, Any],
    *,
    outcome: str,
    classification: str | None = None,
    error_message: str | None = None,
    response_payload: dict[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hydra_attempts (
                item_id, lane, lineage_id, cycle_id, draft_id, attempt_number,
                outcome, classification, error_message, response_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                item["item_id"],
                item["lane"],
                item["lineage_id"],
                item.get("cycle_id"),
                item.get("draft_id"),
                item["attempts"],
                outcome,
                classification,
                error_message,
                Jsonb(response_payload or {}),
            ),
        )
    conn.commit()


def ack_work_item(
    conn: psycopg.Connection,
    item: dict[str, Any],
    *,
    response_payload: dict[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE hydra_work_items
            SET status='succeeded',
                claimed_by=NULL,
                claim_expires_at=NULL,
                completed_at=NOW(),
                updated_at=NOW()
            WHERE item_id=%s
            """,
            (item["item_id"],),
        )
    conn.commit()
    record_attempt(conn, item, outcome="succeeded", response_payload=response_payload)


def compute_retry_delay(attempt_number: int, classification: str) -> int:
    base = HYDRA_BACKOFF_BASE_SECONDS
    multiplier = min(max(attempt_number, 1), 6)
    delay = base * multiplier
    if classification == "rate_limit":
        delay = max(delay, 600)
    if classification == "provider_timeout":
        delay = max(delay, 180)
    return delay


def fail_work_item(
    conn: psycopg.Connection,
    cache: redis.Redis,
    item: dict[str, Any],
    *,
    classification: str,
    message: str,
    retriable: bool,
    blocked: bool,
    response_payload: dict[str, Any] | None = None,
) -> str:
    attempts = int(item["attempts"])
    max_attempts = int(item["max_attempts"])
    next_status = "dead_letter"
    next_attempt_at = None
    blocked_reason = None
    if blocked:
        next_status = "blocked_external"
        blocked_reason = classification
    elif retriable and attempts < max_attempts:
        next_status = "queued"
        next_attempt_at = now_utc() + dt.timedelta(seconds=compute_retry_delay(attempts, classification))
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE hydra_work_items
            SET status=%s,
                last_error_class=%s,
                last_error_message=%s,
                blocked_reason=%s,
                claimed_by=NULL,
                claim_expires_at=NULL,
                next_attempt_at=COALESCE(%s, next_attempt_at),
                updated_at=NOW()
            WHERE item_id=%s
            """,
            (
                next_status,
                classification,
                message[:800],
                blocked_reason,
                next_attempt_at,
                item["item_id"],
            ),
        )
    conn.commit()
    record_attempt(
        conn,
        item,
        outcome=next_status,
        classification=classification,
        error_message=message,
        response_payload=response_payload,
    )
    if next_status == "queued":
        cache.rpush(queue_signal_key(item["lane"]), item["item_id"])
    else:
        cache.hincrby(f"{TELEMETRY_PREFIX}:terminal", next_status, 1)
    return next_status


def store_draft(
    conn: psycopg.Connection,
    cache: redis.Redis,
    draft: dict[str, Any],
    *,
    lineage_id: str,
    cycle_id: str,
    source_item_id: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hydra_drafts (
                draft_id, lineage_id, cycle_id, source_item_id, fingerprint, payload, status, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'ready_for_publish', NOW(), NOW())
            ON CONFLICT (draft_id)
            DO UPDATE SET
                fingerprint=EXCLUDED.fingerprint,
                payload=EXCLUDED.payload,
                status='ready_for_publish',
                updated_at=NOW()
            """,
            (
                draft["draft_id"],
                lineage_id,
                cycle_id,
                source_item_id,
                draft["fingerprint"],
                Jsonb(draft),
            ),
        )
    conn.commit()
    cache.setex(f"draft:{draft['draft_id']}", 30 * 24 * 3600, json.dumps(draft))


def validate_build_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise HydraClassifiedError("Build payload must be an object.", "invalid_payload")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise HydraClassifiedError("Build payload schema_version mismatch.", "invalid_payload")
    niche = payload.get("niche")
    if not isinstance(niche, dict) or not str(niche.get("name", "")).strip():
        raise HydraClassifiedError("Build payload is missing niche.name.", "invalid_payload")
    return payload


def validate_publish_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise HydraClassifiedError("Publish payload must be an object.", "invalid_payload")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise HydraClassifiedError("Publish payload schema_version mismatch.", "invalid_payload")
    required = ["draft_id", "listing_title", "listing_description", "fingerprint", "lineage_id"]
    missing = [name for name in required if not str(payload.get(name, "")).strip()]
    if missing:
        raise HydraClassifiedError(
            f"Publish payload missing required fields: {', '.join(missing)}",
            "invalid_payload",
        )
    return payload


def normalize_builder_output(source: dict[str, Any], parsed: dict[str, Any]) -> dict[str, Any]:
    niche = str(source["niche"]["name"]).strip()
    title = str(parsed.get("title") or parsed.get("listing_title") or "").strip()
    description = str(parsed.get("description") or parsed.get("listing_description") or "").strip()
    if not title or not description:
        raise HydraClassifiedError("Builder draft missing title or description.", "invalid_payload")
    tags = parsed.get("tags") or parsed.get("listing_tags") or []
    if not isinstance(tags, list):
        tags = [piece.strip() for piece in str(tags).split(",")]
    materials = parsed.get("materials") or ["digital download"]
    if not isinstance(materials, list):
        materials = [piece.strip() for piece in str(materials).split(",")]
    price = float(parsed.get("price_usd") or parsed.get("price_recommendation_usd") or parsed.get("price") or 4.99)
    draft_id = f"{source['lineage_id']}-draft"
    fingerprint = f"{source['lineage_id']}::{title}".lower()
    fingerprint = "".join(ch if ch.isalnum() else "-" for ch in fingerprint)
    fingerprint = "-".join(part for part in fingerprint.split("-") if part)[:160]
    return {
        "schema_version": SCHEMA_VERSION,
        "draft_id": draft_id,
        "lineage_id": source["lineage_id"],
        "cycle_id": source["cycle_id"],
        "fingerprint": fingerprint,
        "niche": niche,
        "source_payload": source,
        "generated_at": iso_utc(),
        "listing_title": title[:140],
        "listing_description": description,
        "listing_tags": [str(tag).strip() for tag in tags if str(tag).strip()][:13],
        "materials": [str(item).strip() for item in materials if str(item).strip()],
        "price_usd": round(price, 2) if price > 0 else 4.99,
        "quantity": 999,
        "taxonomy_id": int(parsed.get("taxonomy_id") or 1),
    }


def is_placeholder(value: str) -> bool:
    lowered = value.strip().lower()
    return not lowered or lowered in {"...", "replace_me"} or "replace" in lowered or "placeholder" in lowered


def http_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
    form_payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    elif form_payload is not None:
        body = parse.urlencode(form_payload).encode("utf-8")
        request_headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else {}
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        detail = body[:400]
        lowered = detail.lower()
        provider = "etsy" if "etsy.com" in url else "http"
        if exc.code == 429:
            raise HydraClassifiedError(
                f"{provider} rate limit: {detail}",
                "rate_limit",
                retriable=True,
                circuit=provider,
            ) from exc
        if exc.code >= 500:
            raise HydraClassifiedError(
                f"{provider} upstream error: {detail}",
                "provider_error",
                retriable=True,
                circuit=provider,
            ) from exc
        if provider == "etsy" and exc.code in {401, 403}:
            if "api key not found or not active" in lowered or "scope" in lowered or "invalid_grant" in lowered:
                raise HydraClassifiedError(
                    f"Etsy blocked access: {detail}",
                    "blocked_external",
                    blocked=True,
                    circuit="etsy",
                ) from exc
            raise HydraClassifiedError(
                f"Etsy auth failure: {detail}",
                "auth_failure",
                blocked=True,
                circuit="etsy",
            ) from exc
        raise HydraClassifiedError(f"HTTP {exc.code}: {detail}", "fatal_http_error") from exc
    except error.URLError as exc:
        raise HydraClassifiedError(
            str(exc.reason),
            "provider_timeout",
            retriable=True,
            circuit="etsy" if "etsy.com" in url else "openrouter",
        ) from exc


def load_etsy_state(cache: redis.Redis) -> dict[str, Any]:
    state: dict[str, Any] = {}
    cached = cache.get(ETSY_OAUTH_KEY)
    if cached:
        try:
            state = json.loads(cached)
        except json.JSONDecodeError as exc:
            raise HydraClassifiedError(
                f"Stored Etsy OAuth state is invalid JSON: {exc}",
                "invalid_payload",
            ) from exc
    if not state.get("access_token"):
        access_token = read_env("ETSY_ACCESS_TOKEN")
        if not is_placeholder(access_token):
            state["access_token"] = access_token
    if not state.get("refresh_token"):
        refresh_token = read_env("ETSY_REFRESH_TOKEN")
        if not is_placeholder(refresh_token):
            state["refresh_token"] = refresh_token
    if not state.get("shop_id"):
        shop_id = read_env("ETSY_SHOP_ID")
        if not is_placeholder(shop_id):
            state["shop_id"] = shop_id
    state["shop_name"] = state.get("shop_name") or read_env("ETSY_SHOP_NAME")
    state["scope"] = state.get("scope") or read_env("ETSY_SCOPES")
    state["keystring"] = read_env("ETSY_KEYSTRING")
    if is_placeholder(state.get("keystring", "")):
        raise HydraClassifiedError("Missing ETSY_KEYSTRING.", "blocked_external", blocked=True, circuit="etsy")
    return state


def store_etsy_state(cache: redis.Redis, state: dict[str, Any]) -> None:
    cache.set(ETSY_OAUTH_KEY, json.dumps(state))


def refresh_etsy_token(cache: redis.Redis, state: dict[str, Any]) -> dict[str, Any]:
    refresh_token = str(state.get("refresh_token", "")).strip()
    if not refresh_token:
        return state
    expires_at = int(state.get("expires_at") or 0)
    if state.get("access_token") and expires_at and expires_at > int(time.time()) + 300:
        return state
    _, token = http_json(
        "https://api.etsy.com/v3/public/oauth/token",
        method="POST",
        form_payload={
            "grant_type": "refresh_token",
            "client_id": state["keystring"],
            "refresh_token": refresh_token,
        },
    )
    expires_in = int(token.get("expires_in") or 0)
    refreshed = {
        **state,
        "access_token": token["access_token"],
        "refresh_token": token.get("refresh_token") or refresh_token,
        "token_type": token.get("token_type", "Bearer"),
        "scope": token.get("scope") or state.get("scope", ""),
        "expires_at": int(time.time()) + expires_in if expires_in else None,
        "refreshed_at": iso_utc(),
    }
    store_etsy_state(cache, refreshed)
    return refreshed


def create_etsy_draft_listing(payload: dict[str, Any], etsy_state: dict[str, Any]) -> dict[str, Any]:
    access_token = str(etsy_state.get("access_token", "")).strip()
    shop_id = str(etsy_state.get("shop_id", "")).strip()
    if not access_token and not str(etsy_state.get("refresh_token", "")).strip():
        raise HydraClassifiedError(
            "Etsy OAuth is not connected. Complete the prepared callback workflow first.",
            "blocked_external",
            blocked=True,
            circuit="etsy",
        )
    if not shop_id:
        raise HydraClassifiedError(
            "Etsy shop_id is missing from runtime OAuth state.",
            "blocked_external",
            blocked=True,
            circuit="etsy",
        )
    if not access_token:
        raise HydraClassifiedError(
            "Etsy access token is missing after refresh evaluation.",
            "auth_failure",
            blocked=True,
            circuit="etsy",
        )
    _, response = http_json(
        f"https://openapi.etsy.com/v3/application/shops/{shop_id}/listings",
        method="POST",
        headers={
            "x-api-key": etsy_state["keystring"],
            "Authorization": f"Bearer {access_token}",
        },
        form_payload={
            "quantity": str(payload.get("quantity", 999)),
            "title": payload["listing_title"],
            "description": payload["listing_description"],
            "price": str(payload.get("price_usd", 4.99)),
            "who_made": "i_did",
            "when_made": "made_to_order",
            "taxonomy_id": str(payload.get("taxonomy_id", 1)),
            "is_digital": "true",
            "type": "download",
            "should_auto_renew": "false",
            "is_supply": "false",
            "tags": ",".join(payload.get("listing_tags", [])),
            "materials": ",".join(payload.get("materials", [])),
        },
    )
    return response


def open_circuit(cache: redis.Redis, provider: str, reason: str) -> None:
    ttl = HYDRA_ETSY_CIRCUIT_SECONDS if provider == "etsy" else HYDRA_OPENROUTER_CIRCUIT_SECONDS
    cache.setex(
        f"{CIRCUIT_PREFIX}:{provider}",
        ttl,
        json.dumps({"reason": reason[:300], "opened_at": iso_utc(), "ttl_seconds": ttl}),
    )


def circuit_state(cache: redis.Redis, provider: str) -> dict[str, Any] | None:
    raw = cache.get(f"{CIRCUIT_PREFIX}:{provider}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"reason": raw}


def classify_exception(exc: Exception) -> tuple[str, bool, bool, str | None]:
    if isinstance(exc, HydraClassifiedError):
        return exc.classification, exc.retriable, exc.blocked, exc.circuit
    message = str(exc)
    lowered = message.lower()
    if "timeout" in lowered or "timed out" in lowered:
        return "provider_timeout", True, False, None
    return "unexpected_error", False, False, None


def queue_metrics(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                lane,
                status,
                COUNT(*) AS count,
                MIN(created_at) AS oldest_created_at,
                MIN(next_attempt_at) AS oldest_next_attempt_at
            FROM hydra_work_items
            GROUP BY lane, status
            ORDER BY lane, status
            """
        )
        return cur.fetchall() or []


def recent_attempts(conn: psycopg.Connection, limit: int = 20) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT item_id, lane, lineage_id, attempt_number, outcome, classification, error_message, created_at
            FROM hydra_attempts
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall() or []


def blocked_or_dlq_items(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT item_id, lane, lineage_id, status, blocked_reason, last_error_class, last_error_message, updated_at
            FROM hydra_work_items
            WHERE status IN ('blocked_external', 'dead_letter')
            ORDER BY updated_at DESC
            LIMIT 50
            """
        )
        return cur.fetchall() or []


def requeue_item(
    conn: psycopg.Connection,
    cache: redis.Redis,
    item_id: str,
    *,
    allow_statuses: tuple[str, ...] = ("dead_letter", "blocked_external"),
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE hydra_work_items
            SET status='queued',
                blocked_reason=NULL,
                claimed_by=NULL,
                claim_expires_at=NULL,
                next_attempt_at=NOW(),
                updated_at=NOW()
            WHERE item_id=%s
              AND status = ANY(%s)
            RETURNING lane
            """,
            (item_id, list(allow_statuses)),
        )
        row = cur.fetchone()
    conn.commit()
    if not row:
        return False
    cache.rpush(queue_signal_key(row["lane"]), item_id)
    return True


def worker_sleep_seconds() -> int:
    return max(HYDRA_QUEUE_POLL_SECONDS, 5)
