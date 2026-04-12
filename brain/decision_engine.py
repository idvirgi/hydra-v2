import datetime as dt
import json
import math
import re
import statistics
import time
import uuid
from collections import Counter
from typing import Any
from urllib import error, request

from psycopg.types.json import Jsonb

from hydra_runtime import (
    CACHE_TTL_SECONDS,
    DEBATE_THRESHOLD_SCORE,
    HYDRA_MAX_BUILD_BACKLOG,
    HydraClassifiedError,
    MODELS,
    MAX_TOKENS_SCOUT,
    MAX_TOKENS_DEBATE,
    TEMPERATURE_SCOUT,
    TEMPERATURE_DEBATE,
    chat_json,
    current_daily_cost,
    enqueue_work_item,
    iso_utc,
    lane_backlog,
    now_utc,
    read_env,
)


TOKEN_RE = re.compile(r"[a-z0-9]+")
STOPWORDS = {
    "and",
    "for",
    "the",
    "with",
    "your",
    "you",
    "from",
    "into",
    "that",
    "this",
    "printable",
    "printables",
    "digital",
    "download",
    "template",
    "templates",
}

APIFY_ETSY_ACTOR_ID = read_env("APIFY_ETSY_ACTOR_ID", "crawlerbros~etsy-scraper")
HYDRA_MARKET_SNAPSHOT_TTL_HOURS = int(read_env("HYDRA_MARKET_SNAPSHOT_TTL_HOURS", "48"))
HYDRA_SCOUT_CANDIDATE_COUNT = int(read_env("HYDRA_SCOUT_CANDIDATE_COUNT", "8"))
HYDRA_SCOUT_CANDIDATE_COUNT_CHEAP = int(read_env("HYDRA_SCOUT_CANDIDATE_COUNT_CHEAP", "5"))
HYDRA_SHORTLIST_SIZE = int(read_env("HYDRA_SHORTLIST_SIZE", "3"))
HYDRA_BUILD_HANDOFF_LIMIT = int(read_env("HYDRA_BUILD_HANDOFF_LIMIT", "1"))
HYDRA_EXPLORE_MIN_COUNT = int(read_env("HYDRA_EXPLORE_MIN_COUNT", "1"))
HYDRA_APIFY_MAX_ITEMS = int(read_env("HYDRA_APIFY_MAX_ITEMS", "8"))
HYDRA_APIFY_MAX_ITEMS_CHEAP = int(read_env("HYDRA_APIFY_MAX_ITEMS_CHEAP", "5"))
HYDRA_MODEL_ANALYSIS_LIMIT = int(read_env("HYDRA_MODEL_ANALYSIS_LIMIT", "4"))
HYDRA_MODEL_ANALYSIS_LIMIT_CHEAP = int(read_env("HYDRA_MODEL_ANALYSIS_LIMIT_CHEAP", "2"))
HYDRA_MIN_COMPOSITE_SCORE = float(read_env("HYDRA_MIN_COMPOSITE_SCORE", "60"))
HYDRA_MIN_EXPLORE_SCORE = float(read_env("HYDRA_MIN_EXPLORE_SCORE", "58"))
HYDRA_LOW_COST_REMAINING_USD = float(read_env("HYDRA_LOW_COST_REMAINING_USD", "0.9"))
SCOUT_CACHE_KEY = "decision:scout:etsy:v3"


def clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            cleaned = value.replace("$", "").replace(",", "").strip()
            return float(cleaned) if cleaned else default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_key(text: str) -> str:
    lowered = text.strip().lower()
    tokens = TOKEN_RE.findall(lowered)
    return "-".join(tokens)


def tokenize(text: str) -> list[str]:
    return [token for token in TOKEN_RE.findall(text.lower()) if token not in STOPWORDS]


def jaccard_similarity(left: str, right: str) -> float:
    left_tokens = set(tokenize(left))
    right_tokens = set(tokenize(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * fraction))
    return ordered[max(0, min(len(ordered) - 1, index))]


def ensure_intelligence_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_market_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                lineage_id TEXT NOT NULL,
                source TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                query TEXT NOT NULL,
                sample_size INTEGER NOT NULL,
                listing_count_estimate INTEGER,
                listing_count_available BOOLEAN NOT NULL DEFAULT FALSE,
                metrics JSONB NOT NULL,
                raw_items JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS hydra_market_snapshots_query_created_idx
            ON hydra_market_snapshots (query, created_at DESC);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_candidates (
                lineage_id TEXT PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                niche_key TEXT NOT NULL,
                keyword_key TEXT NOT NULL,
                primary_keyword TEXT NOT NULL,
                keyword_family TEXT NOT NULL,
                candidate_payload JSONB NOT NULL,
                grounded_metrics JSONB NOT NULL,
                model_judgment JSONB NOT NULL,
                scores JSONB NOT NULL,
                composite_score DOUBLE PRECISION NOT NULL,
                novelty_score DOUBLE PRECISION NOT NULL,
                strategy_tag TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason_codes JSONB NOT NULL,
                shortlist_rank INTEGER,
                handoff_rank INTEGER,
                status TEXT NOT NULL,
                build_status TEXT,
                publish_status TEXT,
                failure_classification TEXT,
                memory_snapshot JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS hydra_candidates_cycle_rank_idx
            ON hydra_candidates (cycle_id, composite_score DESC);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_niche_memory (
                niche_key TEXT PRIMARY KEY,
                keyword_family TEXT NOT NULL,
                last_primary_keyword TEXT NOT NULL,
                seen_count INTEGER NOT NULL DEFAULT 0,
                shortlisted_count INTEGER NOT NULL DEFAULT 0,
                handoff_count INTEGER NOT NULL DEFAULT 0,
                build_success_count INTEGER NOT NULL DEFAULT 0,
                build_failure_count INTEGER NOT NULL DEFAULT 0,
                publish_success_count INTEGER NOT NULL DEFAULT 0,
                publish_failure_count INTEGER NOT NULL DEFAULT 0,
                blocked_external_count INTEGER NOT NULL DEFAULT 0,
                avg_composite_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                avg_demand_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                avg_competition_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_seen_at TIMESTAMPTZ,
                last_decision_at TIMESTAMPTZ,
                last_outcome_at TIMESTAMPTZ,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hydra_keyword_memory (
                keyword_key TEXT PRIMARY KEY,
                keyword TEXT NOT NULL,
                keyword_family TEXT NOT NULL,
                seen_count INTEGER NOT NULL DEFAULT 0,
                shortlisted_count INTEGER NOT NULL DEFAULT 0,
                handoff_count INTEGER NOT NULL DEFAULT 0,
                build_success_count INTEGER NOT NULL DEFAULT 0,
                build_failure_count INTEGER NOT NULL DEFAULT 0,
                publish_success_count INTEGER NOT NULL DEFAULT 0,
                publish_failure_count INTEGER NOT NULL DEFAULT 0,
                blocked_external_count INTEGER NOT NULL DEFAULT 0,
                avg_composite_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                avg_demand_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                avg_competition_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_seen_at TIMESTAMPTZ,
                last_decision_at TIMESTAMPTZ,
                last_outcome_at TIMESTAMPTZ,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            );
            """
        )
    conn.commit()


def apify_token() -> str:
    token = read_env("APIFY_API_TOKEN") or read_env("APIFY_API_KEY")
    if not token or "replace" in token.lower():
        raise HydraClassifiedError(
            "APIFY API token is missing; grounded Etsy signal lane is unavailable.",
            "blocked_external",
            blocked=True,
        )
    return token


def decision_mode(cache, conn) -> dict[str, Any]:
    remaining_budget = max(0.0, float(read_env("DAILY_BUDGET_USD", "3.33")) - current_daily_cost(cache))
    backlog = lane_backlog(conn, "build")
    cheap = remaining_budget <= HYDRA_LOW_COST_REMAINING_USD or backlog > 0
    return {
        "name": "cheap" if cheap else "normal",
        "candidate_count": HYDRA_SCOUT_CANDIDATE_COUNT_CHEAP if cheap else HYDRA_SCOUT_CANDIDATE_COUNT,
        "apify_max_items": HYDRA_APIFY_MAX_ITEMS_CHEAP if cheap else HYDRA_APIFY_MAX_ITEMS,
        "analysis_limit": HYDRA_MODEL_ANALYSIS_LIMIT_CHEAP if cheap else HYDRA_MODEL_ANALYSIS_LIMIT,
        "remaining_budget_usd": remaining_budget,
    }


def recent_memory_prompt_context(conn) -> dict[str, list[str]]:
    context = {"successful_families": [], "overused_keywords": []}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT keyword_family
            FROM hydra_niche_memory
            WHERE publish_success_count > 0 OR build_success_count > 0
            ORDER BY publish_success_count DESC, build_success_count DESC, avg_composite_score DESC
            LIMIT 5
            """
        )
        context["successful_families"] = [row["keyword_family"] for row in cur.fetchall() or []]
        cur.execute(
            """
            SELECT keyword
            FROM hydra_keyword_memory
            WHERE seen_count >= 3 AND publish_success_count = 0 AND build_success_count = 0
            ORDER BY seen_count DESC, avg_composite_score ASC
            LIMIT 8
            """
        )
        context["overused_keywords"] = [row["keyword"] for row in cur.fetchall() or []]
    return context


def scout_candidates(openrouter, cache, conn, logger, mode: dict[str, Any], *, force_refresh: bool = False) -> dict[str, Any]:
    cache_key = f"{SCOUT_CACHE_KEY}:{mode['name']}"
    if not force_refresh:
        cached = cache.get(cache_key)
        if cached:
            logger.info("Using cached decision scout candidate pool | mode=%s", mode["name"])
            return json.loads(cached)
    memory_context = recent_memory_prompt_context(conn)
    prompt = (
        "Generate a diverse set of Etsy US digital printable opportunities. "
        f"Return exactly {mode['candidate_count']} candidates in valid JSON only. "
        "Avoid exact keyword duplicates. Focus on downloadable printables only. "
        "Return JSON with this shape only: "
        "{\"generated_at\":\"ISO-8601\",\"candidates\":["
        "{\"niche\":\"string\",\"primary_keyword\":\"string\",\"keyword_family\":\"string\","
        "\"audience\":\"string\",\"product_angle\":\"string\",\"why_now\":\"string\","
        "\"seed_terms\":[\"string\"],\"digital_fit_reason\":\"string\"}"
        "]}. "
        f"Successful keyword families so far: {memory_context['successful_families'] or ['none yet']}. "
        f"Overused weak keywords to avoid repeating too much: {memory_context['overused_keywords'] or ['none yet']}."
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
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list) or not candidates:
        raise HydraClassifiedError("Scout did not return any candidates.", "invalid_payload")
    cache.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(payload))
    logger.info("Generated candidate pool | mode=%s count=%s", mode["name"], len(candidates))
    return payload


def dedupe_candidates(raw_candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    deduped_count = 0
    for candidate in raw_candidates:
        keyword = str(candidate.get("primary_keyword", "")).strip()
        niche = str(candidate.get("niche", "")).strip()
        family = str(candidate.get("keyword_family", "")).strip()
        if not keyword or not niche or not family:
            deduped_count += 1
            continue
        keyword_key = normalize_key(keyword)
        niche_key = normalize_key(niche)
        if not keyword_key or keyword_key in seen or niche_key in seen:
            deduped_count += 1
            continue
        similar = any(
            jaccard_similarity(keyword, existing["primary_keyword"]) >= 0.72
            or normalize_key(existing["keyword_family"]) == normalize_key(family)
            and jaccard_similarity(niche, existing["niche"]) >= 0.7
            for existing in deduped
        )
        if similar:
            deduped_count += 1
            continue
        seen.add(keyword_key)
        seen.add(niche_key)
        deduped.append(
            {
                "niche": niche,
                "primary_keyword": keyword,
                "keyword_family": family,
                "audience": str(candidate.get("audience", "")).strip(),
                "product_angle": str(candidate.get("product_angle", "")).strip(),
                "why_now": str(candidate.get("why_now", "")).strip(),
                "seed_terms": [str(term).strip() for term in candidate.get("seed_terms", []) if str(term).strip()],
                "digital_fit_reason": str(candidate.get("digital_fit_reason", "")).strip(),
                "niche_key": niche_key,
                "keyword_key": keyword_key,
            }
        )
    return deduped, deduped_count


def latest_snapshot(conn, query: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_id, metrics, raw_items, created_at, listing_count_available, listing_count_estimate
            FROM hydra_market_snapshots
            WHERE query=%s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (query,),
        )
        return cur.fetchone()


def fetch_apify_market_snapshot(query: str, max_items: int, logger) -> list[dict[str, Any]]:
    url = f"https://api.apify.com/v2/acts/{APIFY_ETSY_ACTOR_ID}/run-sync-get-dataset-items?token={apify_token()}"
    payload = {
        "startUrls": [{"url": f"https://www.etsy.com/search?q={query.replace(' ', '+')}"}],
        "maxItems": max_items,
    }
    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.time()
    try:
        with request.urlopen(req, timeout=180) as response:
            body = response.read().decode("utf-8", "replace")
            items = json.loads(body)
            logger.info(
                "Apify snapshot fetched | actor=%s query=%s items=%s duration_seconds=%.2f",
                APIFY_ETSY_ACTOR_ID,
                query,
                len(items) if isinstance(items, list) else 0,
                time.time() - started,
            )
            return items if isinstance(items, list) else []
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code in {401, 403}:
            raise HydraClassifiedError(
                f"Apify access blocked: {detail[:300]}",
                "blocked_external",
                blocked=True,
            ) from exc
        if exc.code == 429:
            raise HydraClassifiedError(
                f"Apify rate limited: {detail[:300]}",
                "rate_limit",
                retriable=True,
            ) from exc
        if exc.code >= 500:
            raise HydraClassifiedError(
                f"Apify upstream error: {detail[:300]}",
                "provider_error",
                retriable=True,
            ) from exc
        raise HydraClassifiedError(f"Apify HTTP {exc.code}: {detail[:300]}", "fatal_http_error") from exc
    except Exception as exc:
        raise HydraClassifiedError(str(exc), "provider_timeout", retriable=True) from exc


def compute_market_metrics(query: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    prices = [to_float(item.get("price")) for item in items if to_float(item.get("price")) > 0]
    ratings = [to_float(item.get("rating")) for item in items if to_float(item.get("rating")) > 0]
    reviews = [to_float(item.get("reviewCount")) for item in items if to_float(item.get("reviewCount")) >= 0]
    shops = {str(item.get("shopName", "")).strip().lower() for item in items if str(item.get("shopName", "")).strip()}
    titles = [str(item.get("title", "")).strip() for item in items if str(item.get("title", "")).strip()]
    title_sets = [set(tokenize(title)) for title in titles]
    query_tokens = set(tokenize(query))
    query_coverages = [
        (len(title_tokens & query_tokens) / len(query_tokens) * 100.0) if query_tokens else 0.0
        for title_tokens in title_sets
    ]
    title_counter = Counter(token for title in titles for token in tokenize(title))
    title_token_total = sum(title_counter.values()) or 1
    top_token_share = (title_counter.most_common(1)[0][1] / title_token_total * 100.0) if title_counter else 0.0
    title_diversity = (len({normalize_key(title) for title in titles}) / len(titles) * 100.0) if titles else 0.0
    bestseller_ratio = (
        sum(1 for item in items if bool((item.get("badges") or {}).get("bestseller"))) / len(items) * 100.0
        if items
        else 0.0
    )
    ad_ratio = (
        sum(1 for item in items if bool((item.get("badges") or {}).get("ad"))) / len(items) * 100.0 if items else 0.0
    )
    review_signal = clamp(math.log1p(statistics.mean(reviews) if reviews else 0.0) / math.log(5001) * 100.0)
    price_median = statistics.median(prices) if prices else 0.0
    price_mean = statistics.mean(prices) if prices else 0.0
    price_band_stability = clamp(100.0 - (statistics.pstdev(prices) * 12.0 if len(prices) > 1 else 0.0))
    price_viability = clamp(100.0 - abs(price_median - 4.99) * 12.0) if price_median else 50.0
    shop_diversity = clamp((len(shops) / len(items) * 100.0) if items else 0.0)
    coverage_score = clamp(statistics.mean(query_coverages) if query_coverages else 0.0)
    saturation_score = clamp((top_token_share * 0.4) + (coverage_score * 0.35) + (bestseller_ratio * 0.25))
    demand_score = clamp((review_signal * 0.5) + (bestseller_ratio * 0.2) + (shop_diversity * 0.2) + (price_viability * 0.1))
    competition_pressure = clamp((review_signal * 0.45) + (coverage_score * 0.35) + (saturation_score * 0.20))
    competition_score = clamp(100.0 - competition_pressure)
    beatability_score = clamp((competition_score * 0.55) + (title_diversity * 0.25) + (price_band_stability * 0.20))
    taggability_score = clamp((coverage_score * 0.4) + (title_diversity * 0.4) + ((100.0 - top_token_share) * 0.2))
    metrics = {
        "query": query,
        "sample_size": len(items),
        "listing_count_estimate": None,
        "listing_count_available": False,
        "unavailable_signals": ["listing_count_estimate"],
        "median_price": round(price_median, 2) if price_median else None,
        "average_price": round(price_mean, 2) if price_mean else None,
        "average_rating": round(statistics.mean(ratings), 2) if ratings else None,
        "median_review_count": round(statistics.median(reviews), 2) if reviews else None,
        "average_review_count": round(statistics.mean(reviews), 2) if reviews else None,
        "shop_diversity_ratio": round(shop_diversity, 2),
        "query_title_coverage": round(coverage_score, 2),
        "title_diversity": round(title_diversity, 2),
        "top_token_share": round(top_token_share, 2),
        "bestseller_ratio": round(bestseller_ratio, 2),
        "ad_ratio": round(ad_ratio, 2),
        "price_band_stability": round(price_band_stability, 2),
        "price_viability": round(price_viability, 2),
        "demand_signal": round(demand_score, 2),
        "competition_pressure": round(competition_pressure, 2),
        "competition_signal": round(competition_score, 2),
        "beatability_signal": round(beatability_score, 2),
        "taggability_signal": round(taggability_score, 2),
        "saturation_signal": round(saturation_score, 2),
    }
    return metrics


def get_market_snapshot(conn, candidate: dict[str, Any], logger, mode: dict[str, Any], cycle_id: str) -> dict[str, Any]:
    query = candidate["primary_keyword"]
    existing = latest_snapshot(conn, query)
    if existing and existing["created_at"] >= now_utc() - dt.timedelta(hours=HYDRA_MARKET_SNAPSHOT_TTL_HOURS):
        return {
            "snapshot_id": existing["snapshot_id"],
            "metrics": existing["metrics"],
            "items": existing["raw_items"],
            "source": "apify-cache",
        }
    try:
        items = fetch_apify_market_snapshot(query, mode["apify_max_items"], logger)
    except HydraClassifiedError as exc:
        if existing and existing["created_at"] >= now_utc() - dt.timedelta(days=7):
            metrics = dict(existing["metrics"])
            metrics["stale_snapshot_fallback"] = True
            metrics["fallback_reason"] = exc.classification
            logger.warning(
                "Using stale market snapshot after live fetch failure | query=%s classification=%s snapshot_id=%s",
                query,
                exc.classification,
                existing["snapshot_id"],
            )
            return {
                "snapshot_id": existing["snapshot_id"],
                "metrics": metrics,
                "items": existing["raw_items"],
                "source": "apify-stale-fallback",
            }
        raise
    metrics = compute_market_metrics(query, items)
    snapshot = {
        "snapshot_id": uuid.uuid4().hex,
        "cycle_id": cycle_id,
        "lineage_id": candidate["lineage_id"],
        "source": "apify-live",
        "actor_id": APIFY_ETSY_ACTOR_ID,
        "query": query,
        "sample_size": len(items),
        "listing_count_estimate": None,
        "listing_count_available": False,
        "metrics": metrics,
        "raw_items": items,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hydra_market_snapshots (
                snapshot_id, cycle_id, lineage_id, source, actor_id, query,
                sample_size, listing_count_estimate, listing_count_available,
                metrics, raw_items, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                snapshot["snapshot_id"],
                snapshot["cycle_id"],
                snapshot["lineage_id"],
                snapshot["source"],
                snapshot["actor_id"],
                snapshot["query"],
                snapshot["sample_size"],
                snapshot["listing_count_estimate"],
                snapshot["listing_count_available"],
                Jsonb(snapshot["metrics"]),
                Jsonb(snapshot["raw_items"]),
            ),
        )
    conn.commit()
    return {"snapshot_id": snapshot["snapshot_id"], "metrics": snapshot["metrics"], "items": items, "source": "apify-live"}


def fetch_memory_snapshot(conn, candidate: dict[str, Any]) -> dict[str, Any]:
    snapshot = {
        "niche": {
            "seen_count": 0,
            "shortlisted_count": 0,
            "handoff_count": 0,
            "build_success_count": 0,
            "build_failure_count": 0,
            "publish_success_count": 0,
            "publish_failure_count": 0,
            "blocked_external_count": 0,
            "avg_composite_score": 0.0,
        },
        "keyword": {
            "seen_count": 0,
            "shortlisted_count": 0,
            "handoff_count": 0,
            "build_success_count": 0,
            "build_failure_count": 0,
            "publish_success_count": 0,
            "publish_failure_count": 0,
            "blocked_external_count": 0,
            "avg_composite_score": 0.0,
        },
    }
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM hydra_niche_memory WHERE niche_key=%s", (candidate["niche_key"],))
        niche = cur.fetchone()
        if niche:
            snapshot["niche"].update(niche)
        cur.execute("SELECT * FROM hydra_keyword_memory WHERE keyword_key=%s", (candidate["keyword_key"],))
        keyword = cur.fetchone()
        if keyword:
            snapshot["keyword"].update(keyword)
    return snapshot


def digital_fit_score(candidate: dict[str, Any]) -> float:
    text = " ".join(
        [
            candidate["niche"],
            candidate["primary_keyword"],
            candidate["product_angle"],
            candidate["digital_fit_reason"],
            " ".join(candidate.get("seed_terms", [])),
        ]
    ).lower()
    bonus_terms = ["printable", "planner", "checklist", "tracker", "template", "worksheet", "pdf", "bundle"]
    physical_red_flags = ["shirt", "mug", "sticker", "necklace", "physical", "shipping"]
    bonus = sum(12 for term in bonus_terms if term in text)
    penalty = sum(18 for term in physical_red_flags if term in text)
    return clamp(55 + bonus - penalty)


def model_analyze_candidate(openrouter, cache, candidate: dict[str, Any], mode: dict[str, Any]) -> dict[str, Any]:
    prompt = (
        "You are validating an Etsy digital printable opportunity using grounded public market signals and HYDRA business memory. "
        "Return valid JSON only with this shape: "
        "{\"judgment_score\":0,\"confidence\":0,\"recommended_action\":\"prioritize|monitor|reject\","
        "\"bull_case\":[\"string\"],\"bear_case\":[\"string\"],\"reason_codes\":[\"string\"],"
        "\"risk_summary\":\"string\"}. "
        f"Candidate: {json.dumps(candidate, ensure_ascii=True)}"
    )
    return chat_json(
        openrouter,
        cache,
        MODELS["debate"],
        prompt,
        temperature=TEMPERATURE_DEBATE,
        max_tokens=MAX_TOKENS_DEBATE,
        lane="analysis",
    )


def score_candidate(candidate: dict[str, Any], mode: dict[str, Any]) -> dict[str, Any]:
    metrics = candidate["grounded_metrics"]
    memory = candidate["memory_snapshot"]
    niche_memory = memory["niche"]
    keyword_memory = memory["keyword"]
    model = candidate["model_judgment"]

    novelty_penalty = (
        niche_memory.get("seen_count", 0) * 9
        + niche_memory.get("handoff_count", 0) * 12
        + keyword_memory.get("seen_count", 0) * 6
    )
    novelty_bonus = 18 if niche_memory.get("publish_success_count", 0) == 0 else -8
    novelty_score = clamp(88 + novelty_bonus - novelty_penalty)

    history_bonus = (
        niche_memory.get("build_success_count", 0) * 4
        + niche_memory.get("publish_success_count", 0) * 8
        + max(float(niche_memory.get("avg_composite_score", 0.0)) - 70.0, 0.0) * 0.2
    )
    history_penalty = (
        niche_memory.get("build_failure_count", 0) * 5
        + niche_memory.get("publish_failure_count", 0) * 9
        + niche_memory.get("blocked_external_count", 0) * 3
    )
    model_score = to_float(model.get("judgment_score"), 50.0)
    model_confidence = to_float(model.get("confidence"), 50.0 if model.get("status") == "skipped_due_cost" else 60.0)
    operational_viability_score = clamp(
        metrics["price_viability"] * 0.35
        + digital_fit_score(candidate) * 0.35
        + metrics["taggability_signal"] * 0.15
        + model_score * 0.15
    )
    publishability_score = clamp(
        digital_fit_score(candidate) * 0.45
        + metrics["price_band_stability"] * 0.25
        + metrics["taggability_signal"] * 0.15
        + (100.0 - metrics["ad_ratio"]) * 0.15
    )
    confidence_score = clamp(
        metrics["query_title_coverage"] * 0.25
        + min(metrics["sample_size"] * 12.5, 100.0) * 0.25
        + model_confidence * 0.30
        + max(100.0 - history_penalty, 20.0) * 0.20
    )
    composite_score = clamp(
        metrics["demand_signal"] * 0.22
        + metrics["competition_signal"] * 0.18
        + metrics["beatability_signal"] * 0.18
        + novelty_score * 0.12
        + operational_viability_score * 0.12
        + publishability_score * 0.10
        + confidence_score * 0.08
        + min(history_bonus, 18.0)
        - min(history_penalty, 24.0)
        + (model_score - 50.0) * 0.08
    )

    strategy_tag = "explore"
    if niche_memory.get("publish_success_count", 0) > 0 or niche_memory.get("build_success_count", 0) > 0:
        strategy_tag = "exploit"
    elif niche_memory.get("seen_count", 0) >= 2 and float(niche_memory.get("avg_composite_score", 0.0)) >= 72.0:
        strategy_tag = "exploit"

    reason_codes: list[str] = []
    if metrics["demand_signal"] >= 65:
        reason_codes.append("grounded_demand_strong")
    if metrics["competition_signal"] >= 55:
        reason_codes.append("competition_opening")
    if metrics["beatability_signal"] >= 58:
        reason_codes.append("beatability_viable")
    if novelty_score >= 65:
        reason_codes.append("novelty_high")
    if publishability_score >= 60:
        reason_codes.append("publishability_viable")
    if strategy_tag == "exploit":
        reason_codes.append("exploit_history")
    else:
        reason_codes.append("explore_slot_candidate")
    if history_penalty >= 12:
        reason_codes.append("history_penalty")
    if metrics["listing_count_available"] is False:
        reason_codes.append("listing_count_unavailable")
    for code in model.get("reason_codes", []) if isinstance(model.get("reason_codes"), list) else []:
        normalized = normalize_key(str(code).replace("_", "-")).replace("-", "_")
        if normalized:
            reason_codes.append(normalized)

    scores = {
        "demand_score": round(metrics["demand_signal"], 2),
        "competition_score": round(metrics["competition_signal"], 2),
        "beatability_score": round(metrics["beatability_signal"], 2),
        "novelty_score": round(novelty_score, 2),
        "operational_viability_score": round(operational_viability_score, 2),
        "publishability_score": round(publishability_score, 2),
        "confidence_score": round(confidence_score, 2),
        "model_judgment_score": round(model_score, 2),
        "history_bonus": round(min(history_bonus, 18.0), 2),
        "history_penalty": round(min(history_penalty, 24.0), 2),
        "composite_score": round(composite_score, 2),
    }
    return {
        "scores": scores,
        "composite_score": scores["composite_score"],
        "novelty_score": scores["novelty_score"],
        "strategy_tag": strategy_tag,
        "reason_codes": sorted(set(reason_codes)),
    }


def shortlist_candidates(candidates: list[dict[str, Any]], build_backlog: int) -> dict[str, Any]:
    ordered = sorted(candidates, key=lambda item: item["composite_score"], reverse=True)
    explore_candidates = [item for item in ordered if item["strategy_tag"] == "explore" and item["composite_score"] >= HYDRA_MIN_EXPLORE_SCORE]
    shortlisted: list[dict[str, Any]] = []
    theme_counts: Counter[str] = Counter()

    def can_add(candidate: dict[str, Any]) -> bool:
        theme = candidate["theme_key"]
        if theme_counts[theme] >= 1:
            return False
        return all(jaccard_similarity(candidate["primary_keyword"], picked["primary_keyword"]) < 0.68 for picked in shortlisted)

    explore_target = min(HYDRA_EXPLORE_MIN_COUNT, HYDRA_SHORTLIST_SIZE, len(explore_candidates))
    for candidate in explore_candidates:
        if len([item for item in shortlisted if item["strategy_tag"] == "explore"]) >= explore_target:
            break
        if can_add(candidate):
            shortlisted.append(candidate)
            theme_counts[candidate["theme_key"]] += 1

    for candidate in ordered:
        if candidate in shortlisted:
            continue
        if len(shortlisted) >= HYDRA_SHORTLIST_SIZE:
            break
        if can_add(candidate):
            shortlisted.append(candidate)
            theme_counts[candidate["theme_key"]] += 1

    for candidate in ordered:
        if len(shortlisted) >= HYDRA_SHORTLIST_SIZE:
            break
        if candidate in shortlisted:
            continue
        if all(jaccard_similarity(candidate["primary_keyword"], picked["primary_keyword"]) < 0.82 for picked in shortlisted):
            shortlisted.append(candidate)

    shortlist_scores = [candidate["composite_score"] for candidate in shortlisted]
    dynamic_threshold = max(
        HYDRA_MIN_COMPOSITE_SCORE,
        percentile(shortlist_scores, 0.5) - 4.0 if shortlist_scores else HYDRA_MIN_COMPOSITE_SCORE,
    )
    handoff_limit = max(0, min(HYDRA_BUILD_HANDOFF_LIMIT, max(HYDRA_MAX_BUILD_BACKLOG - build_backlog, 0)))
    handoff_candidates = [
        candidate
        for candidate in shortlisted
        if candidate["composite_score"] >= dynamic_threshold
    ][:handoff_limit]

    for index, candidate in enumerate(shortlisted, start=1):
        candidate["shortlist_rank"] = index
        candidate["decision"] = "shortlist"
    for index, candidate in enumerate(handoff_candidates, start=1):
        candidate["handoff_rank"] = index
        candidate["decision"] = "build_handoff"
        candidate["status"] = "queued_for_build"

    for candidate in ordered:
        if candidate in handoff_candidates:
            continue
        if candidate in shortlisted:
            candidate["status"] = "shortlisted"
        else:
            candidate["status"] = "rejected"
            candidate["decision"] = "reject"
            if candidate["composite_score"] >= HYDRA_MIN_COMPOSITE_SCORE:
                candidate["reason_codes"].append("portfolio_constraint")
            else:
                candidate["reason_codes"].append("composite_below_threshold")

    return {
        "ordered": ordered,
        "shortlisted": shortlisted,
        "handoff": handoff_candidates,
        "dynamic_threshold": round(dynamic_threshold, 2),
    }


def record_candidate_decision(conn, candidate: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hydra_candidates (
                lineage_id, cycle_id, niche_key, keyword_key, primary_keyword, keyword_family,
                candidate_payload, grounded_metrics, model_judgment, scores, composite_score,
                novelty_score, strategy_tag, decision, reason_codes, shortlist_rank, handoff_rank,
                status, build_status, publish_status, failure_classification, memory_snapshot,
                created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (lineage_id)
            DO UPDATE SET
                candidate_payload=EXCLUDED.candidate_payload,
                grounded_metrics=EXCLUDED.grounded_metrics,
                model_judgment=EXCLUDED.model_judgment,
                scores=EXCLUDED.scores,
                composite_score=EXCLUDED.composite_score,
                novelty_score=EXCLUDED.novelty_score,
                strategy_tag=EXCLUDED.strategy_tag,
                decision=EXCLUDED.decision,
                reason_codes=EXCLUDED.reason_codes,
                shortlist_rank=EXCLUDED.shortlist_rank,
                handoff_rank=EXCLUDED.handoff_rank,
                status=EXCLUDED.status,
                memory_snapshot=EXCLUDED.memory_snapshot,
                updated_at=NOW()
            """,
            (
                candidate["lineage_id"],
                candidate["cycle_id"],
                candidate["niche_key"],
                candidate["keyword_key"],
                candidate["primary_keyword"],
                candidate["keyword_family"],
                Jsonb(candidate["candidate_payload"]),
                Jsonb(candidate["grounded_metrics"]),
                Jsonb(candidate["model_judgment"]),
                Jsonb(candidate["scores"]),
                candidate["composite_score"],
                candidate["novelty_score"],
                candidate["strategy_tag"],
                candidate["decision"],
                Jsonb(candidate["reason_codes"]),
                candidate.get("shortlist_rank"),
                candidate.get("handoff_rank"),
                candidate["status"],
                candidate.get("build_status"),
                candidate.get("publish_status"),
                candidate.get("failure_classification"),
                Jsonb(candidate["memory_snapshot"]),
            ),
        )
    conn.commit()


def update_memory_table(
    conn,
    table: str,
    key_column: str,
    key_value: str,
    payload: dict[str, Any],
    *,
    keyword_family: str,
    last_value_column: str,
    last_value: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} WHERE {key_column}=%s", (key_value,))
        current = cur.fetchone()
        if current:
            seen_count = int(current["seen_count"]) + 1
            avg_composite = ((float(current["avg_composite_score"]) * int(current["seen_count"])) + payload["scores"]["composite_score"]) / seen_count
            avg_demand = ((float(current["avg_demand_score"]) * int(current["seen_count"])) + payload["scores"]["demand_score"]) / seen_count
            avg_competition = ((float(current["avg_competition_score"]) * int(current["seen_count"])) + payload["scores"]["competition_score"]) / seen_count
            cur.execute(
                f"""
                UPDATE {table}
                SET keyword_family=%s,
                    {last_value_column}=%s,
                    seen_count=%s,
                    shortlisted_count=shortlisted_count + %s,
                    handoff_count=handoff_count + %s,
                    avg_composite_score=%s,
                    avg_demand_score=%s,
                    avg_competition_score=%s,
                    last_seen_at=NOW(),
                    last_decision_at=NOW()
                WHERE {key_column}=%s
                """,
                (
                    keyword_family,
                    last_value,
                    seen_count,
                    1 if payload["decision"] in {"shortlist", "build_handoff"} else 0,
                    1 if payload["decision"] == "build_handoff" else 0,
                    avg_composite,
                    avg_demand,
                    avg_competition,
                    key_value,
                ),
            )
        else:
            columns = [key_column, "keyword_family", last_value_column]
            values = [key_value, keyword_family, last_value]
            cur.execute(
                f"""
                INSERT INTO {table} (
                    {", ".join(columns)},
                    seen_count, shortlisted_count, handoff_count,
                    avg_composite_score, avg_demand_score, avg_competition_score,
                    last_seen_at, last_decision_at
                )
                VALUES ({", ".join(["%s"] * len(values))}, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (
                    *values,
                    1,
                    1 if payload["decision"] in {"shortlist", "build_handoff"} else 0,
                    1 if payload["decision"] == "build_handoff" else 0,
                    payload["scores"]["composite_score"],
                    payload["scores"]["demand_score"],
                    payload["scores"]["competition_score"],
                ),
            )
    conn.commit()


def update_memory_on_decision(conn, candidate: dict[str, Any]) -> None:
    update_memory_table(
        conn,
        "hydra_niche_memory",
        "niche_key",
        candidate["niche_key"],
        candidate,
        keyword_family=candidate["keyword_family"],
        last_value_column="last_primary_keyword",
        last_value=candidate["primary_keyword"],
    )
    update_memory_table(
        conn,
        "hydra_keyword_memory",
        "keyword_key",
        candidate["keyword_key"],
        candidate,
        keyword_family=candidate["keyword_family"],
        last_value_column="keyword",
        last_value=candidate["primary_keyword"],
    )


def decision_telemetry(candidates: list[dict[str, Any]], raw_count: int, deduped_count: int, shortlist: list[dict[str, Any]], handoff: list[dict[str, Any]], mode: dict[str, Any]) -> dict[str, Any]:
    scores = [candidate["composite_score"] for candidate in candidates]
    rejected = [candidate for candidate in candidates if candidate["decision"] == "reject"]
    reason_counter = Counter(code for candidate in candidates for code in candidate["reason_codes"])
    return {
        "candidate_count_raw": raw_count,
        "candidate_count_deduped": len(candidates),
        "dedup_rate": round((deduped_count / raw_count * 100.0), 2) if raw_count else 0.0,
        "shortlist_count": len(shortlist),
        "handoff_count": len(handoff),
        "explore_count": sum(1 for candidate in candidates if candidate["strategy_tag"] == "explore"),
        "exploit_count": sum(1 for candidate in candidates if candidate["strategy_tag"] == "exploit"),
        "score_distribution": {
            "max": round(max(scores), 2) if scores else 0.0,
            "p50": round(percentile(scores, 0.5), 2) if scores else 0.0,
            "min": round(min(scores), 2) if scores else 0.0,
        },
        "reject_reasons": reason_counter.most_common(10),
        "winner_reasons": Counter(code for candidate in shortlist for code in candidate["reason_codes"]).most_common(10),
        "mode": mode,
        "rejected_count": len(rejected),
    }


def persist_candidate_batch(conn, candidates: list[dict[str, Any]]) -> None:
    for candidate in candidates:
        record_candidate_decision(conn, candidate)
        update_memory_on_decision(conn, candidate)


def build_cycle_payload(
    cycle_root: dict[str, Any],
    scout_payload: dict[str, Any],
    candidates: list[dict[str, Any]],
    shortlist_candidates: list[dict[str, Any]],
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    top = candidates[0] if candidates else {"niche": "none", "composite_score": 0}
    shortlist = [
        {
            "lineage_id": candidate["lineage_id"],
            "primary_keyword": candidate["primary_keyword"],
            "composite_score": candidate["composite_score"],
            "strategy_tag": candidate["strategy_tag"],
            "decision": candidate["decision"],
            "reason_codes": candidate["reason_codes"],
        }
        for candidate in shortlist_candidates
    ]
    return {
        "cycle_id": cycle_root["cycle_id"],
        "lineage_id": cycle_root["lineage_id"],
        "scout_payload": scout_payload,
        "top_niche": {
            "niche": top.get("niche"),
            "primary_keyword": top.get("primary_keyword"),
            "composite_score": top.get("composite_score"),
        },
        "debate_payload": {
            "portfolio": shortlist,
            "telemetry": telemetry,
        },
        "decision": "PORTFOLIO_READY" if shortlist else "NO_WINNER",
        "score": int(round(float(top.get("composite_score", 0)))),
        "status": "queued_for_build" if telemetry["handoff_count"] else "scored_without_handoff",
    }


def run_decision_cycle(conn, cache, openrouter, logger, *, enqueue: bool = True, force_refresh: bool = False) -> dict[str, Any]:
    ensure_intelligence_schema(conn)
    mode = decision_mode(cache, conn)
    scout_payload = scout_candidates(openrouter, cache, conn, logger, mode, force_refresh=force_refresh)
    raw_candidates = scout_payload.get("candidates", [])
    deduped_candidates, deduped_count = dedupe_candidates(raw_candidates)
    cycle_root = {"cycle_id": uuid.uuid4().hex, "lineage_id": uuid.uuid4().hex}
    if not deduped_candidates:
        raise HydraClassifiedError("All scout candidates were deduplicated out.", "invalid_payload")

    enriched: list[dict[str, Any]] = []
    for candidate in deduped_candidates:
        enriched_candidate = dict(candidate)
        enriched_candidate["cycle_id"] = cycle_root["cycle_id"]
        enriched_candidate["lineage_id"] = uuid.uuid4().hex
        snapshot = get_market_snapshot(conn, enriched_candidate, logger, mode, cycle_root["cycle_id"])
        enriched_candidate["grounded_metrics"] = {
            **snapshot["metrics"],
            "snapshot_id": snapshot["snapshot_id"],
            "source": snapshot["source"],
        }
        enriched_candidate["market_snapshot_id"] = snapshot["snapshot_id"]
        enriched_candidate["market_items_sample"] = snapshot["items"]
        enriched_candidate["memory_snapshot"] = fetch_memory_snapshot(conn, enriched_candidate)
        enriched_candidate["theme_key"] = normalize_key(enriched_candidate["keyword_family"]) or enriched_candidate["niche_key"]
        enriched_candidate["candidate_payload"] = {
            "schema_version": "hydra.intelligence.v1",
            "cycle_id": cycle_root["cycle_id"],
            "lineage_id": enriched_candidate["lineage_id"],
            "niche": enriched_candidate["niche"],
            "primary_keyword": enriched_candidate["primary_keyword"],
            "keyword_family": enriched_candidate["keyword_family"],
            "audience": enriched_candidate["audience"],
            "product_angle": enriched_candidate["product_angle"],
            "why_now": enriched_candidate["why_now"],
            "seed_terms": enriched_candidate["seed_terms"],
            "digital_fit_reason": enriched_candidate["digital_fit_reason"],
        }
        enriched.append(enriched_candidate)

    heuristic_rank = sorted(
        enriched,
        key=lambda item: (
            item["grounded_metrics"]["demand_signal"],
            item["grounded_metrics"]["competition_signal"],
            item["grounded_metrics"]["beatability_signal"],
        ),
        reverse=True,
    )
    analysis_targets = {candidate["lineage_id"] for candidate in heuristic_rank[: mode["analysis_limit"]]}

    for candidate in enriched:
        if candidate["lineage_id"] in analysis_targets:
            candidate["model_judgment"] = model_analyze_candidate(openrouter, cache, candidate["candidate_payload"] | {"grounded_metrics": candidate["grounded_metrics"], "memory_snapshot": candidate["memory_snapshot"]}, mode)
        else:
            candidate["model_judgment"] = {
                "status": "skipped_due_cost",
                "judgment_score": 50,
                "confidence": 45,
                "recommended_action": "monitor",
                "reason_codes": ["skipped_due_cost"],
                "bull_case": [],
                "bear_case": [],
                "risk_summary": "Skipped model analysis because cycle was operating in cost-aware mode.",
            }
        scored = score_candidate(candidate, mode)
        candidate.update(scored)

    selection = shortlist_candidates(enriched, lane_backlog(conn, "build"))
    ordered = selection["ordered"]
    shortlisted = selection["shortlisted"]
    handoff = selection["handoff"]

    telemetry = decision_telemetry(ordered, len(raw_candidates), deduped_count, shortlisted, handoff, mode)
    cycle_payload = build_cycle_payload(cycle_root, scout_payload, ordered, shortlisted, telemetry)
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
                cycle_payload["cycle_id"],
                cycle_payload["lineage_id"],
                Jsonb(cycle_payload["scout_payload"]),
                Jsonb(cycle_payload["top_niche"]),
                Jsonb(cycle_payload["debate_payload"]),
                cycle_payload["decision"],
                cycle_payload["score"],
                cycle_payload["status"],
            ),
        )
    conn.commit()
    persist_candidate_batch(conn, ordered)

    if enqueue:
        for candidate in handoff:
            payload = {
                "schema_version": "hydra.autonomy.foundation.v1",
                "lineage_id": candidate["lineage_id"],
                "cycle_id": candidate["cycle_id"],
                "lane": "build",
                "queued_at": iso_utc(),
                "niche": {
                    "name": candidate["niche"],
                    "score": candidate["composite_score"],
                    "audience": candidate["audience"],
                    "why_now": candidate["why_now"],
                    "keywords": [candidate["primary_keyword"], *candidate["seed_terms"]][:8],
                    "competition_notes": candidate["grounded_metrics"],
                    "price_band_usd": candidate["grounded_metrics"].get("median_price"),
                },
                "scout": candidate["candidate_payload"],
                "debate": {
                    "decision": "GO",
                    "score": candidate["composite_score"],
                    "reason_codes": candidate["reason_codes"],
                    "scores": candidate["scores"],
                    "strategy_tag": candidate["strategy_tag"],
                    "grounded_metrics": candidate["grounded_metrics"],
                    "model_judgment": candidate["model_judgment"],
                },
            }
            enqueue_work_item(conn, cache, "build", candidate["lineage_id"], candidate["cycle_id"], payload)

    summary = {
        "cycle_id": cycle_root["cycle_id"],
        "mode": mode["name"],
        "candidate_count": len(ordered),
        "shortlist_count": len(shortlisted),
        "handoff_count": len(handoff),
        "dynamic_threshold": selection["dynamic_threshold"],
        "top_candidates": [
            {
                "lineage_id": candidate["lineage_id"],
                "primary_keyword": candidate["primary_keyword"],
                "composite_score": candidate["composite_score"],
                "strategy_tag": candidate["strategy_tag"],
                "decision": candidate["decision"],
                "reason_codes": candidate["reason_codes"],
            }
            for candidate in ordered[: max(HYDRA_SHORTLIST_SIZE, 5)]
        ],
        "telemetry": telemetry,
    }
    logger.info(
        "Decision cycle completed | cycle_id=%s mode=%s candidate_count=%s shortlist=%s handoff=%s",
        summary["cycle_id"],
        summary["mode"],
        summary["candidate_count"],
        summary["shortlist_count"],
        summary["handoff_count"],
    )
    return summary


def record_candidate_feedback(conn, lineage_id: str, lane: str, outcome: str, classification: str | None = None) -> None:
    ensure_intelligence_schema(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM hydra_candidates WHERE lineage_id=%s", (lineage_id,))
        candidate = cur.fetchone()
    if not candidate:
        return

    status = candidate["status"]
    build_status = candidate.get("build_status")
    publish_status = candidate.get("publish_status")
    if lane == "build":
        build_status = outcome
        if outcome == "succeeded":
            status = "build_succeeded"
        elif outcome in {"dead_letter", "blocked_external"}:
            status = "build_failed"
    if lane == "publish":
        publish_status = outcome
        if outcome == "succeeded":
            status = "publish_succeeded"
        elif outcome in {"dead_letter", "blocked_external"}:
            status = "publish_failed"

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE hydra_candidates
            SET status=%s,
                build_status=%s,
                publish_status=%s,
                failure_classification=%s,
                updated_at=NOW()
            WHERE lineage_id=%s
            """,
            (status, build_status, publish_status, classification, lineage_id),
        )
    conn.commit()

    def bump(table: str, key_column: str, key_value: str) -> None:
        field = None
        if lane == "build" and outcome == "succeeded":
            field = "build_success_count"
        elif lane == "build":
            field = "build_failure_count"
        elif lane == "publish" and outcome == "succeeded":
            field = "publish_success_count"
        elif lane == "publish" and outcome == "blocked_external":
            field = "blocked_external_count"
        elif lane == "publish":
            field = "publish_failure_count"
        if not field:
            return
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {table} SET {field} = {field} + 1, last_outcome_at=NOW() WHERE {key_column}=%s",
                (key_value,),
            )
        conn.commit()

    bump("hydra_niche_memory", "niche_key", candidate["niche_key"])
    bump("hydra_keyword_memory", "keyword_key", candidate["keyword_key"])


def decision_status_rows(conn, limit: int = 5) -> list[dict[str, Any]]:
    ensure_intelligence_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cycle_id, decision, score, status, created_at
            FROM hydra_cycles
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall() or []


def decision_candidate_rows(conn, limit: int = 10) -> list[dict[str, Any]]:
    ensure_intelligence_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lineage_id, cycle_id, primary_keyword, composite_score, strategy_tag, decision, reason_codes, created_at
            FROM hydra_candidates
            ORDER BY created_at DESC, composite_score DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall() or []

