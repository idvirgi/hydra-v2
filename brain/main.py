import datetime
import json
import logging
import os
import time

import redis
from dotenv import load_dotenv
from openai import OpenAI


load_dotenv()

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


def read_env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.lstrip("\ufeff").strip() if isinstance(value, str) else default

client = OpenAI(
    api_key=read_env("OPENROUTER_API_KEY"),
    base_url=read_env("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
    default_headers={
        "HTTP-Referer": "https://github.com/idvirgi/hydra-v2",
        "X-Title": "HYDRA v2",
    },
)
redis_client = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=30,
)

MODELS = {
    "scout": os.getenv("MODEL_SCOUT", "deepseek/deepseek-v3.2-exp"),
    "builder": os.getenv("MODEL_BUILDER", "openai/gpt-4.1-nano"),
    "debate": os.getenv("MODEL_DEBATE", "qwen/qwen3.5-plus-02-15"),
    "backup": os.getenv("MODEL_BACKUP", "google/gemini-2.5-flash-lite"),
}

MODEL_RATES = {
    "deepseek": {"input": 0.028, "output": 0.42},
    "gpt-4.1-nano": {"input": 0.10, "output": 0.40},
    "qwen": {"input": 0.26, "output": 1.56},
}

CACHE_TTL_SECONDS = int(float(os.getenv("CACHE_TTL_HOURS", "48")) * 3600)
ENABLE_CACHE = os.getenv("ENABLE_CACHE", "true").lower() == "true"
DAILY_BUDGET_USD = float(os.getenv("DAILY_BUDGET_USD", "3.33"))
SCOUT_INTERVAL_SECONDS = int(float(os.getenv("SCOUT_INTERVAL_HOURS", "48")) * 3600)
DEBATE_THRESHOLD_SCORE = int(os.getenv("DEBATE_THRESHOLD_SCORE", "75"))
MAX_TOKENS_SCOUT = int(os.getenv("MAX_TOKENS_SCOUT", "4000"))
MAX_TOKENS_DEBATE = int(os.getenv("MAX_TOKENS_DEBATE", "3000"))
TEMPERATURE_SCOUT = float(os.getenv("TEMPERATURE_SCOUT", "0.3"))
TEMPERATURE_DEBATE = float(os.getenv("TEMPERATURE_DEBATE", "0.7"))


def redis_ping() -> None:
    redis_client.ping()


def verify_openrouter_auth() -> None:
    models = client.models.list()
    visible_models = len(getattr(models, "data", []) or [])
    logger.info("OpenRouter authenticated | models_visible=%s", visible_models)


def daily_cost_key() -> str:
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    return f"cost:daily:{today}"


def resolve_model_rates(model_name: str) -> dict[str, float]:
    lowered = model_name.lower()
    if "deepseek" in lowered:
        return MODEL_RATES["deepseek"]
    if "gpt-4.1-nano" in lowered:
        return MODEL_RATES["gpt-4.1-nano"]
    if "qwen" in lowered:
        return MODEL_RATES["qwen"]
    return {"input": 0.0, "output": 0.0}


def cost_tracker(model_name: str, prompt_tokens: int, completion_tokens: int) -> float:
    rates = resolve_model_rates(model_name)
    input_cost = (prompt_tokens / 1_000_000) * rates["input"]
    output_cost = (completion_tokens / 1_000_000) * rates["output"]
    total_cost = round(input_cost + output_cost, 8)
    cost_key = daily_cost_key()
    redis_client.incrbyfloat(cost_key, total_cost)
    redis_client.expire(cost_key, 7 * 24 * 3600)
    redis_client.hincrbyfloat(f"{cost_key}:models", model_name, total_cost)
    redis_client.expire(f"{cost_key}:models", 7 * 24 * 3600)
    logger.info(
        "Cost tracked | model=%s prompt_tokens=%s completion_tokens=%s cost=$%.6f",
        model_name,
        prompt_tokens,
        completion_tokens,
        total_cost,
    )
    return total_cost


def safe_json_load(text: str) -> dict:
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

    raise ValueError(f"Model did not return valid JSON object: {payload[:400]}")


def extract_usage(response) -> tuple[int, int]:
    usage = getattr(response, "usage", None)
    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    return prompt_tokens, completion_tokens


def chat_json(model: str, prompt: str, temperature: float, max_tokens: int) -> dict:
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
        response = client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            messages=messages,
        )
        prompt_tokens, completion_tokens = extract_usage(response)
        cost_tracker(model, prompt_tokens, completion_tokens)
        content = response.choices[0].message.content or "{}"
        try:
            return safe_json_load(content)
        except ValueError:
            logger.warning(
                "Invalid JSON from model | model=%s attempt=%s content_excerpt=%r",
                model,
                attempt,
                content[:400],
            )
            if attempt == 2:
                raise

    raise RuntimeError("chat_json exhausted retry attempts unexpectedly")


def current_daily_cost() -> float:
    raw_value = redis_client.get(daily_cost_key())
    return float(raw_value) if raw_value else 0.0


def scout_etsy() -> dict:
    cache_key = "scout:etsy"
    if ENABLE_CACHE:
        cached = redis_client.get(cache_key)
        if cached:
            logger.info("Using cached scout payload from Redis.")
            return json.loads(cached)

    prompt = (
        "Find the top 5 Etsy niches for US digital printables with high demand and low competition. "
        "Prioritize niches that are realistic for a lean solo operator. "
        "Return JSON with this shape only: "
        "{\"generated_at\":\"ISO-8601\",\"niches\":["
        "{\"niche\":\"string\",\"score\":0,\"why_now\":\"string\",\"audience\":\"string\","
        "\"keywords\":[\"string\"],\"competition_notes\":\"string\",\"price_band_usd\":\"string\"}"
        "]}"
    )
    payload = chat_json(
        model=MODELS["scout"],
        prompt=prompt,
        temperature=TEMPERATURE_SCOUT,
        max_tokens=MAX_TOKENS_SCOUT,
    )

    niches = payload.get("niches", [])
    if not isinstance(niches, list) or not niches:
        raise ValueError("Scout response did not include any niches.")

    if ENABLE_CACHE:
        redis_client.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(payload))
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
        model=MODELS["debate"],
        prompt=prompt,
        temperature=TEMPERATURE_DEBATE,
        max_tokens=MAX_TOKENS_DEBATE,
    )

    if "decision" not in payload or "score" not in payload:
        raise ValueError("Debate response was missing decision or score.")

    return payload


def builder(niche: str) -> dict:
    logger.info("Builder placeholder invoked for niche '%s'.", niche)
    return {"status": "placeholder", "niche": niche}


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
    logger.info("HYDRA brain starting up.")
    redis_ping()
    logger.info("Redis connectivity confirmed.")
    verify_openrouter_auth()

    while True:
        try:
            daily_cost = current_daily_cost()
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

            queue_payload = {
                "niche": niche_name,
                "scout": top_niche,
                "debate": debate_payload,
                "queued_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }

            if decision == "GO" and score >= DEBATE_THRESHOLD_SCORE:
                redis_client.rpush("hydra:build_queue", json.dumps(queue_payload))
                logger.info(
                    "Queued niche '%s' for build. decision=%s score=%s",
                    niche_name,
                    decision,
                    score,
                )
                builder(niche_name)
            else:
                logger.info(
                    "Niche '%s' rejected. decision=%s score=%s threshold=%s",
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
