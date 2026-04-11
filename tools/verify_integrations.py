import argparse
import json
import os
import sys
from http.cookiejar import CookieJar
from pathlib import Path
from urllib import error, parse, request

import redis


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().lstrip("\ufeff")
    return values


def env_value(values: dict[str, str], name: str, default: str = "") -> str:
    return values.get(name, os.getenv(name, default)).strip()


def first_env_value(values: dict[str, str], *names: str) -> str:
    for name in names:
        value = env_value(values, name)
        if value:
            return value
    return ""


def opener_with_cookies() -> request.OpenerDirector:
    return request.build_opener(request.HTTPCookieProcessor(CookieJar()))


def is_placeholder(value: str) -> bool:
    lowered = value.strip().lower()
    return not lowered or lowered in {"...", "replace_me"} or "replace" in lowered or "placeholder" in lowered


def http_json(
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    payload: dict | None = None,
    opener: request.OpenerDirector | None = None,
) -> tuple[int, object]:
    body = None
    merged_headers = {"Accept": "application/json"}
    if headers:
        merged_headers.update(headers)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        merged_headers["Content-Type"] = "application/json"

    req = request.Request(url, data=body, headers=merged_headers, method=method)
    client = opener or request.build_opener()
    with client.open(req, timeout=60) as response:
        raw = response.read().decode("utf-8")
        return response.status, json.loads(raw) if raw else {}


def print_result(name: str, ok: bool, detail: str) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"{status} {name}: {detail}")


def parse_json_string(raw: str) -> dict:
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def get_etsy_state(
    values: dict[str, str],
    redis_client: redis.Redis | None,
) -> dict[str, str | int]:
    state: dict[str, str | int] = {}
    if redis_client is not None:
        try:
            cached = redis_client.get("hydra:etsy:oauth")
            if cached:
                state.update(parse_json_string(cached))
        except Exception:
            pass

    if not state.get("access_token"):
        access_token = env_value(values, "ETSY_ACCESS_TOKEN")
        if not is_placeholder(access_token):
            state["access_token"] = access_token
    if not state.get("refresh_token"):
        refresh_token = env_value(values, "ETSY_REFRESH_TOKEN")
        if not is_placeholder(refresh_token):
            state["refresh_token"] = refresh_token
    if not state.get("shop_id"):
        shop_id = env_value(values, "ETSY_SHOP_ID")
        if not is_placeholder(shop_id):
            state["shop_id"] = shop_id
    if not state.get("shop_name"):
        shop_name = env_value(values, "ETSY_SHOP_NAME")
        if not is_placeholder(shop_name):
            state["shop_name"] = shop_name

    return state


def verify_etsy(
    values: dict[str, str],
    redis_client: redis.Redis | None,
) -> tuple[bool, str]:
    keystring = first_env_value(values, "ETSY_KEYSTRING", "ETSY_API_KEY")
    shared_secret = env_value(values, "ETSY_SHARED_SECRET")
    redirect_uri = env_value(values, "ETSY_REDIRECT_URI")
    scopes = env_value(values, "ETSY_SCOPES")
    etsy_state = get_etsy_state(values, redis_client)
    access_token = str(etsy_state.get("access_token", "")).strip()
    refresh_token = str(etsy_state.get("refresh_token", "")).strip()
    shop_name = str(etsy_state.get("shop_name", "")).strip()
    shop_id = str(etsy_state.get("shop_id", "")).strip()

    missing = []
    if is_placeholder(keystring):
        missing.append("ETSY_KEYSTRING")
    if is_placeholder(shared_secret):
        missing.append("ETSY_SHARED_SECRET")
    if is_placeholder(shop_name):
        missing.append("ETSY_SHOP_NAME")
    if is_placeholder(redirect_uri):
        missing.append("ETSY_REDIRECT_URI")
    if is_placeholder(scopes):
        missing.append("ETSY_SCOPES")
    if missing:
        return False, f"state=READY_PENDING_ETSY_APPROVAL missing_config={','.join(missing)}"

    try:
        ping_status, ping_payload = http_json(
            "https://openapi.etsy.com/v3/application/openapi-ping",
            headers={"x-api-key": keystring},
        )
        app_id = ping_payload.get("application_id", "unknown") if isinstance(ping_payload, dict) else "unknown"
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, (
            "state=READY_PENDING_ETSY_APPROVAL "
            f"app_access={exc.code} detail={body[:200]}"
        )

    discovered_shop_id = shop_id
    try:
        search_status, search_payload = http_json(
            f"https://openapi.etsy.com/v3/application/shops?{parse.urlencode({'shop_name': shop_name, 'limit': 10})}",
            headers={"x-api-key": keystring},
        )
        results = search_payload.get("results", []) if isinstance(search_payload, dict) else []
        exact_match = next(
            (
                item
                for item in results
                if isinstance(item, dict)
                and str(item.get("shop_name", "")).strip().lower() == shop_name.lower()
            ),
            None,
        )
        if exact_match and not discovered_shop_id:
            discovered_shop_id = str(exact_match.get("shop_id", "")).strip()
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, f"state=READY_PENDING_ETSY_APPROVAL shop_lookup={exc.code} detail={body[:200]}"

    if is_placeholder(access_token) and is_placeholder(refresh_token):
        return False, (
            "state=READY_PENDING_ETSY_APPROVAL "
            f"app_access={ping_status} application_id={app_id} token_state=missing shop_id={discovered_shop_id or 'missing'}"
        )

    if not discovered_shop_id:
        return False, "state=READY_PENDING_ETSY_APPROVAL shop_id=missing_after_lookup"

    etsy_headers = {
        "x-api-key": keystring,
        "Authorization": f"Bearer {access_token}",
    }
    try:
        policy_status, _ = http_json(
            f"https://openapi.etsy.com/v3/application/shops/{discovered_shop_id}/policies/return",
            headers=etsy_headers,
        )
        return True, f"state=GREEN app_access={ping_status} token_check={policy_status} shop_id={discovered_shop_id}"
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 401:
            detail = "token_missing_or_invalid"
        elif exc.code == 403:
            detail = "token_scope_or_approval_blocked"
        elif exc.code == 404:
            detail = "shop_not_found"
        else:
            detail = "unexpected_api_error"
        return False, f"state=READY_PENDING_ETSY_APPROVAL token_check={exc.code} detail={detail} body={body[:200]}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify HYDRA external integrations.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--n8n-base-url", default="http://127.0.0.1:5678")
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--core-only", action="store_true")
    args = parser.parse_args()

    env_values = parse_env_file(Path(args.env_file))
    failures = 0
    redis_client: redis.Redis | None = None

    try:
        redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
        redis_client.ping()
        print_result("redis", True, "ping ok")
    except Exception as exc:
        failures += 1
        print_result("redis", False, str(exc))

    try:
        openrouter_headers = {
            "Authorization": f"Bearer {env_value(env_values, 'OPENROUTER_API_KEY')}",
            "HTTP-Referer": "https://github.com/idvirgi/hydra-v2",
            "X-Title": "HYDRA v2",
        }
        status, payload = http_json(
            f"{env_value(env_values, 'OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/models",
            headers=openrouter_headers,
        )
        model_count = len(payload.get("data", [])) if isinstance(payload, dict) else 0
        print_result("openrouter", status == 200, f"status={status} models={model_count}")
        if status != 200:
            failures += 1
    except Exception as exc:
        failures += 1
        print_result("openrouter", False, str(exc))

    if not args.core_only:
        try:
            supabase_backend_key = first_env_value(
                env_values,
                "SUPABASE_SECRET_KEY",
                "SUPABASE_SERVICE_ROLE_KEY",
                "SUPABASE_SERVICE_KEY",
            )
            supabase_headers = {
                "apikey": supabase_backend_key,
                "Authorization": f"Bearer {supabase_backend_key}",
            }
            status, _ = http_json(
                f"{env_value(env_values, 'SUPABASE_URL').rstrip('/')}/rest/v1/",
                headers=supabase_headers,
            )
            print_result("supabase", status < 400, f"status={status}")
            if status >= 400:
                failures += 1
        except Exception as exc:
            failures += 1
            print_result("supabase", False, str(exc))

        try:
            ok, detail = verify_etsy(env_values, redis_client)
            print_result("etsy", ok, detail)
            if not ok:
                failures += 1
        except Exception as exc:
            failures += 1
            print_result("etsy", False, str(exc))

    try:
        session = opener_with_cookies()
        status, _ = http_json(
            f"{args.n8n_base_url.rstrip('/')}/rest/login",
            method="POST",
            payload={
                "emailOrLdapLoginId": env_value(env_values, "N8N_OWNER_EMAIL"),
                "password": env_value(env_values, "N8N_OWNER_PASSWORD"),
            },
            opener=session,
        )
        settings_status, payload = http_json(
            f"{args.n8n_base_url.rstrip('/')}/rest/settings",
            opener=session,
        )
        setup_flag = (
            payload.get("data", {})
            .get("userManagement", {})
            .get("showSetupOnFirstLoad", None)
            if isinstance(payload, dict)
            else None
        )
        ok = status == 200 and settings_status == 200
        print_result("n8n", ok, f"login={status} settings={settings_status} setupRequired={setup_flag}")
        if not ok:
            failures += 1
    except Exception as exc:
        failures += 1
        print_result("n8n", False, str(exc))

    if args.send_telegram and not args.core_only:
        try:
            telegram_body = parse.urlencode(
                {
                    "chat_id": env_value(env_values, "TELEGRAM_CHAT_ID"),
                    "text": "HYDRA integration verification passed",
                }
            ).encode("utf-8")
            req = request.Request(
                f"https://api.telegram.org/bot{env_value(env_values, 'TELEGRAM_BOT_TOKEN')}/sendMessage",
                data=telegram_body,
                method="POST",
            )
            with request.urlopen(req, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
                ok = response.status == 200 and bool(payload.get("ok"))
                print_result("telegram", ok, f"status={response.status}")
                if not ok:
                    failures += 1
        except Exception as exc:
            failures += 1
            print_result("telegram", False, str(exc))

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
