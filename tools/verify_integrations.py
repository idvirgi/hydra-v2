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


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify HYDRA external integrations.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--n8n-base-url", default="http://127.0.0.1:5678")
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--core-only", action="store_true")
    args = parser.parse_args()

    env_values = parse_env_file(Path(args.env_file))
    failures = 0

    try:
        redis.Redis(host="redis", port=6379, decode_responses=True).ping()
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
            etsy_headers = {
                "x-api-key": env_value(env_values, "ETSY_API_KEY"),
                "Authorization": f"Bearer {env_value(env_values, 'ETSY_ACCESS_TOKEN')}",
            }
            status, payload = http_json(
                f"https://openapi.etsy.com/v3/application/shops/{env_value(env_values, 'ETSY_SHOP_ID')}",
                headers=etsy_headers,
            )
            shop_name = payload.get("shop_name", "") if isinstance(payload, dict) else ""
            print_result("etsy", status == 200, f"status={status} shop={shop_name}")
            if status != 200:
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
