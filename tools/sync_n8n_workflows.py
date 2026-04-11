import argparse
import json
import os
import sys
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any
from urllib import error, request


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


def build_opener() -> request.OpenerDirector:
    return request.build_opener(request.HTTPCookieProcessor(CookieJar()))


def api_request(
    opener: request.OpenerDirector,
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=body, headers=headers, method=method)
    with opener.open(req, timeout=60) as response:
        raw = response.read().decode("utf-8")
        if not raw:
            return None
        return json.loads(raw)


def unwrap_data(payload: Any) -> Any:
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def maybe_setup_owner(
    opener: request.OpenerDirector,
    base_url: str,
    email: str,
    password: str,
) -> None:
    settings = unwrap_data(api_request(opener, "GET", f"{base_url}/rest/settings"))
    setup_required = bool(
        settings
        .get("userManagement", {})
        .get("showSetupOnFirstLoad", False)
    )
    if not setup_required:
        print("N8N_SETUP=skipped")
        return

    payload = {
        "firstName": "Hydra",
        "lastName": "Owner",
        "email": email,
        "password": password,
    }
    api_request(opener, "POST", f"{base_url}/rest/owner/setup", payload)
    print("N8N_SETUP=created")


def login(opener: request.OpenerDirector, base_url: str, email: str, password: str) -> None:
    api_request(
        opener,
        "POST",
        f"{base_url}/rest/login",
        {"emailOrLdapLoginId": email, "password": password},
    )
    print("N8N_LOGIN=ok")


def workflow_payload(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": source["name"],
        "nodes": source["nodes"],
        "connections": source["connections"],
        "settings": source.get("settings", {}),
        "staticData": source.get("staticData"),
        "tags": source.get("tags", []),
        "active": False,
    }


def fetch_workflows(opener: request.OpenerDirector, base_url: str) -> dict[str, dict[str, Any]]:
    response = unwrap_data(api_request(opener, "GET", f"{base_url}/rest/workflows"))
    items = response if isinstance(response, list) else []
    return {item["name"]: item for item in items}


def upsert_workflow(
    opener: request.OpenerDirector,
    base_url: str,
    existing_by_name: dict[str, dict[str, Any]],
    workflow_file: Path,
) -> None:
    source = json.loads(workflow_file.read_text(encoding="utf-8"))
    payload = workflow_payload(source)
    current = existing_by_name.get(payload["name"])

    if current:
        workflow_id = current["id"]
        api_request(opener, "PATCH", f"{base_url}/rest/workflows/{workflow_id}", payload)
        print(f"WORKFLOW_UPDATED={payload['name']}")
    else:
        created = unwrap_data(api_request(opener, "POST", f"{base_url}/rest/workflows", payload))
        workflow_id = created["id"]
        print(f"WORKFLOW_CREATED={payload['name']}")

    api_request(opener, "PATCH", f"{base_url}/rest/workflows/{workflow_id}", {"active": True})
    print(f"WORKFLOW_ACTIVATED={payload['name']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap n8n auth and sync workflows.")
    parser.add_argument("--base-url", default=os.getenv("N8N_BASE_URL", "http://127.0.0.1:5678"))
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--workflow-dir", default="n8n_workflows")
    args = parser.parse_args()

    env_values = parse_env_file(Path(args.env_file))
    owner_email = env_values.get("N8N_OWNER_EMAIL", os.getenv("N8N_OWNER_EMAIL", "")).strip()
    owner_password = env_values.get("N8N_OWNER_PASSWORD", os.getenv("N8N_OWNER_PASSWORD", "")).strip()
    if not owner_email or not owner_password:
        print("Missing N8N owner credentials.", file=sys.stderr)
        return 1

    opener = build_opener()
    try:
        maybe_setup_owner(opener, args.base_url.rstrip("/"), owner_email, owner_password)
        login(opener, args.base_url.rstrip("/"), owner_email, owner_password)
        existing = fetch_workflows(opener, args.base_url.rstrip("/"))
        workflow_dir = Path(args.workflow_dir)
        for workflow_file in sorted(workflow_dir.glob("*.json")):
            upsert_workflow(opener, args.base_url.rstrip("/"), existing, workflow_file)
            existing = fetch_workflows(opener, args.base_url.rstrip("/"))
        return 0
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP_ERROR={exc.code} {details}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"SYNC_ERROR={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
