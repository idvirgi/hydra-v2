import argparse
import base64
import hashlib
import os
import secrets
from pathlib import Path
from urllib.parse import urlencode


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


def is_placeholder(value: str) -> bool:
    lowered = value.strip().lower()
    return not lowered or lowered in {"...", "replace_me"} or "replace" in lowered or "placeholder" in lowered


def parse_scopes(raw: str) -> list[str]:
    scopes: list[str] = []
    for token in raw.replace(",", " ").split():
        scope = token.strip()
        if scope and scope not in scopes:
            scopes.append(scope)
    return scopes


def base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def build_oauth_url(client_id: str, redirect_uri: str, scopes: list[str]) -> str:
    code_verifier = secrets.token_urlsafe(48)
    code_challenge = base64url(hashlib.sha256(code_verifier.encode("utf-8")).digest())
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(scopes),
        "state": code_verifier,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"https://www.etsy.com/oauth/connect?{urlencode(params)}"


def authorize_url(env_file: str) -> int:
    env_values = parse_env_file(Path(env_file))
    client_id = first_env_value(env_values, "ETSY_KEYSTRING", "ETSY_API_KEY")
    redirect_uri = env_value(env_values, "ETSY_REDIRECT_URI")
    scopes = parse_scopes(env_value(env_values, "ETSY_SCOPES", "shops_r listings_r listings_w"))

    if is_placeholder(client_id):
        raise SystemExit("Missing ETSY_KEYSTRING in env.")
    if is_placeholder(redirect_uri):
        raise SystemExit("Missing ETSY_REDIRECT_URI in env.")
    if not scopes:
        raise SystemExit("Missing ETSY_SCOPES in env.")

    print(f"ETSY_OAUTH_URL={build_oauth_url(client_id, redirect_uri, scopes)}")
    print(f"ETSY_REDIRECT_URI={redirect_uri}")
    print(f"ETSY_SCOPES={' '.join(scopes)}")
    print("ETSY_STATE_MODE=pkce_verifier_roundtrip")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare Etsy OAuth for HYDRA.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    authorize = subparsers.add_parser("authorize-url", help="Generate the Etsy OAuth connect URL.")
    authorize.add_argument("--env-file", default=".env")

    args = parser.parse_args()
    if args.command == "authorize-url":
        return authorize_url(args.env_file)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
