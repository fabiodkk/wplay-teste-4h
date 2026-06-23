import base64
import hashlib
import json
import os
import random
import socket
import string
import threading
import unicodedata
from urllib.parse import parse_qsl, unquote, urlparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json


LOGIN_URL = "https://mcapi.knewcms.com:2087/auth/login"
TEST_URL = "https://mcapi.knewcms.com:2087/lines/test"
RESALE_MOVIE_URL = "https://mcapi.knewcms.com:2087/streams/resale/movie"
RESALE_CANAL_URL = "https://mcapi.knewcms.com:2087/streams/resale/canal"
RESALE_FINDALL_URL = "https://mcapi.knewcms.com:2087/streams/resale/findAll"
REPORT_CONTENT_RESALE_URL = "https://mcapi.knewcms.com:2087/report-content/resale/create"
TMDB_SEARCH_MOVIE_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_SEARCH_TV_URL = "https://api.themoviedb.org/3/search/tv"
TMDB_DEFAULT_BEARER_TOKEN = (
    "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmMGU2ZDExZDllZTg3N2ViZTgyNTFiYmJiMzE3OGI1NSIsIm5iZiI6MTc3OTQwOTk0Mi4wODgsInN1YiI6IjZhMGZhNDE2MTZjNmUzYmIyMTIzNGZmNCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.C2DLCX9AxO1MjTHlEXTJBGgMwZHEgDpFoT7ARGRSZLg"
)
TMDB_DEFAULT_API_KEY = "f0e6d11d9ee877ebe8251bbbb3178b55"
BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")
TOKEN_LOCK_KEY = 92745131
KIRVANO_ACCESS_EVENTS = {"SALE_APPROVED", "SUBSCRIPTION_RENEWED"}
KIRVANO_APPROVED_STATUSES = {"APPROVED", "PAID", "COMPLETED"}
MEMORY_TOKEN_LOCK = threading.Lock()
MEMORY_TOKEN_CACHE = {"token": None, "expires_at": None}
MEMORY_WEBHOOK_LOCK = threading.Lock()
MEMORY_WEBHOOK_RESULTS = {}

LOGIN_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "origin": "https://wwpanel.link",
    "referer": "https://wwpanel.link/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
}

TEST_HEADERS_BASE = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "origin": "https://wwpanel.link",
    "referer": "https://wwpanel.link/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
}

STREAM_HEADERS_BASE = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9,en;q=0.8",
    "origin": "https://wwpanel.link",
    "referer": "https://wwpanel.link/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
}


def rand_suffix(n=8):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def only_digits(value: str) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def mask_phone_for_note(phone: str) -> str:
    digits = only_digits(phone)
    if len(digits) >= 11:
        base = digits[-11:]
        ddd = base[:2]
        first_mobile = base[2]
        last4 = base[-4:]
        return f"{ddd}{first_mobile}xxxx-{last4}"

    if len(digits) >= 4:
        ddd = (digits[:2] if len(digits) >= 2 else "xx")
        first_mobile = digits[2] if len(digits) >= 3 else "9"
        last4 = digits[-4:]
        return f"{ddd}{first_mobile}xxxx-{last4}"

    return "xx9xxxx-xxxx"


def mask_telegram_for_note(telegram_id: str) -> str:
    digits = only_digits(telegram_id)
    if not digits:
        return ""
    return f"tg:xxxx{digits[-4:].rjust(4, 'x')}"


def build_lead_note(phone: str, telegram_id: str) -> str:
    phone_mask = mask_phone_for_note(phone)
    tg_mask = mask_telegram_for_note(telegram_id)
    if tg_mask:
        return f"cap:{phone_mask} | {tg_mask}"
    return f"cap:{phone_mask}"


def get_supabase_project_info(access_token: str, project_name: str) -> dict:
    resp = requests.get(
        "https://api.supabase.com/v1/projects",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    resp.raise_for_status()
    projects = resp.json()
    for project in projects:
        if project.get("name") == project_name:
            return {
                "ref": project.get("id") or project.get("ref"),
                "region": project.get("region") or "",
            }
    names = [project.get("name") for project in projects]
    raise RuntimeError(f"Projeto '{project_name}' nao encontrado. Disponiveis: {names}")


def get_supabase_ref(access_token: str, project_name: str) -> str:
    return get_supabase_project_info(access_token, project_name)["ref"]


def connect_with_ipv4_fallback(conn_kwargs: dict):
    host = conn_kwargs.get("host")
    port = int(conn_kwargs.get("port", 5432))
    ipv4_kwargs = dict(conn_kwargs)
    try:
        info = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        if info:
            ipv4 = info[0][4][0]
            if ipv4:
                ipv4_kwargs["hostaddr"] = ipv4
    except Exception:
        pass

    try:
        return psycopg2.connect(**ipv4_kwargs)
    except psycopg2.OperationalError:
        return psycopg2.connect(**conn_kwargs)


def db_url_to_conn_kwargs(database_url: str) -> dict:
    parsed = urlparse(database_url)
    if not parsed.scheme or not parsed.hostname:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL invalida.")

    query = dict(parse_qsl(parsed.query))
    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or int(query.pop("port", 5432)),
        "dbname": unquote(parsed.path.lstrip("/") or "postgres"),
        "user": unquote(parsed.username or "postgres"),
        "password": unquote(parsed.password or ""),
        "sslmode": query.pop("sslmode", os.getenv("SUPABASE_DB_SSLMODE", "require")),
    }
    kwargs.update(query)
    return kwargs


def connect_db(
    db_password: str,
    project_ref: str,
    project_region: str = "",
    pooler_host_override: str = "",
    pooler_port_override: str = "",
    database_url: str = "",
    db_host_override: str = "",
    db_port_override: str = "",
    db_name_override: str = "",
    db_user_override: str = "",
    db_sslmode: str = "require",
):
    if database_url:
        return connect_with_ipv4_fallback(db_url_to_conn_kwargs(database_url))

    if db_host_override:
        host_user = db_user_override or ("postgres" if db_host_override.startswith("db.") else f"postgres.{project_ref}")
        host_kwargs = {
            "host": db_host_override,
            "port": int(db_port_override or 5432),
            "dbname": db_name_override or "postgres",
            "user": host_user,
            "password": db_password,
            "sslmode": db_sslmode or "require",
        }
        return connect_with_ipv4_fallback(host_kwargs)

    if not project_ref:
        raise RuntimeError(
            "Informe SUPABASE_PROJECT_REF, SUPABASE_DB_HOST ou DATABASE_URL/SUPABASE_DB_URL para conectar ao banco."
        )

    direct_host = f"db.{project_ref}.supabase.co"
    direct_kwargs = {
        "host": direct_host,
        "port": 5432,
        "dbname": "postgres",
        "user": "postgres",
        "password": db_password,
        "sslmode": "require",
    }

    try:
        return connect_with_ipv4_fallback(direct_kwargs)
    except psycopg2.OperationalError as direct_exc:
        if not project_region:
            raise direct_exc

        pooler_hosts = []
        if pooler_host_override:
            pooler_hosts.append(pooler_host_override)
        else:
            pooler_hosts = [
                f"aws-1-{project_region}.pooler.supabase.com",
                f"aws-0-{project_region}.pooler.supabase.com",
            ]
        pooler_ports = []
        if pooler_port_override:
            try:
                pooler_ports.append(int(pooler_port_override))
            except ValueError:
                pass
        if not pooler_ports:
            pooler_ports = [6543, 5432]
        last_exc = direct_exc
        for pooler_host in pooler_hosts:
            for pooler_port in pooler_ports:
                pooler_kwargs = {
                    "host": pooler_host,
                    "port": pooler_port,
                    "dbname": "postgres",
                    "user": f"postgres.{project_ref}",
                    "password": db_password,
                    "sslmode": "require",
                }
                try:
                    return connect_with_ipv4_fallback(pooler_kwargs)
                except psycopg2.OperationalError as pooler_exc:
                    last_exc = pooler_exc

        raise last_exc


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists public.mcapi_events (
              id bigserial primary key,
              created_at timestamptz not null default now(),
              event_type text not null,
              status_code integer,
              bearer_token text,
              request_payload jsonb,
              response_payload jsonb,
              error_message text
            );
            """
        )
        cur.execute("alter table public.mcapi_events add column if not exists client_ip text;")
        cur.execute(
            """
            create table if not exists public.ip_liberados (
              id bigserial primary key,
              ip text not null unique,
              descricao text,
              ativo boolean not null default true,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );
            """
        )
        cur.execute(
            """
            create table if not exists public.mcapi_token_cache (
              id smallint primary key check (id = 1),
              bearer_token text not null,
              expires_at timestamptz,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );
            """
        )
        cur.execute(
            """
            create table if not exists public.mcapi_user_token_cache (
              user_id text primary key,
              bearer_token text not null,
              expires_at timestamptz,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );
            """
        )
        cur.execute(
            """
            create table if not exists public.usuarios_criados_ip (
              id bigserial primary key,
              client_ip text not null,
              username text not null,
              telefone text,
              telegram_id text,
              exp_date timestamptz,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique (client_ip, username)
            );
            """
        )
        cur.execute(
            """
            create table if not exists public.kirvano_webhooks (
              id bigserial primary key,
              webhook_key text not null unique,
              event text not null,
              sale_id text,
              checkout_id text,
              status text,
              customer_name text,
              customer_email text,
              customer_phone text,
              access_username text,
              access_password text,
              access_exp_date timestamptz,
              request_payload jsonb,
              result_payload jsonb,
              processed_at timestamptz,
              error_message text,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            );
            """
        )
        cur.execute(
            "create index if not exists idx_mcapi_events_event_ip_created on public.mcapi_events (event_type, client_ip, created_at desc);"
        )
        cur.execute(
            "create index if not exists idx_usuarios_criados_ip_client_ip on public.usuarios_criados_ip (client_ip, created_at desc);"
        )
        cur.execute(
            "create index if not exists idx_ip_liberados_ip_ativo on public.ip_liberados (ip, ativo);"
        )
        cur.execute(
            "create index if not exists idx_mcapi_user_token_cache_updated on public.mcapi_user_token_cache (updated_at desc);"
        )
        cur.execute(
            "create index if not exists idx_kirvano_webhooks_event_created on public.kirvano_webhooks (event, created_at desc);"
        )
        cur.execute(
            "create index if not exists idx_kirvano_webhooks_sale_id on public.kirvano_webhooks (sale_id);"
        )
    conn.commit()


def load_config():
    load_dotenv()

    mc_username = os.getenv("MC_USERNAME")
    mc_password = os.getenv("MC_PASSWORD")
    supabase_access_token = os.getenv("SUPABASE_ACCESS_TOKEN")
    supabase_project_name = os.getenv("SUPABASE_PROJECT_NAME")
    supabase_db_password = os.getenv("SUPABASE_DB_PASSWORD")
    database_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL") or ""
    supabase_db_host = os.getenv("SUPABASE_DB_HOST", "")
    supabase_db_port = os.getenv("SUPABASE_DB_PORT", "")
    supabase_db_name = os.getenv("SUPABASE_DB_NAME", "postgres")
    supabase_db_user = os.getenv("SUPABASE_DB_USER", "")
    supabase_db_sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")

    if not all([mc_username, mc_password]):
        raise RuntimeError("Variaveis MC_USERNAME/MC_PASSWORD faltando no .env ou no Render.")

    project_ref = os.getenv("SUPABASE_PROJECT_REF")
    project_region = os.getenv("SUPABASE_PROJECT_REGION", "")
    supabase_pooler_host = os.getenv("SUPABASE_POOLER_HOST", "")
    supabase_pooler_port = os.getenv("SUPABASE_POOLER_PORT", "")

    if not database_url and not supabase_db_host and not supabase_db_password:
        raise RuntimeError("Informe SUPABASE_DB_PASSWORD ou DATABASE_URL/SUPABASE_DB_URL no .env ou no Render.")

    if not database_url and not supabase_db_host:
        if not project_ref and supabase_access_token and supabase_project_name:
            try:
                project_info = get_supabase_project_info(supabase_access_token, supabase_project_name)
            except Exception as exc:
                raise RuntimeError(
                    "Nao foi possivel consultar o projeto pelo SUPABASE_ACCESS_TOKEN. "
                    "Configure SUPABASE_PROJECT_REF no Render/.env para nao depender desse token administrativo."
                ) from exc
            if not project_ref:
                project_ref = project_info["ref"]
            if not project_region:
                project_region = project_info.get("region", "")
        elif not project_ref:
            raise RuntimeError(
                "Informe SUPABASE_PROJECT_REF, SUPABASE_DB_HOST ou DATABASE_URL/SUPABASE_DB_URL no .env ou no Render."
            )
        elif not project_region and supabase_access_token and supabase_project_name:
            try:
                project_region = get_supabase_project_info(supabase_access_token, supabase_project_name).get("region", "")
            except Exception:
                project_region = ""

    return {
        "mc_username": mc_username,
        "mc_password": mc_password,
        "project_ref": project_ref,
        "project_region": project_region,
        "supabase_pooler_host": supabase_pooler_host,
        "supabase_pooler_port": supabase_pooler_port,
        "supabase_db_password": supabase_db_password,
        "database_url": database_url,
        "supabase_db_host": supabase_db_host,
        "supabase_db_port": supabase_db_port,
        "supabase_db_name": supabase_db_name,
        "supabase_db_user": supabase_db_user,
        "supabase_db_sslmode": supabase_db_sslmode,
    }


def load_mc_config():
    load_dotenv()
    mc_username = os.getenv("MC_USERNAME")
    mc_password = os.getenv("MC_PASSWORD")
    if not all([mc_username, mc_password]):
        raise RuntimeError("Variaveis MC_USERNAME/MC_PASSWORD faltando no .env ou no Render.")
    return {"mc_username": mc_username, "mc_password": mc_password}


def open_db_from_env():
    cfg = load_config()
    conn = connect_db(
        cfg["supabase_db_password"],
        cfg["project_ref"],
        cfg.get("project_region", ""),
        cfg.get("supabase_pooler_host", ""),
        cfg.get("supabase_pooler_port", ""),
        cfg.get("database_url", ""),
        cfg.get("supabase_db_host", ""),
        cfg.get("supabase_db_port", ""),
        cfg.get("supabase_db_name", "postgres"),
        cfg.get("supabase_db_user", ""),
        cfg.get("supabase_db_sslmode", "require"),
    )
    ensure_schema(conn)
    return conn, cfg


def save_event(
    conn,
    event_type,
    status_code=None,
    bearer_token=None,
    request_payload=None,
    response_payload=None,
    error_message=None,
    client_ip=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.mcapi_events (
                event_type, status_code, bearer_token, request_payload, response_payload, error_message, client_ip
            ) values (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_type,
                status_code,
                bearer_token,
                Json(request_payload) if request_payload is not None else None,
                Json(response_payload) if response_payload is not None else None,
                error_message,
                client_ip,
            ),
        )
    conn.commit()


def login(username: str, password: str):
    payload = {"username": username, "password": password, "twoFacToken": ""}
    resp = requests.post(LOGIN_URL, headers=LOGIN_HEADERS, json=payload, timeout=30)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}

    token = None
    if isinstance(body, dict):
        for key in ["token", "accessToken", "access_token", "jwt"]:
            if body.get(key):
                token = body[key]
                break

    return resp.status_code, payload, body, token


def create_test_line(bearer_token: str, note_text: str = ""):
    payload = {
        "notes": note_text or f"auto-{rand_suffix()}",
        "package_p2p": "646d1492db22a7b1bc518941",
        "package_iptv": "70",
        "testDuration": 4,
        "krator_package": "1",
    }
    headers = dict(TEST_HEADERS_BASE)
    headers["authorization"] = f"Bearer {bearer_token}"
    resp = requests.post(TEST_URL, headers=headers, json=payload, timeout=30)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    return resp.status_code, payload, body


def fetch_resale_stream_page(bearer_token: str, stream_kind: str, page: int = 1):
    safe_token = normalize_bearer_token(bearer_token)
    kind = (stream_kind or "").strip().lower()
    if kind == "movie":
        url = RESALE_MOVIE_URL
        params = {"removeTmdbNull": "true", "page": page}
    elif kind == "canal":
        url = RESALE_CANAL_URL
        params = {"page": page}
    else:
        raise ValueError("stream_kind invalido")

    headers = dict(STREAM_HEADERS_BASE)
    headers["authorization"] = f"Bearer {safe_token}"
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    return resp.status_code, {"url": url, "params": params}, body


def fetch_resale_findall_page(bearer_token: str, search_text: str = "ss", page: int = 1):
    safe_token = normalize_bearer_token(bearer_token)
    headers = dict(STREAM_HEADERS_BASE)
    headers["authorization"] = f"Bearer {safe_token}"
    params = {"removeTmdbNull": "true", "search": (search_text or "ss"), "page": page}
    resp = requests.get(RESALE_FINDALL_URL, headers=headers, params=params, timeout=30)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    return resp.status_code, {"url": RESALE_FINDALL_URL, "params": params}, body


def tmdb_image_url(path: str, width: str = "w400") -> str:
    safe = (path or "").strip()
    if not safe:
        return ""
    if safe.startswith("http://") or safe.startswith("https://"):
        return safe
    if not safe.startswith("/"):
        safe = f"/{safe}"
    return f"https://image.tmdb.org/t/p/{width}{safe}"


def normalize_title_for_match(title: str) -> str:
    text = unicodedata.normalize("NFKD", (title or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    cleaned = []
    for ch in text.lower():
        cleaned.append(ch if ch.isalnum() else " ")
    return " ".join("".join(cleaned).split())


def normalize_media_family(kind: str) -> str:
    safe = (kind or "").strip().lower()
    if safe in {"movie", "filme"}:
        return "movie"
    if safe in {"series", "serie", "tv", "show"}:
        return "tv"
    return ""


def stream_kind_label(type_stream: str) -> str:
    safe = (type_stream or "").strip().lower()
    if safe == "movie":
        return "Filme"
    if safe == "series":
        return "Serie"
    return "Canal"


def request_content_type_for_stream(type_stream: str) -> str:
    safe = (type_stream or "").strip().lower()
    if safe == "series":
        return "serie"
    if safe == "movie":
        return "filme"
    return "canal"


def normalize_findall_search_items(payload, limit: int = 18):
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []

    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        type_stream = (row.get("type_stream") or "movie").strip().lower()
        sort_text = row.get("sort_order") or row.get("release_date") or row.get("added") or ""
        sort_dt = parse_recent_datetime(sort_text)
        tmdb_id = row.get("tmdb_id")
        tmdb_id_text = str(tmdb_id).strip() if tmdb_id not in (None, "") else ""
        normalized.append(
            {
                "id": row.get("id") or f"findall-{tmdb_id_text or rand_suffix(6)}",
                "title": (row.get("title") or "").strip(),
                "cover": (row.get("cover") or "").strip(),
                "backdrop": (row.get("backdrop") or "").strip(),
                "added": (row.get("added") or "").strip(),
                "sort_order": (row.get("sort_order") or "").strip(),
                "type_stream": type_stream,
                "kind": type_stream,
                "kind_label": stream_kind_label(type_stream),
                "tmdb_id": tmdb_id_text,
                "source": "mcapi_findall",
                "can_request": True,
                "request_content_type": request_content_type_for_stream(type_stream),
                "sort_dt": sort_dt,
            }
        )

    normalized.sort(key=lambda item: item.get("sort_dt") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return normalized[: max(1, int(limit))]


def fetch_tmdb_search_page(url: str, query: str = "ss", page: int = 1, language: str = "pt-BR"):
    tmdb_api_key = (os.getenv("TMDB_API_KEY") or TMDB_DEFAULT_API_KEY).strip()
    tmdb_bearer = (os.getenv("TMDB_BEARER_TOKEN") or TMDB_DEFAULT_BEARER_TOKEN).strip()
    if not tmdb_api_key and not tmdb_bearer:
        return 0, {"url": url, "params": {"query": query, "page": page}}, {"error": "tmdb_missing_credentials"}

    headers = {"accept": "application/json"}
    params = {
        "query": query or "ss",
        "include_adult": "false",
        "language": language or "pt-BR",
        "page": page,
    }
    if tmdb_api_key:
        params["api_key"] = tmdb_api_key
    if tmdb_bearer:
        headers["authorization"] = f"Bearer {tmdb_bearer}"

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    safe_params = dict(params)
    if "api_key" in safe_params:
        safe_params["api_key"] = "***"
    return resp.status_code, {"url": url, "params": safe_params}, body


def fetch_tmdb_search_movie_page(query: str = "ss", page: int = 1, language: str = "pt-BR"):
    return fetch_tmdb_search_page(TMDB_SEARCH_MOVIE_URL, query=query, page=page, language=language)


def fetch_tmdb_search_tv_page(query: str = "ss", page: int = 1, language: str = "pt-BR"):
    return fetch_tmdb_search_page(TMDB_SEARCH_TV_URL, query=query, page=page, language=language)


def normalize_tmdb_movie_items(payload, limit: int = 18):
    rows = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []

    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        tmdb_id = row.get("id")
        if tmdb_id in (None, ""):
            continue
        title = (row.get("title") or row.get("original_title") or "").strip()
        if not title:
            continue
        release_date = (row.get("release_date") or "").strip()
        sort_dt = parse_recent_datetime(release_date) if release_date else None
        added_text = ""
        if release_date:
            try:
                added_text = datetime.strptime(release_date, "%Y-%m-%d").strftime("%d/%m/%Y")
            except Exception:
                added_text = ""
        normalized.append(
            {
                "id": f"tmdb-{tmdb_id}",
                "title": title,
                "cover": tmdb_image_url(row.get("poster_path"), width="w400"),
                "backdrop": tmdb_image_url(row.get("backdrop_path"), width="w780"),
                "added": added_text,
                "sort_order": release_date,
                "type_stream": "movie",
                "kind": "movie",
                "kind_label": "Filme",
                "tmdb_id": str(tmdb_id),
                "source": "tmdb_movie",
                "can_request": True,
                "request_content_type": "filme",
                "sort_dt": sort_dt,
            }
        )

    normalized.sort(key=lambda item: item.get("sort_dt") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return normalized[: max(1, int(limit))]


def normalize_tmdb_tv_items(payload, limit: int = 18):
    rows = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []

    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        tmdb_id = row.get("id")
        if tmdb_id in (None, ""):
            continue
        title = (row.get("name") or row.get("original_name") or "").strip()
        if not title:
            continue
        first_air_date = (row.get("first_air_date") or "").strip()
        sort_dt = parse_recent_datetime(first_air_date) if first_air_date else None
        added_text = ""
        if first_air_date:
            try:
                added_text = datetime.strptime(first_air_date, "%Y-%m-%d").strftime("%d/%m/%Y")
            except Exception:
                added_text = ""
        normalized.append(
            {
                "id": f"tmdb-tv-{tmdb_id}",
                "title": title,
                "cover": tmdb_image_url(row.get("poster_path"), width="w400"),
                "backdrop": tmdb_image_url(row.get("backdrop_path"), width="w780"),
                "added": added_text,
                "sort_order": first_air_date,
                "type_stream": "series",
                "kind": "series",
                "kind_label": "Serie",
                "tmdb_id": str(tmdb_id),
                "source": "tmdb_tv",
                "can_request": True,
                "request_content_type": "serie",
                "sort_dt": sort_dt,
            }
        )

    normalized.sort(key=lambda item: item.get("sort_dt") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return normalized[: max(1, int(limit))]


def reconcile_findall_with_tmdb(findall_items, tmdb_items, merged_limit: int = 18):
    if not isinstance(findall_items, list):
        findall_items = []
    if not isinstance(tmdb_items, list):
        tmdb_items = []

    tmdb_by_family_id = {}
    tmdb_by_family_title = {}
    for tmdb in tmdb_items:
        family = normalize_media_family(tmdb.get("kind") or tmdb.get("type_stream"))
        if not family:
            continue
        tmdb_id = (tmdb.get("tmdb_id") or "").strip()
        if tmdb_id:
            tmdb_by_family_id[(family, tmdb_id)] = tmdb
        norm_title = normalize_title_for_match(tmdb.get("title", ""))
        if norm_title:
            tmdb_by_family_title[(family, norm_title)] = tmdb

    seen_family_tmdb_ids = set()
    seen_family_titles = set()
    enriched_findall = []
    for item in findall_items:
        family = normalize_media_family(item.get("kind") or item.get("type_stream"))
        tmdb_id = (item.get("tmdb_id") or "").strip()
        norm_title = normalize_title_for_match(item.get("title", ""))
        tmdb_ref = None
        if family:
            if tmdb_id:
                tmdb_ref = tmdb_by_family_id.get((family, tmdb_id))
            if tmdb_ref is None and norm_title:
                tmdb_ref = tmdb_by_family_title.get((family, norm_title))
        if tmdb_ref:
            if not item.get("cover"):
                item["cover"] = tmdb_ref.get("cover", "")
            if not item.get("backdrop"):
                item["backdrop"] = tmdb_ref.get("backdrop", "")
        if family and tmdb_id:
            seen_family_tmdb_ids.add((family, tmdb_id))
        if family and norm_title:
            seen_family_titles.add((family, norm_title))
        item["can_request"] = False
        enriched_findall.append(item)

    tmdb_missing = []
    for tmdb in tmdb_items:
        family = normalize_media_family(tmdb.get("kind") or tmdb.get("type_stream"))
        if not family:
            continue
        tmdb_id = (tmdb.get("tmdb_id") or "").strip()
        norm_title = normalize_title_for_match(tmdb.get("title", ""))
        if tmdb_id and (family, tmdb_id) in seen_family_tmdb_ids:
            continue
        if norm_title and (family, norm_title) in seen_family_titles:
            continue
        tmdb_missing.append(tmdb)

    merged = sorted(
        tmdb_missing,
        key=lambda item: item.get("sort_dt") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )[: max(1, int(merged_limit))]
    return enriched_findall, tmdb_missing, merged


def create_resale_content_request(
    bearer_token: str,
    *,
    content_name: str,
    content_type: str,
    tmdb_id: str = "",
    img_url: str = "",
    user_id: str = "",
):
    safe_token = normalize_bearer_token(bearer_token)
    headers = dict(STREAM_HEADERS_BASE)
    headers["authorization"] = f"Bearer {safe_token}"

    multipart_fields = [
        ("content_name", (None, (content_name or "").strip())),
        ("content_type", (None, (content_type or "filme").strip())),
        ("tmdb_id", (None, (tmdb_id or "").strip())),
        ("request", (None, "true")),
        ("request_type", (None, "solicitacao")),
        ("img_url", (None, (img_url or "").strip())),
        ("user_id", (None, (user_id or "").strip())),
    ]

    resp = requests.post(REPORT_CONTENT_RESALE_URL, headers=headers, files=multipart_fields, timeout=30)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}

    request_summary = {
        "content_name": (content_name or "").strip(),
        "content_type": (content_type or "filme").strip(),
        "tmdb_id": (tmdb_id or "").strip(),
        "user_id": (user_id or "").strip(),
    }
    return resp.status_code, request_summary, body


def parse_recent_datetime(value):
    text = (value or "").strip()
    if not text:
        return None

    for parser in (
        lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
        lambda x: datetime.strptime(x, "%Y-%m-%d").replace(tzinfo=BRAZIL_TZ),
        lambda x: datetime.strptime(x, "%d/%m/%Y").replace(tzinfo=BRAZIL_TZ),
    ):
        try:
            dt = parser(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None


def normalize_recent_stream_items(payload, stream_kind: str, limit: int = 10):
    safe_kind = (stream_kind or "").strip().lower()
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []

    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sort_text = row.get("sort_order") or row.get("release_date") or row.get("added") or ""
        sort_dt = parse_recent_datetime(sort_text)
        normalized.append(
            {
                "id": row.get("id") or "",
                "title": row.get("title") or "",
                "cover": row.get("cover") or "",
                "backdrop": row.get("backdrop") or "",
                "added": row.get("added") or "",
                "sort_order": row.get("sort_order") or "",
                "type_stream": row.get("type_stream") or "",
                "kind": safe_kind,
                "kind_label": "Filme" if safe_kind == "movie" else "Canal",
                "sort_dt": sort_dt,
            }
        )

    normalized.sort(key=lambda item: item.get("sort_dt") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return normalized[: max(1, int(limit))]


def parse_iso_to_utc(iso_text: str):
    if not iso_text:
        return None
    try:
        dt = datetime.fromisoformat(iso_text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def normalize_bearer_token(token: str) -> str:
    raw = (token or "").strip()
    if not raw:
        return ""
    if raw.lower().startswith("bearer "):
        return raw.split(" ", 1)[1].strip()
    return raw


def parse_jwt_payload(token: str):
    safe_token = normalize_bearer_token(token)
    try:
        parts = safe_token.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload_json = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        payload = json.loads(payload_json)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def parse_jwt_exp_to_utc(token: str):
    payload = parse_jwt_payload(token)
    exp = payload.get("exp")
    if exp is None:
        return None
    try:
        return datetime.fromtimestamp(int(exp), tz=timezone.utc)
    except Exception:
        return None


def upsert_usuario_criado_ip(conn, client_ip: str, username: str, telefone: str = "", telegram_id: str = "", exp_date_text: str = ""):
    if not client_ip or not username:
        return
    exp_date = parse_iso_to_utc(exp_date_text) if exp_date_text else None

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.usuarios_criados_ip (
              client_ip, username, telefone, telegram_id, exp_date, updated_at
            )
            values (%s, %s, %s, %s, %s, now())
            on conflict (client_ip, username)
            do update
              set telefone = excluded.telefone,
                  telegram_id = excluded.telegram_id,
                  exp_date = excluded.exp_date,
                  updated_at = now()
            """,
            (client_ip, username, telefone or None, telegram_id or None, exp_date),
        )
    conn.commit()


def register_usuario_criado_ip(client_ip: str, username: str, telefone: str = "", telegram_id: str = "", exp_date_text: str = ""):
    conn, _ = open_db_from_env()
    try:
        upsert_usuario_criado_ip(conn, client_ip, username, telefone, telegram_id, exp_date_text)
    finally:
        conn.close()


def get_cached_token(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select bearer_token, expires_at
            from public.mcapi_token_cache
            where id = 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def token_is_valid(expires_at, margin_seconds: int = 60):
    if not expires_at:
        return False
    return expires_at > (datetime.now(timezone.utc) + timedelta(seconds=margin_seconds))


def save_cached_token(conn, token: str, expires_at):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.mcapi_token_cache (id, bearer_token, expires_at, updated_at)
            values (1, %s, %s, now())
            on conflict (id)
            do update set bearer_token = excluded.bearer_token, expires_at = excluded.expires_at, updated_at = now()
            """,
            (token, expires_at),
        )
    conn.commit()


def save_user_cached_token(conn, token: str):
    safe_token = normalize_bearer_token(token)
    if not safe_token:
        return {"user_id": None, "expires_at": None}

    payload = parse_jwt_payload(safe_token)
    user_id = payload.get("id")
    user_id_text = str(user_id) if user_id is not None else None
    expires_at = parse_jwt_exp_to_utc(safe_token)
    if not expires_at:
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=18)

    save_cached_token(conn, safe_token, expires_at)

    if user_id_text:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.mcapi_user_token_cache (user_id, bearer_token, expires_at, updated_at)
                values (%s, %s, %s, now())
                on conflict (user_id)
                do update
                  set bearer_token = excluded.bearer_token,
                      expires_at = excluded.expires_at,
                      updated_at = now()
                """,
                (user_id_text, safe_token, expires_at),
            )
        conn.commit()

    return {"user_id": user_id_text, "expires_at": expires_at}


def get_user_cached_token(conn, user_id: str):
    safe_user_id = (user_id or "").strip()
    if not safe_user_id:
        return None, None
    with conn.cursor() as cur:
        cur.execute(
            """
            select bearer_token, expires_at
            from public.mcapi_user_token_cache
            where user_id = %s
            """,
            (safe_user_id,),
        )
        row = cur.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def get_latest_user_cached_token(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select user_id, bearer_token, expires_at
            from public.mcapi_user_token_cache
            order by updated_at desc
            limit 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def resolve_user_bearer_token(
    conn,
    provided_token: str = "",
    preferred_user_id: str = "",
    allow_shared_fallback: bool = False,
):
    safe_provided = normalize_bearer_token(provided_token)
    if safe_provided:
        saved = save_user_cached_token(conn, safe_provided)
        return {
            "token": safe_provided,
            "source": "request_bearer",
            "user_id": saved.get("user_id"),
            "expires_at": saved.get("expires_at"),
        }

    safe_user_id = (preferred_user_id or "").strip()
    if safe_user_id:
        token, exp = get_user_cached_token(conn, safe_user_id)
        if token and token_is_valid(exp, margin_seconds=0):
            return {
                "token": token,
                "source": "user_cache",
                "user_id": safe_user_id,
                "expires_at": exp,
            }

    if allow_shared_fallback:
        latest_user_id, latest_token, latest_exp = get_latest_user_cached_token(conn)
        if latest_token and token_is_valid(latest_exp, margin_seconds=0):
            return {
                "token": latest_token,
                "source": "latest_user_cache",
                "user_id": latest_user_id,
                "expires_at": latest_exp,
            }

        cached_token, cached_exp = get_cached_token(conn)
        if cached_token and token_is_valid(cached_exp, margin_seconds=0):
            return {
                "token": cached_token,
                "source": "legacy_shared_cache",
                "user_id": None,
                "expires_at": cached_exp,
            }

    return {"token": None, "source": "none", "user_id": None, "expires_at": None}


def get_or_refresh_shared_token(conn, cfg, client_ip: str = None, force_refresh: bool = False):
    with conn.cursor() as cur:
        cur.execute("select pg_advisory_lock(%s);", (TOKEN_LOCK_KEY,))

    try:
        if not force_refresh:
            cached_token, cached_exp = get_cached_token(conn)
            if cached_token and token_is_valid(cached_exp):
                return {
                    "ok": True,
                    "token": cached_token,
                    "expires_at": cached_exp,
                    "from_cache": True,
                }

        login_status, login_req, login_resp, token = login(cfg["mc_username"], cfg["mc_password"])
        save_event(
            conn,
            event_type="login_refresh",
            status_code=login_status,
            bearer_token=token,
            request_payload=login_req,
            response_payload=login_resp,
            error_message=None if login_status < 400 else "login refresh failed",
            client_ip=client_ip,
        )

        if login_status >= 400 or not token:
            return {
                "ok": False,
                "token": None,
                "expires_at": None,
                "from_cache": False,
                "login_status": login_status,
                "login_response": login_resp,
            }

        expires_at = parse_jwt_exp_to_utc(token)
        if not expires_at:
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=18)
        save_cached_token(conn, token, expires_at)
        save_user_cached_token(conn, token)

        return {
            "ok": True,
            "token": token,
            "expires_at": expires_at,
            "from_cache": False,
            "login_status": login_status,
            "login_response": login_resp,
        }
    finally:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_unlock(%s);", (TOKEN_LOCK_KEY,))


def get_or_refresh_memory_token(cfg, force_refresh: bool = False):
    with MEMORY_TOKEN_LOCK:
        cached_token = MEMORY_TOKEN_CACHE.get("token")
        cached_exp = MEMORY_TOKEN_CACHE.get("expires_at")
        if not force_refresh and cached_token and token_is_valid(cached_exp):
            return {
                "ok": True,
                "token": cached_token,
                "expires_at": cached_exp,
                "from_cache": True,
            }

        login_status, login_req, login_resp, token = login(cfg["mc_username"], cfg["mc_password"])
        if login_status >= 400 or not token:
            return {
                "ok": False,
                "token": None,
                "expires_at": None,
                "from_cache": False,
                "login_status": login_status,
                "login_response": login_resp,
            }

        expires_at = parse_jwt_exp_to_utc(token)
        if not expires_at:
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=18)

        MEMORY_TOKEN_CACHE["token"] = token
        MEMORY_TOKEN_CACHE["expires_at"] = expires_at

        return {
            "ok": True,
            "token": token,
            "expires_at": expires_at,
            "from_cache": False,
            "login_status": login_status,
            "login_response": login_resp,
        }


def generate_access_without_db(client_ip: str = None, telefone: str = "", telegram_id: str = "", db_error: str = ""):
    cfg = load_mc_config()
    token_info = get_or_refresh_memory_token(cfg, force_refresh=False)
    if not token_info.get("ok"):
        return {
            "ok": False,
            "login_status": token_info.get("login_status"),
            "login_response": token_info.get("login_response"),
            "error": "Falha ao obter token compartilhado em memoria.",
            "storage": "memory",
            "db_error": db_error,
        }

    token = token_info["token"]
    lead_note = build_lead_note(telefone, telegram_id)
    test_status, test_req, test_resp = create_test_line(token, note_text=lead_note)

    if test_status in (401, 403):
        token_info = get_or_refresh_memory_token(cfg, force_refresh=True)
        if not token_info.get("ok"):
            return {
                "ok": False,
                "login_status": token_info.get("login_status"),
                "login_response": token_info.get("login_response"),
                "error": "Token em memoria expirou e nao foi possivel renovar.",
                "storage": "memory",
                "db_error": db_error,
            }
        token = token_info["token"]
        test_status, test_req, test_resp = create_test_line(token, note_text=lead_note)

    return {
        "ok": test_status < 400,
        "login_status": token_info.get("login_status"),
        "login_response": token_info.get("login_response"),
        "bearer_token": token,
        "test_status": test_status,
        "test_response": test_resp,
        "token_from_cache": token_info.get("from_cache", False),
        "storage": "memory",
        "db_error": db_error,
    }


def get_active_access_for_ip(client_ip: str):
    if not client_ip:
        return None

    conn, _ = open_db_from_env()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select response_payload
                from public.mcapi_events
                where event_type = 'line_test'
                  and status_code < 400
                  and client_ip = %s
                order by created_at desc
                limit 1
                """,
                (client_ip,),
            )
            row = cur.fetchone()

        if not row:
            return None

        payload = row[0] if isinstance(row[0], dict) else {}
        exp_date = payload.get("exp_date")
        exp_utc = parse_iso_to_utc(exp_date)
        if not exp_utc or exp_utc <= datetime.now(timezone.utc):
            return None

        reused_payload = {
            "username": payload.get("username", ""),
            "password": payload.get("password", ""),
            "exp_date": exp_date,
        }
        save_event(
            conn,
            event_type="line_test_reuse",
            status_code=200,
            request_payload={"reason": "active_access_reused"},
            response_payload=reused_payload,
            client_ip=client_ip,
        )
        return reused_payload
    finally:
        conn.close()


def ensure_first_allowed_ip(conn, client_ip: str):
    if not client_ip:
        return False

    with conn.cursor() as cur:
        cur.execute("select count(*) from public.ip_liberados;")
        total = cur.fetchone()[0]
        if total == 0:
            cur.execute(
                """
                insert into public.ip_liberados (ip, descricao, ativo)
                values (%s, %s, true)
                on conflict (ip) do nothing
                """,
                (client_ip, "Primeiro IP liberado automaticamente"),
            )
            conn.commit()
            return True
    return False


def is_ip_liberado(conn, client_ip: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            select exists (
              select 1
              from public.ip_liberados
              where ip = %s and ativo = true
            )
            """,
            (client_ip,),
        )
        return bool(cur.fetchone()[0])


def get_ip_usage(conn, client_ip: str):
    now_brt = datetime.now(BRAZIL_TZ)
    day_start_brt = now_brt.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start_brt = day_start_brt - timedelta(days=day_start_brt.weekday())

    day_start_utc = day_start_brt.astimezone(timezone.utc)
    week_start_utc = week_start_brt.astimezone(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            select
              count(*) filter (where created_at >= %s) as day_count,
              count(*) filter (where created_at >= %s) as week_count
            from public.mcapi_events
            where event_type = 'line_test'
              and status_code < 400
              and client_ip = %s
            """,
            (day_start_utc, week_start_utc, client_ip),
        )
        row = cur.fetchone()
        return int(row[0] or 0), int(row[1] or 0)


def check_ip_limits(client_ip: str):
    conn, _ = open_db_from_env()
    try:
        first_seeded = ensure_first_allowed_ip(conn, client_ip)
        liberado = is_ip_liberado(conn, client_ip)
        day_count, week_count = get_ip_usage(conn, client_ip)

        allowed_by_rule = day_count < 1 and week_count < 2
        allowed = liberado or allowed_by_rule

        reason = ""
        if not allowed:
            if day_count >= 1:
                reason = "Limite diario atingido: 1 geracao por dia para este IP."
            elif week_count >= 2:
                reason = "Limite semanal atingido: 2 geracoes por semana para este IP."
            else:
                reason = "Limite de IP atingido."

        return {
            "allowed": allowed,
            "is_liberado": liberado,
            "first_seeded": first_seeded,
            "day_count": day_count,
            "week_count": week_count,
            "reason": reason,
        }
    finally:
        conn.close()


def add_ip_liberado(client_ip: str, descricao: str = "Liberado manualmente"):
    conn, _ = open_db_from_env()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.ip_liberados (ip, descricao, ativo, updated_at)
                values (%s, %s, true, now())
                on conflict (ip)
                do update set ativo = true, descricao = excluded.descricao, updated_at = now()
                """,
                (client_ip, descricao),
            )
        conn.commit()
    finally:
        conn.close()


def remove_ip_liberado(client_ip: str):
    conn, _ = open_db_from_env()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.ip_liberados
                set ativo = false, updated_at = now()
                where ip = %s
                """,
                (client_ip,),
            )
        conn.commit()
    finally:
        conn.close()


def list_ip_liberados():
    conn, _ = open_db_from_env()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select ip, descricao, ativo, created_at, updated_at
                from public.ip_liberados
                order by ativo desc, created_at asc
                """
            )
            rows = cur.fetchall()

        data = []
        for row in rows:
            data.append(
                {
                    "ip": row[0],
                    "descricao": row[1] or "",
                    "ativo": bool(row[2]),
                    "created_at": row[3].astimezone(BRAZIL_TZ).strftime("%d/%m/%Y %H:%M:%S"),
                    "updated_at": row[4].astimezone(BRAZIL_TZ).strftime("%d/%m/%Y %H:%M:%S"),
                }
            )
        return data
    finally:
        conn.close()


def summarize_stream_payload(payload):
    if not isinstance(payload, dict):
        return {"ok": False}
    data = payload.get("data", [])
    sample = []
    if isinstance(data, list):
        for row in data[:3]:
            if isinstance(row, dict):
                sample.append(row.get("id"))
    return {
        "ok": True,
        "total": payload.get("total"),
        "count": len(data) if isinstance(data, list) else 0,
        "sample_ids": sample,
    }


def resolve_bearer_for_user(
    conn,
    cfg,
    *,
    provided_bearer: str = "",
    preferred_user_id: str = "",
    client_ip: str = None,
    allow_shared_fallback: bool = False,
):
    bearer_info = resolve_user_bearer_token(
        conn,
        provided_token=provided_bearer,
        preferred_user_id=preferred_user_id,
        allow_shared_fallback=allow_shared_fallback,
    )
    bearer_token = bearer_info.get("token")
    if not bearer_token and allow_shared_fallback:
        token_info = get_or_refresh_shared_token(conn, cfg, client_ip=client_ip, force_refresh=False)
        if token_info.get("ok") and token_info.get("token"):
            bearer_token = token_info.get("token")
            parsed = parse_jwt_payload(bearer_token)
            user_id_text = str(parsed.get("id")) if parsed.get("id") is not None else None
            bearer_info = {
                "source": "shared_token",
                "user_id": user_id_text,
                "expires_at": token_info.get("expires_at"),
            }

    return {
        "token": bearer_token,
        "source": bearer_info.get("source"),
        "user_id": bearer_info.get("user_id"),
        "expires_at": bearer_info.get("expires_at"),
    }


def fetch_recent_streams_for_user(
    provided_bearer: str = "",
    preferred_user_id: str = "",
    movies_limit: int = 12,
    channels_limit: int = 12,
    client_ip: str = None,
    allow_shared_fallback: bool = False,
):
    conn, cfg = open_db_from_env()
    try:
        bearer_info = resolve_bearer_for_user(
            conn,
            cfg,
            provided_bearer=provided_bearer,
            preferred_user_id=preferred_user_id,
            client_ip=client_ip,
            allow_shared_fallback=allow_shared_fallback,
        )
        bearer_token = bearer_info.get("token")

        if not bearer_token:
            return {
                "ok": False,
                "error": "Nenhum bearer disponivel para este usuario.",
                "token_source": bearer_info.get("source"),
                "user_id": bearer_info.get("user_id"),
                "movies": [],
                "channels": [],
                "merged": [],
            }

        movie_status, movie_req, movie_resp = fetch_resale_stream_page(bearer_token, "movie", page=1)
        save_event(
            conn,
            event_type="resale_movie_recent",
            status_code=movie_status,
            request_payload=movie_req,
            response_payload=summarize_stream_payload(movie_resp),
            error_message=None if movie_status < 400 else "movie recent fetch failed",
            client_ip=client_ip,
        )

        channel_status, channel_req, channel_resp = fetch_resale_stream_page(bearer_token, "canal", page=1)
        save_event(
            conn,
            event_type="resale_canal_recent",
            status_code=channel_status,
            request_payload=channel_req,
            response_payload=summarize_stream_payload(channel_resp),
            error_message=None if channel_status < 400 else "canal recent fetch failed",
            client_ip=client_ip,
        )

        movies = normalize_recent_stream_items(movie_resp, "movie", limit=movies_limit) if movie_status < 400 else []
        channels = normalize_recent_stream_items(channel_resp, "canal", limit=channels_limit) if channel_status < 400 else []
        merged = sorted(
            movies + channels,
            key=lambda item: item.get("sort_dt") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

        for row in merged:
            sort_dt = row.get("sort_dt")
            if not row.get("added") and sort_dt:
                row["added"] = sort_dt.astimezone(BRAZIL_TZ).strftime("%d/%m/%Y")
            row.pop("sort_dt", None)
        for row in movies:
            row.pop("sort_dt", None)
        for row in channels:
            row.pop("sort_dt", None)

        return {
            "ok": movie_status < 400 and channel_status < 400,
            "token_source": bearer_info.get("source"),
            "user_id": bearer_info.get("user_id"),
            "movie_status": movie_status,
            "channel_status": channel_status,
            "movies": movies,
            "channels": channels,
            "merged": merged,
        }
    finally:
        conn.close()


def fetch_catalog_request_tracks_for_user(
    provided_bearer: str = "",
    preferred_user_id: str = "",
    search_query: str = "ss",
    findall_limit: int = 18,
    tmdb_limit: int = 18,
    merged_limit: int = 20,
    client_ip: str = None,
    allow_shared_fallback: bool = False,
):
    conn, cfg = open_db_from_env()
    try:
        bearer_info = resolve_bearer_for_user(
            conn,
            cfg,
            provided_bearer=provided_bearer,
            preferred_user_id=preferred_user_id,
            client_ip=client_ip,
            allow_shared_fallback=allow_shared_fallback,
        )
        bearer_token = bearer_info.get("token")
        if not bearer_token:
            return {
                "ok": False,
                "error": "Nenhum bearer disponivel para esta consulta.",
                "token_source": bearer_info.get("source"),
                "user_id": bearer_info.get("user_id"),
                "findall_items": [],
                "tmdb_missing_items": [],
                "merged_items": [],
            }

        findall_status, findall_req, findall_resp = fetch_resale_findall_page(
            bearer_token,
            search_text=search_query,
            page=1,
        )
        save_event(
            conn,
            event_type="resale_findall_search",
            status_code=findall_status,
            request_payload=findall_req,
            response_payload=summarize_stream_payload(findall_resp),
            error_message=None if findall_status < 400 else "findall search failed",
            client_ip=client_ip,
        )
        findall_items = normalize_findall_search_items(findall_resp, limit=findall_limit) if findall_status < 400 else []

        tmdb_movie_status, tmdb_movie_req, tmdb_movie_resp = fetch_tmdb_search_movie_page(query=search_query, page=1)
        tmdb_movie_summary = (
            summarize_stream_payload({"data": tmdb_movie_resp.get("results", [])})
            if isinstance(tmdb_movie_resp, dict)
            else {"ok": False}
        )
        save_event(
            conn,
            event_type="tmdb_search_movie",
            status_code=tmdb_movie_status if tmdb_movie_status else None,
            request_payload=tmdb_movie_req,
            response_payload=tmdb_movie_summary,
            error_message=None if tmdb_movie_status == 200 else "tmdb movie search unavailable",
            client_ip=client_ip,
        )

        tmdb_tv_status, tmdb_tv_req, tmdb_tv_resp = fetch_tmdb_search_tv_page(query=search_query, page=1)
        tmdb_tv_summary = (
            summarize_stream_payload({"data": tmdb_tv_resp.get("results", [])})
            if isinstance(tmdb_tv_resp, dict)
            else {"ok": False}
        )
        save_event(
            conn,
            event_type="tmdb_search_tv",
            status_code=tmdb_tv_status if tmdb_tv_status else None,
            request_payload=tmdb_tv_req,
            response_payload=tmdb_tv_summary,
            error_message=None if tmdb_tv_status == 200 else "tmdb tv search unavailable",
            client_ip=client_ip,
        )

        tmdb_movie_items = normalize_tmdb_movie_items(tmdb_movie_resp, limit=tmdb_limit) if tmdb_movie_status == 200 else []
        tmdb_tv_items = normalize_tmdb_tv_items(tmdb_tv_resp, limit=tmdb_limit) if tmdb_tv_status == 200 else []
        tmdb_items = sorted(
            tmdb_movie_items + tmdb_tv_items,
            key=lambda item: item.get("sort_dt") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        tmdb_status = 200 if (tmdb_movie_status == 200 or tmdb_tv_status == 200) else (tmdb_movie_status or tmdb_tv_status or 0)

        findall_items, tmdb_missing_items, merged_items = reconcile_findall_with_tmdb(
            findall_items,
            tmdb_items,
            merged_limit=merged_limit,
        )

        for row in merged_items:
            sort_dt = row.get("sort_dt")
            if not row.get("added") and sort_dt:
                row["added"] = sort_dt.astimezone(BRAZIL_TZ).strftime("%d/%m/%Y")
            row.pop("sort_dt", None)
        for row in findall_items:
            row.pop("sort_dt", None)
        for row in tmdb_missing_items:
            row.pop("sort_dt", None)

        return {
            "ok": findall_status < 400,
            "token_source": bearer_info.get("source"),
            "user_id": bearer_info.get("user_id"),
            "findall_status": findall_status,
            "tmdb_status": tmdb_status,
            "tmdb_movie_status": tmdb_movie_status,
            "tmdb_tv_status": tmdb_tv_status,
            "findall_items": findall_items,
            "tmdb_missing_items": tmdb_missing_items,
            "merged_items": merged_items,
        }
    finally:
        conn.close()


def submit_content_request_for_user(
    *,
    provided_bearer: str = "",
    preferred_user_id: str = "",
    content_name: str,
    content_type: str = "filme",
    tmdb_id: str = "",
    img_url: str = "",
    user_id: str = "",
    client_ip: str = None,
    allow_shared_fallback: bool = False,
):
    conn, cfg = open_db_from_env()
    try:
        bearer_info = resolve_bearer_for_user(
            conn,
            cfg,
            provided_bearer=provided_bearer,
            preferred_user_id=preferred_user_id,
            client_ip=client_ip,
            allow_shared_fallback=allow_shared_fallback,
        )
        bearer_token = bearer_info.get("token")
        if not bearer_token:
            return {
                "ok": False,
                "error": "Nenhum bearer disponivel para enviar pedido.",
                "token_source": bearer_info.get("source"),
                "user_id": bearer_info.get("user_id"),
            }

        resolved_user_id = (user_id or "").strip()
        if not resolved_user_id:
            resolved_user_id = (bearer_info.get("user_id") or "").strip()
        if not resolved_user_id:
            payload = parse_jwt_payload(bearer_token)
            if payload.get("id") is not None:
                resolved_user_id = str(payload.get("id"))

        status_code, request_payload, response_payload = create_resale_content_request(
            bearer_token,
            content_name=content_name,
            content_type=content_type,
            tmdb_id=tmdb_id,
            img_url=img_url,
            user_id=resolved_user_id,
        )
        save_event(
            conn,
            event_type="resale_content_request",
            status_code=status_code,
            request_payload=request_payload,
            response_payload=response_payload if isinstance(response_payload, dict) else {"raw": str(response_payload)},
            error_message=None if status_code < 400 else "content request failed",
            client_ip=client_ip,
        )

        ok = status_code < 400 and isinstance(response_payload, dict)
        return {
            "ok": ok,
            "status_code": status_code,
            "token_source": bearer_info.get("source"),
            "user_id": resolved_user_id,
            "request_payload": request_payload,
            "response": response_payload,
        }
    finally:
        conn.close()


def get_nested_value(data: dict, *path):
    value = data
    for key in path:
        if not isinstance(value, dict):
            return ""
        value = value.get(key)
    return value if value is not None else ""


def make_kirvano_webhook_key(payload: dict) -> str:
    event = str(payload.get("event") or "UNKNOWN").strip().upper()
    sale_id = str(payload.get("sale_id") or "").strip()
    checkout_id = str(payload.get("checkout_id") or "").strip()
    created_at = str(payload.get("created_at") or get_nested_value(payload, "payment", "finished_at") or "").strip()

    parts = [event, sale_id or checkout_id, created_at]
    if any(parts[1:]):
        return ":".join(part or "-" for part in parts)

    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return f"{event}:{digest}"


def extract_kirvano_customer(payload: dict) -> dict:
    customer = payload.get("customer") if isinstance(payload.get("customer"), dict) else {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    custom_fields = payload.get("custom_fields") if isinstance(payload.get("custom_fields"), dict) else {}

    telegram_id = (
        customer.get("telegram_id")
        or metadata.get("telegram_id")
        or custom_fields.get("telegram_id")
        or payload.get("telegram_id")
        or ""
    )

    return {
        "name": str(customer.get("name") or payload.get("customer_name") or "").strip(),
        "email": str(customer.get("email") or payload.get("customer_email") or "").strip(),
        "phone": str(customer.get("phone_number") or customer.get("phone") or payload.get("phone_number") or "").strip(),
        "telegram_id": str(telegram_id or "").strip(),
    }


def build_kirvano_public_result(result: dict) -> dict:
    test_response = result.get("test_response") if isinstance(result.get("test_response"), dict) else {}
    username = test_response.get("username", "")
    password = test_response.get("password", "")
    ok = bool(result.get("ok") and username and password)
    return {
        "ok": ok,
        "test_status": result.get("test_status"),
        "token_from_cache": bool(result.get("token_from_cache")),
        "storage": result.get("storage", "supabase"),
        "access": {
            "username": username,
            "password": password,
            "exp_date": test_response.get("exp_date", ""),
        },
        "error": result.get("error", "") if ok else result.get("error", "") or "MCAPI nao retornou usuario/senha.",
    }


def process_kirvano_webhook(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {"ok": False, "error": "Payload invalido."}

    event = str(payload.get("event") or "").strip().upper()
    status = str(payload.get("status") or "").strip().upper()
    sale_id = str(payload.get("sale_id") or "").strip()
    checkout_id = str(payload.get("checkout_id") or "").strip()
    webhook_key = make_kirvano_webhook_key(payload)
    customer = extract_kirvano_customer(payload)
    should_release_access = event in KIRVANO_ACCESS_EVENTS and status in KIRVANO_APPROVED_STATUSES

    try:
        conn, _ = open_db_from_env()
    except Exception:
        if not should_release_access:
            return {
                "ok": True,
                "action": "ignored",
                "reason": "Evento sem liberacao de acesso.",
                "event": event,
                "status": status,
                "storage": "memory",
                "db_available": False,
            }

        with MEMORY_WEBHOOK_LOCK:
            cached_result = MEMORY_WEBHOOK_RESULTS.get(webhook_key)
            if cached_result:
                return {**cached_result, "duplicate": True, "action": "already_processed"}

            lead_key = sale_id or checkout_id or webhook_key
            result = generate_access_once(
                client_ip=f"kirvano:{lead_key}",
                telefone=customer["phone"],
                telegram_id=customer["telegram_id"],
            )
            public_result = {
                **build_kirvano_public_result(result),
                "webhook_key": webhook_key,
                "event": event,
                "sale_id": sale_id,
                "checkout_id": checkout_id,
                "db_available": False,
            }
            if public_result.get("ok"):
                MEMORY_WEBHOOK_RESULTS[webhook_key] = public_result
            return public_result

    try:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_lock(hashtext(%s));", (webhook_key,))

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.kirvano_webhooks (
                      webhook_key, event, sale_id, checkout_id, status,
                      customer_name, customer_email, customer_phone,
                      request_payload, updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    on conflict (webhook_key)
                    do update set
                      status = excluded.status,
                      customer_name = excluded.customer_name,
                      customer_email = excluded.customer_email,
                      customer_phone = excluded.customer_phone,
                      request_payload = excluded.request_payload,
                      updated_at = now()
                    returning processed_at, access_username, access_password, access_exp_date, result_payload
                    """,
                    (
                        webhook_key,
                        event or "UNKNOWN",
                        sale_id or None,
                        checkout_id or None,
                        status or None,
                        customer["name"] or None,
                        customer["email"] or None,
                        customer["phone"] or None,
                        Json(payload),
                    ),
                )
                row = cur.fetchone()
            conn.commit()

            if row and row[0] and row[1]:
                return {
                    "ok": True,
                    "duplicate": True,
                    "action": "already_processed",
                    "webhook_key": webhook_key,
                    "event": event,
                    "access": {
                        "username": row[1],
                        "password": row[2] or "",
                        "exp_date": row[3].isoformat() if row[3] else "",
                    },
                }

            if not should_release_access:
                ignored_result = {
                    "ok": True,
                    "action": "ignored",
                    "reason": "Evento sem liberacao de acesso.",
                    "event": event,
                    "status": status,
                }
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        update public.kirvano_webhooks
                        set result_payload = %s,
                            error_message = null,
                            updated_at = now()
                        where webhook_key = %s
                        """,
                        (Json(ignored_result), webhook_key),
                    )
                conn.commit()
                return ignored_result

            lead_key = sale_id or checkout_id or webhook_key
            client_key = f"kirvano:{lead_key}"
            result = generate_access_once(
                client_ip=client_key,
                telefone=customer["phone"],
                telegram_id=customer["telegram_id"],
            )
            public_result = build_kirvano_public_result(result)
            access = public_result["access"]
            access_exp = parse_iso_to_utc(access.get("exp_date")) if access.get("exp_date") else None

            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.kirvano_webhooks
                    set access_username = %s,
                        access_password = %s,
                        access_exp_date = %s,
                        result_payload = %s,
                        processed_at = case when %s then now() else processed_at end,
                        error_message = %s,
                        updated_at = now()
                    where webhook_key = %s
                    """,
                    (
                        access.get("username") or None,
                        access.get("password") or None,
                        access_exp,
                        Json(public_result),
                        bool(public_result["ok"]),
                        None if public_result["ok"] else public_result.get("error") or "Falha ao gerar acesso.",
                        webhook_key,
                    ),
                )
            conn.commit()

            return {
                **public_result,
                "webhook_key": webhook_key,
                "event": event,
                "sale_id": sale_id,
                "checkout_id": checkout_id,
            }
        finally:
            with conn.cursor() as cur:
                cur.execute("select pg_advisory_unlock(hashtext(%s));", (webhook_key,))
            conn.commit()
    finally:
        conn.close()


def generate_access_once(client_ip: str = None, telefone: str = "", telegram_id: str = ""):
    try:
        conn, cfg = open_db_from_env()
    except Exception as exc:
        return generate_access_without_db(
            client_ip=client_ip,
            telefone=telefone,
            telegram_id=telegram_id,
            db_error=str(exc),
        )

    try:
        token_info = get_or_refresh_shared_token(conn, cfg, client_ip=client_ip, force_refresh=False)
        if not token_info.get("ok"):
            return {
                "ok": False,
                "login_status": token_info.get("login_status"),
                "login_response": token_info.get("login_response"),
                "error": "Falha ao obter token compartilhado.",
            }

        token = token_info["token"]
        lead_note = build_lead_note(telefone, telegram_id)
        test_status, test_req, test_resp = create_test_line(token, note_text=lead_note)

        if test_status in (401, 403):
            save_event(
                conn,
                event_type="line_test_token_expired",
                status_code=test_status,
                bearer_token=token,
                request_payload=test_req,
                response_payload=test_resp,
                error_message="token expirado/invalido - tentando refresh",
                client_ip=client_ip,
            )
            token_info = get_or_refresh_shared_token(conn, cfg, client_ip=client_ip, force_refresh=True)
            if not token_info.get("ok"):
                return {
                    "ok": False,
                    "login_status": token_info.get("login_status"),
                    "login_response": token_info.get("login_response"),
                    "error": "Token expirou e nao foi possivel renovar.",
                }
            token = token_info["token"]
            test_status, test_req, test_resp = create_test_line(token, note_text=lead_note)

        save_event(
            conn,
            event_type="line_test",
            status_code=test_status,
            bearer_token=token,
            request_payload=test_req,
            response_payload=test_resp,
            error_message=None if test_status < 400 else "line test failed",
            client_ip=client_ip,
        )

        if test_status < 400 and isinstance(test_resp, dict):
            upsert_usuario_criado_ip(
                conn,
                client_ip=client_ip or "",
                username=test_resp.get("username", ""),
                telefone=telefone,
                telegram_id=telegram_id,
                exp_date_text=test_resp.get("exp_date", ""),
            )

        return {
            "ok": test_status < 400,
            "login_status": token_info.get("login_status"),
            "login_response": token_info.get("login_response"),
            "bearer_token": token,
            "test_status": test_status,
            "test_response": test_resp,
            "token_from_cache": token_info.get("from_cache", False),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    print(generate_access_once())
