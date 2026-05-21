import base64
import json
import os
import random
import socket
import string
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
BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")
TOKEN_LOCK_KEY = 92745131

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


def connect_db(
    db_password: str,
    project_ref: str,
    project_region: str = "",
    pooler_host_override: str = "",
    pooler_port_override: str = "",
):
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
    conn.commit()


def load_config():
    load_dotenv()

    mc_username = os.getenv("MC_USERNAME")
    mc_password = os.getenv("MC_PASSWORD")
    supabase_access_token = os.getenv("SUPABASE_ACCESS_TOKEN")
    supabase_project_name = os.getenv("SUPABASE_PROJECT_NAME")
    supabase_db_password = os.getenv("SUPABASE_DB_PASSWORD")

    if not all([mc_username, mc_password, supabase_access_token, supabase_project_name, supabase_db_password]):
        raise RuntimeError("Variaveis faltando no .env. Confira o arquivo.")

    project_ref = os.getenv("SUPABASE_PROJECT_REF")
    project_region = os.getenv("SUPABASE_PROJECT_REGION", "")
    supabase_pooler_host = os.getenv("SUPABASE_POOLER_HOST", "")
    supabase_pooler_port = os.getenv("SUPABASE_POOLER_PORT", "")
    if not project_ref or not project_region:
        project_info = get_supabase_project_info(supabase_access_token, supabase_project_name)
        if not project_ref:
            project_ref = project_info["ref"]
        if not project_region:
            project_region = project_info.get("region", "")

    return {
        "mc_username": mc_username,
        "mc_password": mc_password,
        "project_ref": project_ref,
        "project_region": project_region,
        "supabase_pooler_host": supabase_pooler_host,
        "supabase_pooler_port": supabase_pooler_port,
        "supabase_db_password": supabase_db_password,
    }


def open_db_from_env():
    cfg = load_config()
    conn = connect_db(
        cfg["supabase_db_password"],
        cfg["project_ref"],
        cfg.get("project_region", ""),
        cfg.get("supabase_pooler_host", ""),
        cfg.get("supabase_pooler_port", ""),
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


def parse_recent_datetime(value):
    text = (value or "").strip()
    if not text:
        return None

    for parser in (
        lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
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
                bearer_info = {
                    "source": "shared_token",
                    "user_id": None,
                }

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


def generate_access_once(client_ip: str = None, telefone: str = "", telegram_id: str = ""):
    conn, cfg = open_db_from_env()
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
