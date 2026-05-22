import os
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, render_template, request
from automate_mcapi import (
    add_ip_liberado,
    check_ip_limits,
    fetch_catalog_request_tracks_for_user,
    fetch_recent_streams_for_user,
    generate_access_once,
    get_active_access_for_ip,
    list_ip_liberados,
    register_usuario_criado_ip,
    remove_ip_liberado,
    submit_content_request_for_user,
)

app = Flask(__name__)


def default_limit_info(reason=""):
    return {
        "allowed": True,
        "is_liberado": False,
        "first_seeded": False,
        "day_count": 0,
        "week_count": 0,
        "reason": reason,
    }


def safe_check_ip_limits(client_ip: str):
    try:
        return check_ip_limits(client_ip)
    except Exception:
        return default_limit_info("Controle temporariamente indisponivel.")


def to_brasilia(iso_date: str) -> str:
    if not iso_date:
        return ""
    dt = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
    return dt.astimezone(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")


def get_client_ip(req) -> str:
    forwarded = req.headers.get("X-Forwarded-For", "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = req.headers.get("X-Real-IP", "").strip()
    if real_ip:
        return real_ip
    return req.remote_addr or "desconhecido"


def extract_bearer_from_request(req) -> str:
    auth = (req.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return (req.args.get("bearer") or "").strip()


def safe_recent_media(req, client_ip: str):
    bearer = extract_bearer_from_request(req)
    preferred_user_id = (req.args.get("user_id") or req.headers.get("X-Mcapi-User-Id") or "").strip()
    try:
        result = fetch_recent_streams_for_user(
            provided_bearer=bearer,
            preferred_user_id=preferred_user_id,
            movies_limit=10,
            channels_limit=10,
            client_ip=client_ip,
            allow_shared_fallback=True,
        )
    except Exception:
        return {"items": [], "ok": False, "token_source": "error"}

    return {
        "items": result.get("merged", [])[:12],
        "ok": bool(result.get("ok")),
        "token_source": result.get("token_source", "none"),
    }


def safe_request_media(req, client_ip: str, search_query: str = "ss"):
    bearer = extract_bearer_from_request(req)
    preferred_user_id = (req.args.get("user_id") or req.headers.get("X-Mcapi-User-Id") or "").strip()
    safe_search = (search_query or "ss").strip() or "ss"
    try:
        result = fetch_catalog_request_tracks_for_user(
            provided_bearer=bearer,
            preferred_user_id=preferred_user_id,
            search_query=safe_search,
            findall_limit=14,
            tmdb_limit=14,
            merged_limit=18,
            client_ip=client_ip,
            allow_shared_fallback=True,
        )
    except Exception:
        return {"items": [], "ok": False, "token_source": "error", "user_id": preferred_user_id}

    return {
        "items": result.get("merged_items", [])[:18],
        "ok": bool(result.get("ok")),
        "token_source": result.get("token_source", "none"),
        "user_id": result.get("user_id") or preferred_user_id,
    }


def render_liberados(status_msg="", is_error=False):
    client_ip = get_client_ip(request)
    rows = list_ip_liberados()
    return render_template(
        "indexdeliberados.html",
        rows=rows,
        client_ip=client_ip,
        status_msg=status_msg,
        is_error=is_error,
    )


@app.get("/")
def index():
    client_ip = get_client_ip(request)
    limit_info = safe_check_ip_limits(client_ip)
    recent_media = safe_recent_media(request, client_ip)
    request_media = safe_request_media(request, client_ip)
    return render_template(
        "index.html",
        result=None,
        basic=None,
        client_ip=client_ip,
        limit_info=limit_info,
        recent_media=recent_media["items"],
        request_media=request_media["items"],
        request_user_id=request_media.get("user_id", ""),
        error_msg="",
        info_msg="",
        form_phone="",
        form_telegram_id="",
    )


@app.post("/gerar")
def gerar():
    client_ip = get_client_ip(request)
    recent_media = safe_recent_media(request, client_ip)
    request_media = safe_request_media(request, client_ip)
    telefone = (request.form.get("telefone") or "").strip()
    telegram_id = (request.form.get("telegram_id") or "").strip()

    if not telefone:
        return render_template(
            "index.html",
            result={"ok": False},
            basic=None,
            client_ip=client_ip,
            limit_info=safe_check_ip_limits(client_ip),
            recent_media=recent_media["items"],
            request_media=request_media["items"],
            request_user_id=request_media.get("user_id", ""),
            error_msg="Para liberar seu teste, informe um WhatsApp valido.",
            info_msg="",
            form_phone=telefone,
            form_telegram_id=telegram_id,
        )

    active_access = get_active_access_for_ip(client_ip)
    if active_access:
        register_usuario_criado_ip(
            client_ip=client_ip,
            username=active_access.get("username", ""),
            telefone=telefone,
            telegram_id=telegram_id,
            exp_date_text=active_access.get("exp_date", ""),
        )
        basic = {
            "username": active_access.get("username", ""),
            "password": active_access.get("password", ""),
            "exp_date_brasilia": to_brasilia(active_access.get("exp_date", "")),
        }
        return render_template(
            "index.html",
            result={"ok": True, "reused": True, "test_response": active_access},
            basic=basic,
            client_ip=client_ip,
            limit_info=safe_check_ip_limits(client_ip),
            recent_media=recent_media["items"],
            request_media=request_media["items"],
            request_user_id=request_media.get("user_id", ""),
            error_msg="",
            info_msg="Seu teste atual ainda esta ativo. Reapresentamos o mesmo acesso para voce continuar.",
            form_phone=telefone,
            form_telegram_id=telegram_id,
        )

    limit_info = safe_check_ip_limits(client_ip)

    if not limit_info.get("allowed"):
        return render_template(
            "index.html",
            result={"ok": False},
            basic=None,
            client_ip=client_ip,
            limit_info=limit_info,
            recent_media=recent_media["items"],
            request_media=request_media["items"],
            request_user_id=request_media.get("user_id", ""),
            error_msg="Seu teste ja foi gerado recentemente. Assim que liberar novamente, voce consegue gerar de novo.",
            info_msg="",
            form_phone=telefone,
            form_telegram_id=telegram_id,
        )

    result = generate_access_once(client_ip=client_ip, telefone=telefone, telegram_id=telegram_id)
    basic = None
    error_msg = ""
    info_msg = ""
    if result.get("ok"):
        test_response = result.get("test_response", {})
        basic = {
            "username": test_response.get("username", ""),
            "password": test_response.get("password", ""),
            "exp_date_brasilia": to_brasilia(test_response.get("exp_date", "")),
        }
        info_msg = "Teste liberado com sucesso. Agora e so configurar e aproveitar."
    else:
        error_msg = "Nao conseguimos liberar agora. Tente novamente em instantes ou fale com nosso time no WhatsApp."

    limit_info = safe_check_ip_limits(client_ip)
    return render_template(
        "index.html",
        result=result,
        basic=basic,
        client_ip=client_ip,
        limit_info=limit_info,
        recent_media=recent_media["items"],
        request_media=request_media["items"],
        request_user_id=request_media.get("user_id", ""),
        error_msg=error_msg,
        info_msg=info_msg,
        form_phone=telefone,
        form_telegram_id=telegram_id,
    )


@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "wplay-teste-4h"}, 200


@app.get("/api/recentes")
def api_recentes():
    client_ip = get_client_ip(request)
    bearer = extract_bearer_from_request(request)
    preferred_user_id = (request.args.get("user_id") or request.headers.get("X-Mcapi-User-Id") or "").strip()
    result = fetch_recent_streams_for_user(
        provided_bearer=bearer,
        preferred_user_id=preferred_user_id,
        movies_limit=20,
        channels_limit=20,
        client_ip=client_ip,
        allow_shared_fallback=True,
    )
    status_code = 200 if result.get("ok") else 400
    return result, status_code


@app.get("/api/solicitacoes")
def api_solicitacoes():
    client_ip = get_client_ip(request)
    bearer = extract_bearer_from_request(request)
    preferred_user_id = (request.args.get("user_id") or request.headers.get("X-Mcapi-User-Id") or "").strip()
    search_query = (request.args.get("query") or request.args.get("search") or "ss").strip() or "ss"

    result = fetch_catalog_request_tracks_for_user(
        provided_bearer=bearer,
        preferred_user_id=preferred_user_id,
        search_query=search_query,
        findall_limit=18,
        tmdb_limit=18,
        merged_limit=24,
        client_ip=client_ip,
        allow_shared_fallback=True,
    )
    status_code = 200 if result.get("ok") else 400
    return result, status_code


@app.post("/api/pedir-conteudo")
def api_pedir_conteudo():
    client_ip = get_client_ip(request)
    bearer = extract_bearer_from_request(request)
    preferred_user_id = (request.args.get("user_id") or request.headers.get("X-Mcapi-User-Id") or "").strip()

    payload = request.get_json(silent=True) or {}
    content_name = (payload.get("content_name") or request.form.get("content_name") or "").strip()
    content_type = (payload.get("content_type") or request.form.get("content_type") or "filme").strip().lower()
    tmdb_id = (payload.get("tmdb_id") or request.form.get("tmdb_id") or "").strip()
    img_url = (payload.get("img_url") or request.form.get("img_url") or "").strip()
    user_id = (payload.get("user_id") or request.form.get("user_id") or "").strip()

    if not content_name:
        return {"ok": False, "error": "Informe o nome do conteudo para solicitar."}, 400

    if content_type not in {"filme", "serie", "canal"}:
        content_type = "filme"

    result = submit_content_request_for_user(
        provided_bearer=bearer,
        preferred_user_id=preferred_user_id,
        content_name=content_name,
        content_type=content_type,
        tmdb_id=tmdb_id,
        img_url=img_url,
        user_id=user_id,
        client_ip=client_ip,
        allow_shared_fallback=True,
    )
    status_code = result.get("status_code") if isinstance(result.get("status_code"), int) else None
    if status_code is None:
        status_code = 200 if result.get("ok") else 400
    return result, status_code


@app.get("/indexdeliberados")
def indexdeliberados():
    return render_liberados()


@app.post("/indexdeliberados/adicionar")
def adicionar_deliberado():
    ip = (request.form.get("ip") or "").strip()
    descricao = (request.form.get("descricao") or "Liberado manualmente").strip()
    if not ip:
        return render_liberados("IP invalido.", is_error=True)

    add_ip_liberado(ip, descricao)
    return render_liberados(f"IP {ip} liberado com sucesso.")


@app.post("/indexdeliberados/liberar-meu-ip")
def liberar_meu_ip():
    ip = get_client_ip(request)
    add_ip_liberado(ip, "Liberado pelo botao 'Liberar meu IP'")
    return render_liberados(f"Seu IP {ip} foi liberado com sucesso.")


@app.post("/indexdeliberados/remover")
def remover_deliberado():
    ip = (request.form.get("ip") or "").strip()
    if not ip:
        return render_liberados("IP invalido.", is_error=True)

    remove_ip_liberado(ip)
    return render_liberados(f"IP {ip} removido da liberacao.")


if __name__ == "__main__":
    debug_enabled = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=5000, debug=debug_enabled)
