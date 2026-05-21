import os
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, render_template, request
from automate_mcapi import (
    add_ip_liberado,
    check_ip_limits,
    generate_access_once,
    get_active_access_for_ip,
    list_ip_liberados,
    register_usuario_criado_ip,
    remove_ip_liberado,
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
    return render_template(
        "index.html",
        result=None,
        basic=None,
        client_ip=client_ip,
        limit_info=limit_info,
        error_msg="",
        info_msg="",
        form_phone="",
        form_telegram_id="",
    )


@app.post("/gerar")
def gerar():
    client_ip = get_client_ip(request)
    telefone = (request.form.get("telefone") or "").strip()
    telegram_id = (request.form.get("telegram_id") or "").strip()

    if not telefone:
        return render_template(
            "index.html",
            result={"ok": False},
            basic=None,
            client_ip=client_ip,
            limit_info=safe_check_ip_limits(client_ip),
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
        error_msg=error_msg,
        info_msg=info_msg,
        form_phone=telefone,
        form_telegram_id=telegram_id,
    )


@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "wplay-teste-4h"}, 200


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
