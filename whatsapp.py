"""
whatsapp.py — Evolution API v2 client with anti-ban safety features.

Safety measures:
  - Random delays between messages (configurable)
  - Daily message cap (default 30 new conversations/day)
  - Business hours restriction
  - Number validation before sending
  - Typing simulation (composing presence)
  - Message template rotation with personalization
  - Duplicate detection (won't message same number twice)
"""
from __future__ import annotations

import logging
import random
import time
from datetime import datetime
from typing import Callable, Optional

import requests
import streamlit as st

logger = logging.getLogger(__name__)

# ── Config from secrets ───────────────────────────────────────────────────

try:
    _WA_URL: str = st.secrets["whatsapp"]["api_url"].rstrip("/")
    _WA_KEY: str = st.secrets["whatsapp"]["api_key"]
    _WA_INSTANCE: str = st.secrets["whatsapp"]["instance"]
    WA_CONFIGURED = bool(_WA_URL and _WA_KEY and _WA_INSTANCE)
except Exception:
    _WA_URL = ""
    _WA_KEY = ""
    _WA_INSTANCE = ""
    WA_CONFIGURED = False

# ── Default templates ─────────────────────────────────────────────────────

DEFAULT_TEMPLATES = [
    "Hola, buenos días. He visto tu anuncio del {titulo} a {precio} en coches.net y me interesa bastante. ¿Sigue disponible?",
    "Buenas tardes, estoy interesado en el {titulo} que tienes publicado por {precio}. ¿Podríamos hablar sobre él? Un saludo.",
    "Hola! He visto tu {titulo} en coches.net ({precio}) y me gustaría saber más detalles. ¿Está aún a la venta? Gracias.",
]

# ── Safety defaults ───────────────────────────────────────────────────────

SAFETY_DEFAULTS = {
    "min_delay_s": 45,
    "max_delay_s": 120,
    "max_daily": 30,
    "hour_start": 9,
    "hour_end": 21,
    "typing_delay_ms": 3000,
    "country_code": "34",
}


# ── Phone utils ───────────────────────────────────────────────────────────

def normalize_phone(phone: str, country_code: str = "34") -> str:
    """Normalize phone to international format without '+' prefix."""
    if not phone:
        return ""
    digits = "".join(c for c in str(phone) if c.isdigit())
    if not digits:
        return ""
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("+"):
        digits = digits[1:]
    if digits.startswith(country_code) and len(digits) > len(country_code) + 6:
        return digits
    if digits.startswith("0"):
        digits = digits[1:]
    if len(digits) >= 9:
        return country_code + digits
    return ""


# ── API calls ─────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "apikey": _WA_KEY,
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "true",  # bypass ngrok interstitial
    }


def check_connection() -> dict:
    """Check Evolution API instance connection status."""
    if not WA_CONFIGURED:
        return {"connected": False, "error": "WhatsApp no configurado en secrets"}
    try:
        r = requests.get(
            f"{_WA_URL}/instance/connectionState/{_WA_INSTANCE}",
            headers=_headers(),
            timeout=30,
        )
        data = r.json()
        state = (data.get("instance") or {}).get("state", data.get("state", "unknown"))
        connected = state in ("open", "connected", "connecting")
        return {"connected": connected, "state": state, "data": data}
    except Exception as e:
        return {"connected": False, "error": str(e)}


def check_whatsapp_numbers(phones: list[str]) -> dict[str, bool]:
    """Check which phone numbers are registered on WhatsApp.
    Returns dict mapping phone → exists (True/False).
    """
    if not WA_CONFIGURED or not phones:
        return {}
    try:
        r = requests.post(
            f"{_WA_URL}/chat/whatsappNumbers/{_WA_INSTANCE}",
            headers=_headers(),
            json={"numbers": phones},
            timeout=60,
        )
        results = r.json()
        if isinstance(results, list):
            return {item["number"]: item.get("exists", False) for item in results}
        return {}
    except Exception as e:
        logger.error("Error checking WhatsApp numbers: %s", e)
        return {}


def send_presence(phone: str, delay_ms: int = 3000) -> bool:
    """Send 'composing' (typing) presence indicator."""
    if not WA_CONFIGURED:
        return False
    try:
        requests.post(
            f"{_WA_URL}/chat/sendPresence/{_WA_INSTANCE}",
            headers=_headers(),
            json={
                "number": phone,
                "options": {
                    "delay": delay_ms,
                    "presence": "composing",
                    "number": phone,
                },
            },
            timeout=30,
        )
        return True
    except Exception:
        return False


def send_text(phone: str, message: str, delay_ms: int = 0, retries: int = 2) -> dict:
    """Send a text message via Evolution API with retry on timeout."""
    if not WA_CONFIGURED:
        return {"success": False, "error": "WhatsApp no configurado"}
    body: dict = {"number": phone, "text": message}
    if delay_ms > 0:
        body["delay"] = delay_ms
    last_err = ""
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(
                f"{_WA_URL}/message/sendText/{_WA_INSTANCE}",
                headers=_headers(),
                json=body,
                timeout=60,
            )
            data = r.json()
            success = r.status_code in (200, 201)
            return {"success": success, "data": data, "status_code": r.status_code}
        except requests.exceptions.Timeout:
            last_err = f"Timeout (intento {attempt}/{retries})"
            logger.warning("send_text timeout attempt %d/%d for %s", attempt, retries, phone)
            time.sleep(5)
        except Exception as e:
            return {"success": False, "error": str(e)}
    return {"success": False, "error": last_err}


# ── Template engine ───────────────────────────────────────────────────────

def render_template(template: str, data: dict) -> str:
    """Fill template placeholders with contact/car data."""
    replacements = {
        "titulo": data.get("titulo_vehiculo") or data.get("titulo") or "vehículo",
        "precio": data.get("precio") or data.get("precio_€") or "",
        "nombre": data.get("nombre") or data.get("vendedor") or "",
        "marca":  data.get("marca") or "",
        "modelo": data.get("modelo") or "",
        "año":    str(data.get("año") or ""),
        "ciudad": data.get("ciudad") or "",
    }
    result = template
    for key, val in replacements.items():
        result = result.replace(f"{{{key}}}", str(val).strip())
    return result


def pick_template(templates: list[str]) -> str:
    """Randomly pick a template from the list."""
    return random.choice(templates) if templates else DEFAULT_TEMPLATES[0]


# ── Safety checks ─────────────────────────────────────────────────────────

def is_within_hours(hour_start: int = 9, hour_end: int = 21) -> bool:
    """Check if current time is within allowed sending hours."""
    return hour_start <= datetime.now().hour < hour_end


def safe_delay(min_s: int = 45, max_s: int = 120) -> float:
    """Generate a random human-like delay."""
    return random.uniform(min_s, max_s)


# ── Bulk sender ───────────────────────────────────────────────────────────

def send_bulk(
    contacts: list[dict],
    templates: list[str],
    limits: dict,
    already_sent: set[str],
    daily_count: int,
    on_progress: Optional[Callable] = None,
    on_log: Optional[Callable] = None,
) -> dict:
    """Send messages to a list of contacts with full safety measures.

    Args:
        contacts:     List of dicts with at least 'telefono' key.
        templates:    List of message template strings.
        limits:       Safety limits dict (see SAFETY_DEFAULTS).
        already_sent: Set of phone numbers already messaged.
        daily_count:  Messages already sent today.
        on_progress:  Callback(current, total, phone) for UI updates.
        on_log:       Callback(phone, message, success, error) to log results.

    Returns:
        Summary dict with counts.
    """
    country_code = limits.get("country_code", "34")
    max_daily    = limits.get("max_daily", 30)
    min_delay    = limits.get("min_delay_s", 45)
    max_delay    = limits.get("max_delay_s", 120)
    typing_ms    = limits.get("typing_delay_ms", 3000)
    hour_start   = limits.get("hour_start", 9)
    hour_end     = limits.get("hour_end", 21)

    sent = 0
    skipped = 0
    failed = 0
    stopped_reason = ""

    for i, contact in enumerate(contacts):
        # Check hours
        if not is_within_hours(hour_start, hour_end):
            stopped_reason = f"Fuera de horario permitido ({hour_start}:00–{hour_end}:00)"
            break

        # Check daily cap
        if daily_count + sent >= max_daily:
            stopped_reason = f"Límite diario alcanzado ({max_daily} mensajes)"
            break

        # Normalize phone
        raw_phone = str(contact.get("telefono") or "")
        phone = normalize_phone(raw_phone, country_code)
        if not phone:
            skipped += 1
            if on_log:
                on_log(raw_phone, "", False, "Teléfono inválido")
            continue

        # Skip already sent
        if phone in already_sent:
            skipped += 1
            if on_log:
                on_log(phone, "", False, "Ya contactado previamente")
            continue

        if on_progress:
            on_progress(i + 1, len(contacts), phone)

        # Simulate typing
        send_presence(phone, typing_ms)
        time.sleep(random.uniform(2, 4))

        # Pick template and render message
        template = pick_template(templates)
        message = render_template(template, contact)

        # Send
        result = send_text(phone, message)
        if result["success"]:
            sent += 1
            already_sent.add(phone)
            if on_log:
                on_log(phone, message, True, "")
        else:
            failed += 1
            error = result.get("error") or str(result.get("data", ""))
            if on_log:
                on_log(phone, message, False, error)

        # Human-like delay before next message
        if i < len(contacts) - 1:
            delay = safe_delay(min_delay, max_delay)
            # Show delay in progress callback
            if on_progress:
                on_progress(i + 1, len(contacts), f"Esperando {delay:.0f}s…")
            time.sleep(delay)

    return {
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "stopped_reason": stopped_reason,
    }
