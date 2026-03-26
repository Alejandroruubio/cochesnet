"""
database.py — Persistence layer for scrape history, CRM contacts and notes.

Supports two backends:
  • Local SQLite  (default, for development)
  • Turso cloud   (when [turso] credentials exist in st.secrets — via HTTP API,
                   no native extension needed, works on any Python version)
"""
from __future__ import annotations

import base64
import gzip
import logging
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests as _http
import streamlit as st

logger = logging.getLogger(__name__)

# ── Backend detection ─────────────────────────────────────────────────────

try:
    _TURSO_URL: str = st.secrets["turso"]["database_url"]
    _TURSO_TOKEN: str = st.secrets["turso"]["auth_token"]
    _USE_TURSO = bool(_TURSO_URL and _TURSO_TOKEN)
except Exception:
    _TURSO_URL = ""
    _TURSO_TOKEN = ""
    _USE_TURSO = False

DB_PATH = Path(__file__).parent / "cochesnet.db"

# ── Constants ─────────────────────────────────────────────────────────────

CRM_COLUMNS = [
    "id", "nombre", "telefono", "email",
    "url_anuncio", "titulo_vehiculo", "precio",
    "tipo_vendedor", "estado",
    "fecha_contacto", "fecha_seguimiento", "notas", "created_at",
]

CRM_ESTADOS = [
    "Pendiente", "Contactado", "En negociación",
    "Interesado", "Descartado", "Comprado",
]

CRM_TIPOS_VENDEDOR = ["Desconocido", "Particular", "Profesional"]


# ══════════════════════════════════════════════════════════════════════════
# Turso HTTP Pipeline API — lightweight sqlite3-compatible adapter
# Uses only `requests`, works on any Python version (no native extensions)
# ══════════════════════════════════════════════════════════════════════════

class _TursoCursor:
    """Minimal cursor-like object returned by TursoConnection.execute()."""

    def __init__(self, result: dict):
        self._result = result or {}
        self._rows = self._decode_rows()
        self._pos = 0

    @property
    def description(self):
        cols = self._result.get("cols")
        if not cols:
            return None
        return [(c["name"],) + (None,) * 6 for c in cols]

    @property
    def lastrowid(self):
        rid = self._result.get("last_insert_rowid")
        return int(rid) if rid is not None else None

    def fetchall(self):
        return self._rows

    def fetchone(self):
        if self._pos < len(self._rows):
            row = self._rows[self._pos]
            self._pos += 1
            return row
        return None

    def _decode_rows(self):
        return [tuple(self._decode_val(cell) for cell in row)
                for row in self._result.get("rows", [])]

    @staticmethod
    def _decode_val(cell):
        if not isinstance(cell, dict):
            return cell
        t = cell.get("type", "text")
        v = cell.get("value")
        if t == "null" or v is None:
            return None
        if t == "integer":
            return int(v)
        if t == "float":
            return float(v)
        if t == "blob":
            return base64.b64decode(cell.get("base64", ""))
        return str(v)


class _TursoConnection:
    """sqlite3.Connection-compatible wrapper over Turso HTTP Pipeline API."""

    def __init__(self, url: str, token: str):
        # libsql://db-org.turso.io → https://db-org.turso.io
        self._api = url.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ── Context manager (matches sqlite3 behaviour) ───────────────────
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False  # don't suppress exceptions

    # ── Query execution ───────────────────────────────────────────────
    def execute(self, sql: str, params=None) -> _TursoCursor:
        stmt: dict = {"sql": sql}
        if params:
            stmt["args"] = [self._encode(p) for p in params]
        results = self._send([{"type": "execute", "stmt": stmt}])
        return _TursoCursor(results[0] if results else {})

    def executescript(self, script: str):
        stmts = [s.strip() for s in script.split(";") if s.strip()]
        batch = [{"type": "execute", "stmt": {"sql": s}} for s in stmts]
        self._send(batch)

    def commit(self):
        pass  # HTTP API auto-commits each request

    # ── Param encoding ────────────────────────────────────────────────
    @staticmethod
    def _encode(val):
        if val is None:
            return {"type": "null", "value": None}
        if isinstance(val, bool):
            return {"type": "integer", "value": str(int(val))}
        if isinstance(val, int):
            return {"type": "integer", "value": str(val)}
        if isinstance(val, float):
            return {"type": "float", "value": val}
        if isinstance(val, bytes):
            return {"type": "blob", "base64": base64.b64encode(val).decode()}
        return {"type": "text", "value": str(val)}

    # ── HTTP transport ────────────────────────────────────────────────
    def _send(self, stmts: list) -> list:
        body = {"requests": stmts + [{"type": "close"}]}
        r = _http.post(self._api, headers=self._headers, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        out = []
        for item in data.get("results", []):
            if item.get("type") == "ok":
                resp = item.get("response", {})
                if resp.get("type") == "execute":
                    out.append(resp.get("result", {}))
            elif item.get("type") == "error":
                err = item.get("error", {})
                logger.error("Turso error: %s", err.get("message", err))
        return out


# ── Connection factory ────────────────────────────────────────────────────

def _conn():
    if _USE_TURSO:
        return _TursoConnection(_TURSO_URL, _TURSO_TOKEN)
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


def _to_dicts(cursor) -> list[dict]:
    """Convert cursor results to list of dicts (works with sqlite3 and Turso)."""
    if cursor.description is None:
        return []
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _to_dict(cursor) -> Optional[dict]:
    """Convert single cursor result to dict."""
    if cursor.description is None:
        return None
    row = cursor.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cursor.description]
    return dict(zip(cols, row))


def db_backend() -> str:
    """Return active backend name for display."""
    return "Turso (nube)" if _USE_TURSO else "SQLite (local)"


# ── Init ──────────────────────────────────────────────────────────────────

def init_db():
    with _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS scrape_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id  TEXT NOT NULL,
                timestamp   TEXT NOT NULL,
                filters_txt TEXT,
                num_results INTEGER DEFAULT 0,
                results_gz  BLOB,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS notes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                title      TEXT DEFAULT 'Sin título',
                content    TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS crm_contacts (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre            TEXT DEFAULT '',
                telefono          TEXT DEFAULT '',
                email             TEXT DEFAULT '',
                url_anuncio       TEXT DEFAULT '',
                titulo_vehiculo   TEXT DEFAULT '',
                precio            TEXT DEFAULT '',
                tipo_vendedor     TEXT DEFAULT 'Desconocido',
                estado            TEXT DEFAULT 'Pendiente',
                fecha_contacto    TEXT DEFAULT '',
                fecha_seguimiento TEXT DEFAULT '',
                notas             TEXT DEFAULT '',
                created_at        TEXT DEFAULT (datetime('now')),
                updated_at        TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS whatsapp_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                phone       TEXT NOT NULL,
                message     TEXT NOT NULL,
                crm_id      INTEGER,
                status      TEXT DEFAULT 'sent',
                error       TEXT DEFAULT '',
                created_at  TEXT DEFAULT (datetime('now'))
            );
        """)
        # Migration for existing databases that predate tipo_vendedor
        try:
            c.execute("ALTER TABLE crm_contacts ADD COLUMN tipo_vendedor TEXT DEFAULT 'Desconocido'")
        except Exception:
            pass  # Column already exists


# ── History ───────────────────────────────────────────────────────────────

def cleanup_old_scrapes(days: int = 7):
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    with _conn() as c:
        c.execute("DELETE FROM scrape_history WHERE created_at < ?", (cutoff,))


def _compress(df: pd.DataFrame) -> bytes:
    raw = df.to_json(orient="records", force_ascii=False).encode("utf-8")
    return gzip.compress(raw, compresslevel=6)


def _decompress(data: bytes) -> pd.DataFrame:
    raw = gzip.decompress(data).decode("utf-8")
    return pd.read_json(raw, orient="records", dtype=False)


def save_scrape(filters_txt: str, df: pd.DataFrame) -> int:
    gz = _compress(df)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO scrape_history (session_id, timestamp, filters_txt, num_results, results_gz) "
            "VALUES (?,?,?,?,?)",
            (str(uuid.uuid4()), ts, filters_txt, len(df), gz),
        )
        return cur.lastrowid


def list_scrapes() -> list:
    with _conn() as c:
        cur = c.execute(
            "SELECT id, timestamp, filters_txt, num_results "
            "FROM scrape_history ORDER BY timestamp DESC"
        )
        return _to_dicts(cur)


def load_scrape(scrape_id: int) -> Optional[pd.DataFrame]:
    with _conn() as c:
        row = _to_dict(c.execute(
            "SELECT results_gz FROM scrape_history WHERE id = ?", (scrape_id,)
        ))
    return _decompress(row["results_gz"]) if row else None


def delete_scrape(scrape_id: int):
    with _conn() as c:
        c.execute("DELETE FROM scrape_history WHERE id = ?", (scrape_id,))


def count_scrapes() -> int:
    with _conn() as c:
        row = c.execute("SELECT COUNT(*) AS cnt FROM scrape_history").fetchone()
        return row[0] if row else 0


# ── CRM ───────────────────────────────────────────────────────────────────

def get_crm() -> pd.DataFrame:
    with _conn() as c:
        cur = c.execute(
            "SELECT id, nombre, telefono, email, url_anuncio, titulo_vehiculo, "
            "precio, tipo_vendedor, estado, fecha_contacto, fecha_seguimiento, notas, created_at "
            "FROM crm_contacts ORDER BY id"
        )
        rows = _to_dicts(cur)
    if not rows:
        return pd.DataFrame(columns=CRM_COLUMNS)
    return pd.DataFrame(rows)


def save_crm(df: pd.DataFrame):
    """Replace all CRM contacts with the current DataFrame (preserving created_at)."""
    with _conn() as c:
        c.execute("DELETE FROM crm_contacts")
        for _, row in df.iterrows():
            raw_id = row.get("id")
            try:
                row_id = int(raw_id) if pd.notna(raw_id) and str(raw_id).strip() not in ("", "nan") else None
            except (ValueError, TypeError):
                row_id = None

            created_at = str(row.get("created_at") or "")
            if not created_at or created_at == "nan":
                created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            c.execute(
                """INSERT INTO crm_contacts
                   (id, nombre, telefono, email, url_anuncio, titulo_vehiculo,
                    precio, tipo_vendedor, estado, fecha_contacto, fecha_seguimiento, notas,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                (
                    row_id,
                    str(row.get("nombre") or ""),
                    str(row.get("telefono") or ""),
                    str(row.get("email") or ""),
                    str(row.get("url_anuncio") or ""),
                    str(row.get("titulo_vehiculo") or ""),
                    str(row.get("precio") or ""),
                    str(row.get("tipo_vendedor") or "Desconocido"),
                    str(row.get("estado") or "Pendiente"),
                    str(row.get("fecha_contacto") or ""),
                    str(row.get("fecha_seguimiento") or ""),
                    str(row.get("notas") or ""),
                    created_at,
                ),
            )


def add_crm_from_car(car: dict) -> int:
    raw = car.get("vendedor_profesional", False)
    # Handle string booleans from JSON-loaded DataFrames ("True"/"False")
    if isinstance(raw, str):
        is_pro = raw.lower() in ("true", "1", "yes", "sí")
    else:
        is_pro = bool(raw)
    tipo_vendedor = "Profesional" if is_pro else "Particular"
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO crm_contacts (titulo_vehiculo, url_anuncio, precio, telefono, tipo_vendedor) "
            "VALUES (?,?,?,?,?)",
            (
                str(car.get("titulo", "")),
                str(car.get("url", "")),
                str(car.get("precio_€", "")),
                str(car.get("telefono", "")),
                tipo_vendedor,
            ),
        )
        return cur.lastrowid


def import_csv_to_crm(df: pd.DataFrame) -> int:
    """Import a CSV DataFrame into the CRM. Maps matching columns, ignores the rest."""
    # Column alias map: common CSV names → CRM column names
    ALIASES = {
        "nombre": "nombre", "name": "nombre",
        "telefono": "telefono", "teléfono": "telefono", "phone": "telefono", "tel": "telefono",
        "email": "email", "correo": "email", "e-mail": "email", "mail": "email",
        "url": "url_anuncio", "url_anuncio": "url_anuncio", "enlace": "url_anuncio", "link": "url_anuncio",
        "titulo": "titulo_vehiculo", "titulo_vehiculo": "titulo_vehiculo",
        "vehiculo": "titulo_vehiculo", "vehículo": "titulo_vehiculo", "title": "titulo_vehiculo",
        "precio": "precio", "price": "precio", "precio_€": "precio",
        "tipo_vendedor": "tipo_vendedor", "tipo vendedor": "tipo_vendedor", "seller_type": "tipo_vendedor",
        "estado": "estado", "status": "estado",
        "notas": "notas", "notes": "notas", "comentarios": "notas",
        "fecha_contacto": "fecha_contacto", "fecha_seguimiento": "fecha_seguimiento",
    }

    # Normalize CSV column names and map to CRM columns
    col_map = {}
    for csv_col in df.columns:
        normalized = csv_col.strip().lower().replace(" ", "_")
        if normalized in ALIASES:
            col_map[csv_col] = ALIASES[normalized]

    if not col_map:
        return 0

    mapped = df.rename(columns=col_map)
    count = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with _conn() as c:
        for _, row in mapped.iterrows():
            c.execute(
                """INSERT INTO crm_contacts
                   (nombre, telefono, email, url_anuncio, titulo_vehiculo,
                    precio, tipo_vendedor, estado, notas, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                (
                    str(row.get("nombre") or ""),
                    str(row.get("telefono") or ""),
                    str(row.get("email") or ""),
                    str(row.get("url_anuncio") or ""),
                    str(row.get("titulo_vehiculo") or ""),
                    str(row.get("precio") or ""),
                    str(row.get("tipo_vendedor") or "Desconocido"),
                    str(row.get("estado") or "Pendiente"),
                    str(row.get("notas") or ""),
                    now,
                ),
            )
            count += 1
    return count


def count_crm() -> int:
    with _conn() as c:
        row = c.execute("SELECT COUNT(*) AS cnt FROM crm_contacts").fetchone()
        return row[0] if row else 0


# ── Notes ─────────────────────────────────────────────────────────────────

def list_notes() -> list:
    with _conn() as c:
        cur = c.execute(
            "SELECT id, title, updated_at FROM notes ORDER BY updated_at DESC"
        )
        return _to_dicts(cur)


def get_note(note_id: int) -> Optional[dict]:
    with _conn() as c:
        return _to_dict(c.execute("SELECT * FROM notes WHERE id = ?", (note_id,)))


def save_note(note_id: Optional[int], title: str, content: str) -> int:
    with _conn() as c:
        if note_id:
            c.execute(
                "UPDATE notes SET title=?, content=?, updated_at=datetime('now') WHERE id=?",
                (title, content, note_id),
            )
            return note_id
        else:
            cur = c.execute(
                "INSERT INTO notes (title, content) VALUES (?,?)", (title, content)
            )
            return cur.lastrowid


def delete_note(note_id: int):
    with _conn() as c:
        c.execute("DELETE FROM notes WHERE id = ?", (note_id,))


# ── WhatsApp log ──────────────────────────────────────────────────────────

def log_whatsapp(phone: str, message: str, crm_id: Optional[int] = None,
                 status: str = "sent", error: str = ""):
    with _conn() as c:
        c.execute(
            "INSERT INTO whatsapp_log (phone, message, crm_id, status, error) VALUES (?,?,?,?,?)",
            (phone, message, crm_id, status, error),
        )


def get_whatsapp_daily_count() -> int:
    """Count messages sent today."""
    today = datetime.now().strftime("%Y-%m-%d")
    with _conn() as c:
        row = c.execute(
            "SELECT COUNT(*) AS cnt FROM whatsapp_log WHERE status='sent' AND created_at LIKE ?",
            (today + "%",),
        ).fetchone()
        return row[0] if row else 0


def get_whatsapp_sent_phones() -> set:
    """Get all phone numbers that have already been messaged."""
    with _conn() as c:
        cur = c.execute("SELECT DISTINCT phone FROM whatsapp_log WHERE status='sent'")
        return {row[0] for row in cur.fetchall()}


def get_whatsapp_log(limit: int = 100) -> list:
    with _conn() as c:
        cur = c.execute(
            "SELECT id, phone, message, crm_id, status, error, created_at "
            "FROM whatsapp_log ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return _to_dicts(cur)
