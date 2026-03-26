"""
database.py — Persistence layer for scrape history, CRM contacts and notes.

Supports two backends:
  • Local SQLite  (default, for development)
  • Turso cloud   (when [turso] credentials exist in st.secrets)
"""
from __future__ import annotations

import gzip
import logging
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

# ── Backend detection ─────────────────────────────────────────────────────

try:
    _TURSO_URL: str = st.secrets["turso"]["database_url"]
    _TURSO_TOKEN: str = st.secrets["turso"]["auth_token"]
    _USE_TURSO = bool(_TURSO_URL)
except Exception:
    _TURSO_URL = ""
    _TURSO_TOKEN = ""
    _USE_TURSO = False

if _USE_TURSO:
    try:
        import libsql_experimental as libsql  # type: ignore
    except ImportError:
        logger.warning("libsql_experimental not installed — falling back to local SQLite")
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


# ── Connection ────────────────────────────────────────────────────────────

def _conn():
    if _USE_TURSO:
        return libsql.connect(database=_TURSO_URL, auth_token=_TURSO_TOKEN)
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    return c


def _to_dicts(cursor) -> list[dict]:
    """Convert cursor results to list of dicts (works with both sqlite3 and libsql)."""
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
