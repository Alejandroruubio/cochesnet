"""
database.py — SQLite persistence for scrape history and CRM contacts.
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

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "cochesnet.db"

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


# ── Connection ─────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


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
        rows = c.execute(
            "SELECT id, timestamp, filters_txt, num_results "
            "FROM scrape_history ORDER BY timestamp DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def load_scrape(scrape_id: int) -> Optional[pd.DataFrame]:
    with _conn() as c:
        row = c.execute(
            "SELECT results_gz FROM scrape_history WHERE id = ?", (scrape_id,)
        ).fetchone()
    return _decompress(row["results_gz"]) if row else None


def delete_scrape(scrape_id: int):
    with _conn() as c:
        c.execute("DELETE FROM scrape_history WHERE id = ?", (scrape_id,))


def count_scrapes() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) FROM scrape_history").fetchone()[0]


# ── CRM ───────────────────────────────────────────────────────────────────

def get_crm() -> pd.DataFrame:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, nombre, telefono, email, url_anuncio, titulo_vehiculo, "
            "precio, tipo_vendedor, estado, fecha_contacto, fecha_seguimiento, notas, created_at "
            "FROM crm_contacts ORDER BY id"
        ).fetchall()
    if not rows:
        return pd.DataFrame(columns=CRM_COLUMNS)
    return pd.DataFrame([dict(r) for r in rows])


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

            # Preserve original created_at if present, else now()
            created_at = str(row.get("created_at") or "")
            created_sql = f"'{created_at}'" if created_at and created_at != "nan" else "datetime('now')"

            c.execute(
                f"""INSERT INTO crm_contacts
                   (id, nombre, telefono, email, url_anuncio, titulo_vehiculo,
                    precio, tipo_vendedor, estado, fecha_contacto, fecha_seguimiento, notas,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,{created_sql},datetime('now'))""",
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
                ),
            )


def add_crm_from_car(car: dict) -> int:
    is_pro = car.get("vendedor_profesional", False)
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


def count_crm() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) FROM crm_contacts").fetchone()[0]


# ── Notes ─────────────────────────────────────────────────────────────────

def list_notes() -> list:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, title, updated_at FROM notes ORDER BY updated_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_note(note_id: int) -> Optional[dict]:
    with _conn() as c:
        row = c.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
    return dict(row) if row else None


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
