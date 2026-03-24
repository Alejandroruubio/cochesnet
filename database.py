"""
database.py — SQLite persistence for scrape history and CRM contacts.
"""
import gzip
import logging
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "cochesnet.db"

CRM_COLUMNS = [
    "id", "nombre", "telefono", "email",
    "url_anuncio", "titulo_vehiculo", "precio", "estado",
    "fecha_contacto", "fecha_seguimiento", "notas",
]

CRM_ESTADOS = [
    "Pendiente", "Contactado", "En negociación",
    "Interesado", "Descartado", "Comprado",
]


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

            CREATE TABLE IF NOT EXISTS crm_contacts (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre            TEXT DEFAULT '',
                telefono          TEXT DEFAULT '',
                email             TEXT DEFAULT '',
                url_anuncio       TEXT DEFAULT '',
                titulo_vehiculo   TEXT DEFAULT '',
                precio            TEXT DEFAULT '',
                estado            TEXT DEFAULT 'Pendiente',
                fecha_contacto    TEXT DEFAULT '',
                fecha_seguimiento TEXT DEFAULT '',
                notas             TEXT DEFAULT '',
                created_at        TEXT DEFAULT (datetime('now')),
                updated_at        TEXT DEFAULT (datetime('now'))
            );
        """)


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


def list_scrapes() -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, timestamp, filters_txt, num_results "
            "FROM scrape_history ORDER BY timestamp DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def load_scrape(scrape_id: int) -> pd.DataFrame | None:
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
            "precio, estado, fecha_contacto, fecha_seguimiento, notas "
            "FROM crm_contacts ORDER BY id"
        ).fetchall()
    if not rows:
        return pd.DataFrame(columns=CRM_COLUMNS)
    return pd.DataFrame([dict(r) for r in rows])


def save_crm(df: pd.DataFrame):
    """Replace all CRM contacts with the current DataFrame."""
    with _conn() as c:
        c.execute("DELETE FROM crm_contacts")
        for _, row in df.iterrows():
            raw_id = row.get("id")
            try:
                row_id = int(raw_id) if pd.notna(raw_id) and str(raw_id).strip() not in ("", "nan") else None
            except (ValueError, TypeError):
                row_id = None

            c.execute(
                """INSERT INTO crm_contacts
                   (id, nombre, telefono, email, url_anuncio, titulo_vehiculo,
                    precio, estado, fecha_contacto, fecha_seguimiento, notas,
                    updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                (
                    row_id,
                    str(row.get("nombre") or ""),
                    str(row.get("telefono") or ""),
                    str(row.get("email") or ""),
                    str(row.get("url_anuncio") or ""),
                    str(row.get("titulo_vehiculo") or ""),
                    str(row.get("precio") or ""),
                    str(row.get("estado") or "Pendiente"),
                    str(row.get("fecha_contacto") or ""),
                    str(row.get("fecha_seguimiento") or ""),
                    str(row.get("notas") or ""),
                ),
            )


def add_crm_from_car(car: dict) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO crm_contacts (titulo_vehiculo, url_anuncio, precio, telefono) "
            "VALUES (?,?,?,?)",
            (
                str(car.get("titulo", "")),
                str(car.get("url", "")),
                str(car.get("precio_€", "")),
                str(car.get("telefono", "")),
            ),
        )
        return cur.lastrowid


def count_crm() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) FROM crm_contacts").fetchone()[0]
