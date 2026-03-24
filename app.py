"""
app.py — CochesNet Pro
Scraper · Historial · CRM
"""
from __future__ import annotations

import io
import logging
from datetime import datetime

import bcrypt
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth

from database import (
    CRM_ESTADOS,
    add_crm_from_car,
    cleanup_old_scrapes,
    count_crm,
    count_scrapes,
    delete_scrape,
    get_crm,
    init_db,
    list_scrapes,
    load_scrape,
    save_crm,
    save_scrape,
)
from scraper import BODY_TYPES, FUEL_TYPES, SORT_OPTIONS, TRANSMISSIONS, CochesNetScraper

logging.basicConfig(level=logging.INFO)

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CochesNet Pro",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════
# CSS — diseño completo
# ══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

/* ── Reset & base ── */
*, *::before, *::after { box-sizing: border-box; }
html, body, [class*="css"], .stApp { font-family: 'Inter', sans-serif !important; }
#MainMenu, footer { display: none !important; }
header[data-testid="stHeader"] { display: none !important; }
.block-container {
    padding: 1.8rem 2.5rem 3rem !important;
    max-width: 1400px !important;
}

/* ── App background ── */
.stApp { background: #F1F5F9 !important; }

/* ══════════════════════════
   SIDEBAR
══════════════════════════ */
section[data-testid="stSidebar"] {
    background: #1E293B !important;
}
section[data-testid="stSidebar"] > div:first-child {
    padding-top: 1.5rem;
}
/* Todos los textos del sidebar en claro */
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] div,
section[data-testid="stSidebar"] small,
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] .stCaption,
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
    color: #94A3B8 !important;
}
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] strong,
section[data-testid="stSidebar"] b {
    color: #F1F5F9 !important;
}
/* Expanders del sidebar */
section[data-testid="stSidebar"] [data-testid="stExpander"] {
    background: rgba(255,255,255,0.05) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important;
    margin-bottom: 6px !important;
}
section[data-testid="stSidebar"] [data-testid="stExpander"] summary p,
section[data-testid="stSidebar"] [data-testid="stExpander"] summary span {
    color: #CBD5E1 !important;
    font-weight: 500 !important;
}
section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.12) !important;
    margin: 1rem 0 !important;
}
/* Inputs en sidebar — fondo oscuro con texto claro */
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea,
section[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background: rgba(255,255,255,0.08) !important;
    color: #F1F5F9 !important;
    border-color: rgba(255,255,255,0.15) !important;
}
section[data-testid="stSidebar"] [data-baseweb="select"] [data-baseweb="tag"] span {
    color: #1E293B !important;
}
/* Métricas del sidebar */
section[data-testid="stSidebar"] [data-testid="metric-container"] {
    background: rgba(255,255,255,0.08) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important;
    padding: 0.8rem 1rem !important;
}
section[data-testid="stSidebar"] [data-testid="metric-container"] * {
    color: #F1F5F9 !important;
}
section[data-testid="stSidebar"] [data-testid="metric-container"] [data-testid="stMetricLabel"] * {
    color: #94A3B8 !important;
    font-size: 0.72rem !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* ══════════════════════════
   ÁREA PRINCIPAL
══════════════════════════ */

/* Métricas de resultados */
[data-testid="metric-container"] {
    background: white !important;
    border-radius: 14px !important;
    padding: 1.2rem 1.5rem !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06), 0 4px 16px rgba(0,0,0,0.05) !important;
    border: 1px solid #E2E8F0 !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 1.9rem !important;
    font-weight: 700 !important;
    color: #0F172A !important;
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    color: #64748B !important;
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
    background: transparent !important;
    border-bottom: 2px solid #E2E8F0;
    padding-bottom: 0 !important;
    margin-bottom: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0 !important;
    padding: 0.65rem 1.6rem !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    color: #64748B !important;
    background: #E8EEF5 !important;
    border: 1px solid #E2E8F0 !important;
    border-bottom: none !important;
    margin-bottom: -2px !important;
    transition: all 0.15s ease !important;
}
.stTabs [data-baseweb="tab"]:hover {
    background: #F8FAFC !important;
    color: #334155 !important;
}
.stTabs [aria-selected="true"] {
    background: white !important;
    color: #E8420F !important;
    border-color: #E2E8F0 !important;
    border-bottom: 2px solid white !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: white !important;
    border-radius: 0 12px 12px 12px !important;
    padding: 2rem !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
    border: 1px solid #E2E8F0 !important;
    border-top: none !important;
}

/* ── Botones ── */
.stButton > button {
    border-radius: 9px !important;
    font-weight: 600 !important;
    font-size: 0.88rem !important;
    transition: all 0.18s ease !important;
    height: 2.5rem !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #E8420F 0%, #C03008 100%) !important;
    border: none !important;
    color: white !important;
    box-shadow: 0 2px 8px rgba(232,66,15,0.35) !important;
}
.stButton > button[kind="primary"]:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 5px 18px rgba(232,66,15,0.4) !important;
}
.stButton > button[kind="secondary"] {
    background: white !important;
    border: 1.5px solid #CBD5E1 !important;
    color: #334155 !important;
}
.stButton > button[kind="secondary"]:hover {
    border-color: #E8420F !important;
    color: #E8420F !important;
}

/* ── Alerts ── */
.stAlert { border-radius: 10px !important; }
[data-testid="stInfoMessage"]    { background: #EFF6FF !important; color: #1E40AF !important; border-color: #BFDBFE !important; }
[data-testid="stSuccessMessage"] { background: #F0FDF4 !important; color: #166534 !important; border-color: #BBF7D0 !important; }
[data-testid="stWarningMessage"] { background: #FFFBEB !important; color: #92400E !important; border-color: #FDE68A !important; }
[data-testid="stErrorMessage"]   { background: #FEF2F2 !important; color: #991B1B !important; border-color: #FECACA !important; }

/* ── DataFrames ── */
[data-testid="stDataFrame"] iframe { border-radius: 10px; }

/* ── Expanders en área principal ── */
[data-testid="stExpander"] {
    border-radius: 10px !important;
    border: 1px solid #E2E8F0 !important;
    background: #FAFBFC !important;
}

/* ── Divider ── */
hr { border-color: #E2E8F0 !important; }

/* ══════════════════════════
   LOGIN
══════════════════════════ */
div[data-testid="stForm"] {
    background: white;
    border-radius: 16px;
    padding: 2.5rem;
    box-shadow: 0 8px 40px rgba(0,0,0,0.1);
    border: 1px solid #E2E8F0;
}

/* ── Progress bar ── */
.stProgress > div > div { background: #E8420F !important; border-radius: 99px; }
.stProgress > div { background: #E2E8F0 !important; border-radius: 99px; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# DB INIT
# ══════════════════════════════════════════════════════════════════════════
init_db()
if "db_cleaned" not in st.session_state:
    cleanup_old_scrapes(days=7)
    st.session_state.db_cleaned = True

# ══════════════════════════════════════════════════════════════════════════
# AUTH — token persistente con cookie (30 días)
# ══════════════════════════════════════════════════════════════════════════

@st.cache_resource
def _hashed_pw(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt(rounds=10)).decode()


def _build_authenticator() -> stauth.Authenticate:
    try:
        plain_pw   = st.secrets["credentials"]["password"]
        username   = st.secrets["credentials"]["username"]
        cookie_key = st.secrets["credentials"].get("cookie_key", "CochesNetPro_default_key_xyz")
    except Exception:
        plain_pw, username, cookie_key = "cochesnet2024", "admin", "CochesNetPro_default_key_xyz"

    creds = {
        "usernames": {
            username: {
                "name": username.capitalize(),
                "password": _hashed_pw(plain_pw),
            }
        }
    }
    return stauth.Authenticate(
        credentials=creds,
        cookie_name="cochesnet_pro_auth",
        cookie_key=cookie_key,
        cookie_expiry_days=30,
    )


authenticator = _build_authenticator()

# Render login si no está autenticado
if st.session_state.get("authentication_status") is not True:
    # Columnas para centrar el form de login
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        st.markdown(
            """
            <div style="text-align:center; padding: 2.5rem 0 1rem;">
                <div style="font-size:3.5rem; margin-bottom:.5rem;">🚗</div>
                <h2 style="color:#0F172A; font-size:1.7rem; font-weight:800; margin:0;">CochesNet Pro</h2>
                <p style="color:#64748B; margin:.4rem 0 1.8rem; font-size:.9rem;">
                    Accede a tu panel de scraping y CRM
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        name, auth_status, username_result = authenticator.login(
            fields={
                "Form name": "",
                "Username": "Usuario",
                "Password": "Contraseña",
                "Login": "Entrar →",
            },
            location="main",
        )

        if auth_status is False:
            st.error("Usuario o contraseña incorrectos.")
        elif auth_status is None:
            st.caption("💡 Edita `.streamlit/secrets.toml` para cambiar las credenciales. El token dura 30 días.")
    st.stop()

# Aquí el usuario está autenticado
auth_username = st.session_state.get("username", "")

# ══════════════════════════════════════════════════════════════════════════
# SCRAPER + MAKES (cached)
# ══════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_scraper() -> CochesNetScraper:
    return CochesNetScraper(delay=1.5)


@st.cache_data(ttl=3600, show_spinner="Cargando marcas…")
def fetch_makes() -> list:
    return get_scraper().get_makes()


scraper = get_scraper()
makes   = fetch_makes()
make_label_to_obj = {m["label"]: m for m in makes}

# ══════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════
hcol1, hcol2 = st.columns([6, 1])
with hcol1:
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg,#E8420F 0%,#9B2406 100%);
            border-radius: 14px; padding: 1.1rem 1.8rem;
            display: flex; align-items: center; gap: 1rem;
            box-shadow: 0 4px 20px rgba(232,66,15,.3); margin-bottom:.5rem;">
            <div style="font-size:2rem">🚗</div>
            <div>
                <div style="color:white;font-size:1.35rem;font-weight:800;line-height:1.2">CochesNet Pro</div>
                <div style="color:rgba(255,255,255,.75);font-size:.8rem">Scraper · Historial · CRM de contactos</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with hcol2:
    st.markdown("<div style='padding-top:0.4rem'></div>", unsafe_allow_html=True)
    st.markdown(f"<div style='text-align:right;color:#64748B;font-size:.85rem;padding-top:.6rem'>👤 <b>{auth_username}</b></div>", unsafe_allow_html=True)
    authenticator.logout(button_name="Cerrar sesión", location="main")

# ══════════════════════════════════════════════════════════════════════════
# SIDEBAR — filtros de búsqueda
# ══════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 🔍 Búsqueda")

    with st.expander("🚘 Marca y Modelo", expanded=True):
        make_options = ["(Todas las marcas)"] + [m["label"] for m in makes]
        selected_make_label = st.selectbox("Marca", make_options, key="make")
        selected_make = make_label_to_obj.get(selected_make_label)

        model_options = ["(Todos los modelos)"]
        if selected_make:
            model_options += [m["label"] for m in selected_make.get("models", [])]
        selected_model_label = st.selectbox(
            "Modelo", model_options, key="model", disabled=(selected_make is None)
        )

    with st.expander("💶 Precio (€)"):
        c1, c2 = st.columns(2)
        price_from = c1.number_input("Mín", 0, 2_000_000, 0, 500, help="0 = sin límite", key="pf")
        price_to   = c2.number_input("Máx", 0, 2_000_000, 0, 500, help="0 = sin límite", key="pt")

    with st.expander("📅 Año"):
        year_opts = ["(Cualquiera)"] + [str(y) for y in range(2025, 1989, -1)]
        c1, c2 = st.columns(2)
        year_from_sel = c1.selectbox("Desde", year_opts, key="yf")
        year_to_sel   = c2.selectbox("Hasta", year_opts, key="yt")

    with st.expander("🛣️ Kilómetros"):
        c1, c2 = st.columns(2)
        km_from = c1.number_input("Mín", 0, 1_000_000, 0, 5_000, help="0 = sin límite", key="kmf")
        km_to   = c2.number_input("Máx", 0, 1_000_000, 0, 5_000, help="0 = sin límite", key="kmt")

    with st.expander("⚡ Potencia (CV)"):
        c1, c2 = st.columns(2)
        hp_from = c1.number_input("Mín", 0, 2_000, 0, 10, help="0 = sin límite", key="hpf")
        hp_to   = c2.number_input("Máx", 0, 2_000, 0, 10, help="0 = sin límite", key="hpt")

    with st.expander("⛽ Combustible y Carrocería"):
        selected_fuels  = st.multiselect("Combustible", list(FUEL_TYPES.keys()), default=[])
        selected_bodies = st.multiselect("Carrocería",  list(BODY_TYPES.keys()), default=[])

    with st.expander("⚙️ Más opciones"):
        transmission = st.selectbox("Transmisión", list(TRANSMISSIONS.keys()))
        seller_type  = st.selectbox("Tipo de vendedor", ["Todos", "Particular", "Profesional"])
        has_warranty = st.checkbox("Solo con garantía")

    st.divider()
    st.markdown("### Opciones de raspado")
    sort_by   = st.selectbox("Ordenar por", list(SORT_OPTIONS.keys()))
    max_pages = st.slider("Páginas", 1, 200, 5, help="~30 anuncios/página")
    st.caption(f"Máximo ~{max_pages * 30:,} anuncios")

    scrape_btn = st.button("🚀 Iniciar Raspado", type="primary", use_container_width=True)

    st.divider()
    st.markdown("### 📊 Resumen")
    mc1, mc2 = st.columns(2)
    mc1.metric("Historial", count_scrapes())
    mc2.metric("CRM", count_crm())


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def build_filters() -> dict:
    f: dict = {}
    if selected_make:
        f["make_slug"] = selected_make["slug"]
        if selected_model_label != "(Todos los modelos)":
            model_obj = next(
                (m for m in selected_make.get("models", []) if m["label"] == selected_model_label), None
            )
            if model_obj:
                f["model_slug"] = model_obj["slug"]

    if price_from > 0: f["price_from"] = price_from
    if price_to   > 0: f["price_to"]   = price_to
    if year_from_sel != "(Cualquiera)": f["year_from"] = int(year_from_sel)
    if year_to_sel   != "(Cualquiera)": f["year_to"]   = int(year_to_sel)
    if km_from > 0: f["km_from"] = km_from
    if km_to   > 0: f["km_to"]   = km_to
    if hp_from > 0: f["hp_from"] = hp_from
    if hp_to   > 0: f["hp_to"]   = hp_to

    if selected_fuels:  f["fuel_type_ids"]  = [FUEL_TYPES[x]  for x in selected_fuels]
    if selected_bodies: f["body_type_ids"]  = [BODY_TYPES[x]  for x in selected_bodies]

    trans_id = TRANSMISSIONS.get(transmission)
    if trans_id: f["transmission_id"] = trans_id

    if seller_type != "Todos":
        f["seller_type"] = "particular" if seller_type == "Particular" else "profesional"
    if has_warranty:
        f["has_warranty"] = True

    f["sort"] = SORT_OPTIONS[sort_by]
    return f


def filters_summary(f: dict) -> str:
    parts = []
    if f.get("make_slug"):  parts.append(f["make_slug"].replace("-", " ").title())
    if f.get("model_slug"): parts.append(f["model_slug"].replace("-", " ").title())
    if f.get("price_from") or f.get("price_to"):
        parts.append(f"€{f.get('price_from', 0):,}–{f.get('price_to', '∞')}")
    if f.get("year_from") or f.get("year_to"):
        parts.append(f"{f.get('year_from', '?')}–{f.get('year_to', '?')}")
    if f.get("seller_type"):
        parts.append(f["seller_type"].capitalize())
    return " · ".join(parts) if parts else "Todos los coches"


def _safe_numeric(series: pd.Series, strip_non_digits: bool = False) -> pd.Series:
    s = series.astype(str)
    if strip_non_digits:
        s = s.str.replace(r"[^\d]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


# ══════════════════════════════════════════════════════════════════════════
# COMPONENTE: resultados con filtros
# ══════════════════════════════════════════════════════════════════════════

def show_results(df: pd.DataFrame, scrape_id: int):
    if df.empty:
        st.warning("No hay anuncios en este resultado.")
        return

    uid = str(scrape_id)  # clave única para widgets

    # ── Filtros post-scraping ─────────────────────────────────────────────
    with st.expander("🔧 Filtrar resultados", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        fc4, fc5, fc6 = st.columns(3)

        # Precio
        prices_raw = _safe_numeric(df.get("precio_€", pd.Series(dtype=float)), strip_non_digits=True).fillna(0)
        p_min, p_max = int(prices_raw.min()), int(prices_raw.max())
        if p_min == p_max: p_max += 1
        f_price = fc1.slider("💶 Precio €", p_min, p_max, (p_min, p_max), step=500, key=f"fp{uid}")

        # Año
        years_raw = _safe_numeric(df.get("año", pd.Series(dtype=float))).fillna(2000)
        y_min, y_max = int(years_raw.min()), int(years_raw.max())
        if y_min == y_max: y_max += 1
        f_year = fc2.slider("📅 Año", y_min, y_max, (y_min, y_max), key=f"fy{uid}")

        # Kilómetros
        km_raw = _safe_numeric(df.get("kilometros", pd.Series(dtype=float)), strip_non_digits=True).fillna(0)
        k_min, k_max = int(km_raw.min()), int(km_raw.max())
        if k_min == k_max: k_max += 1
        f_km = fc3.slider("🛣️ Km", k_min, k_max, (k_min, k_max), step=5_000, key=f"fk{uid}")

        # Combustible
        combust = sorted(df["combustible"].dropna().unique()) if "combustible" in df.columns else []
        f_fuel = fc4.multiselect("⛽ Combustible", combust, default=[], key=f"ff{uid}")

        # Vendedor
        f_seller = fc5.selectbox("👤 Vendedor", ["Todos", "Particular", "Profesional"], key=f"fs{uid}")

        # Provincia
        provs = sorted(df["provincia"].dropna().unique()) if "provincia" in df.columns else []
        f_prov = fc6.multiselect("📍 Provincia", provs, default=[], key=f"fprov{uid}")

        # Búsqueda texto
        f_txt = st.text_input(
            "🔎 Buscar en título",
            placeholder="ej: automático, pocos km, color rojo…",
            key=f"ftxt{uid}",
        )

    # ── Aplicar filtros ───────────────────────────────────────────────────
    mask = pd.Series(True, index=df.index)

    if "precio_€" in df.columns:
        p_col = _safe_numeric(df["precio_€"], strip_non_digits=True).fillna(0)
        mask &= p_col.between(f_price[0], f_price[1])

    if "año" in df.columns:
        mask &= _safe_numeric(df["año"]).fillna(0).between(f_year[0], f_year[1])

    if "kilometros" in df.columns:
        mask &= _safe_numeric(df["kilometros"], strip_non_digits=True).fillna(0).between(f_km[0], f_km[1])

    if f_fuel and "combustible" in df.columns:
        mask &= df["combustible"].isin(f_fuel)

    if f_seller != "Todos" and "vendedor_profesional" in df.columns:
        is_pro = df["vendedor_profesional"].astype(str).str.lower().isin(("true", "1", "yes"))
        mask &= is_pro if f_seller == "Profesional" else ~is_pro

    if f_prov and "provincia" in df.columns:
        mask &= df["provincia"].isin(f_prov)

    if f_txt.strip() and "titulo" in df.columns:
        mask &= df["titulo"].astype(str).str.contains(f_txt.strip(), case=False, na=False)

    filtered = df[mask].copy()

    # ── Métricas ──────────────────────────────────────────────────────────
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Anuncios", f"{len(filtered):,}")

    p_f = _safe_numeric(filtered.get("precio_€", pd.Series()), strip_non_digits=True).dropna()
    if not p_f.empty: mc2.metric("Precio medio", f"{p_f.mean():,.0f} €")

    y_f = _safe_numeric(filtered.get("año", pd.Series())).dropna()
    if not y_f.empty: mc3.metric("Año medio", f"{y_f.mean():.0f}")

    k_f = _safe_numeric(filtered.get("kilometros", pd.Series()), strip_non_digits=True).dropna()
    if not k_f.empty: mc4.metric("KM medios", f"{k_f.mean():,.0f}")

    n_total = len(df)
    n_filt  = len(filtered)
    if n_filt < n_total:
        st.markdown(
            f"<p style='color:#64748B;font-size:.82rem;margin:.5rem 0'>Mostrando <b>{n_filt:,}</b> de <b>{n_total:,}</b> anuncios</p>",
            unsafe_allow_html=True,
        )

    # ── Selector columnas visibles ─────────────────────────────────────────
    all_cols = list(filtered.columns)
    default_cols = [c for c in [
        "titulo", "precio_€", "año", "kilometros", "combustible",
        "carroceria", "ciudad", "provincia", "vendedor_profesional", "telefono", "url",
    ] if c in all_cols]
    shown_cols = st.multiselect("Columnas visibles", all_cols, default=default_cols, key=f"cols{uid}")
    if not shown_cols:
        shown_cols = default_cols

    st.dataframe(
        filtered[shown_cols],
        use_container_width=True,
        height=480,
        column_config={
            "url":                  st.column_config.LinkColumn("URL", display_text="Ver anuncio"),
            "precio_€":             st.column_config.NumberColumn("Precio €", format="%d €"),
            "año":                  st.column_config.NumberColumn("Año"),
            "kilometros":           st.column_config.NumberColumn("Kilómetros"),
            "vendedor_profesional": st.column_config.CheckboxColumn("Profesional"),
        },
    )

    # ── Acciones ──────────────────────────────────────────────────────────
    ba, bb = st.columns(2)
    ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    buf = io.StringIO()
    filtered.to_csv(buf, index=False, encoding="utf-8-sig")
    ba.download_button(
        "⬇️ Descargar CSV",
        data=buf.getvalue(),
        file_name=f"cochesnet_{ts}.csv",
        mime="text/csv",
        use_container_width=True,
        key=f"dl{uid}",
    )

    if bb.button(f"➕ Añadir {n_filt:,} resultados al CRM", use_container_width=True, key=f"addcrm{uid}"):
        added = 0
        for _, row in filtered.iterrows():   # ← sin límite
            add_crm_from_car(row.to_dict())
            added += 1
        st.success(f"✅ {added:,} anuncios añadidos al CRM.")
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════
tab_scraper, tab_historial, tab_crm = st.tabs(["🔍 Scraper", "📋 Historial", "👥 CRM"])

# ─────────────────────────────────────────────
# TAB 1 — SCRAPER
# ─────────────────────────────────────────────
with tab_scraper:
    if scrape_btn:
        filters = build_filters()
        ftxt    = filters_summary(filters)

        prog   = st.progress(0.0)
        status = st.empty()

        def on_progress(page: int, total: int, found: int):
            prog.progress(page / total)
            status.info(f"Raspando página {page} / {total} — {found} anuncios encontrados…")

        status.info("⏳ Iniciando raspado…")
        df = scraper.scrape(filters, max_pages=max_pages, progress_callback=on_progress)
        prog.progress(1.0)

        if df.empty:
            status.warning("No se encontraron anuncios con los filtros seleccionados.")
        else:
            status.success(f"✅ Completado — **{len(df):,} anuncios** encontrados.")
            sid = save_scrape(ftxt, df)
            st.session_state["last_scrape_id"] = sid
            st.session_state[f"scrape_{sid}"]  = df
            st.info(f"💾 Guardado en historial (#{sid}) — *{ftxt}*")
            show_results(df, scrape_id=sid)

    elif "last_scrape_id" in st.session_state:
        sid      = st.session_state["last_scrape_id"]
        df_cache = st.session_state.get(f"scrape_{sid}")
        if df_cache is not None and not df_cache.empty:
            st.info("📌 Mostrando el último raspado. Pulsa **🚀 Iniciar Raspado** para buscar de nuevo.")
            show_results(df_cache, scrape_id=sid)
        else:
            st.info("Configura los filtros y pulsa **🚀 Iniciar Raspado**.")
    else:
        st.markdown(
            """
            <div style="text-align:center;padding:4rem 1rem;color:#94A3B8">
                <div style="font-size:4rem;margin-bottom:1rem">🔍</div>
                <h3 style="color:#334155;margin-bottom:.5rem;font-size:1.3rem">Listo para buscar</h3>
                <p style="margin:0;font-size:.9rem">Configura los filtros en la barra lateral y pulsa <b>🚀 Iniciar Raspado</b>.</p>
                <p style="margin:.5rem 0 0;font-size:.78rem;color:#CBD5E1">Los resultados se guardan automáticamente.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ─────────────────────────────────────────────
# TAB 2 — HISTORIAL
# ─────────────────────────────────────────────
with tab_historial:
    st.markdown("### 📋 Historial de raspados")
    st.caption("Los resultados se guardan automáticamente. Haz clic en **Cargar** para recuperar cualquier búsqueda sin volver a scrapear.")

    hc1, hc2 = st.columns([3, 1])
    hist_ttl = hc1.slider("Auto-borrar después de (días)", 1, 60, 7, key="hist_ttl")
    if hc2.button("🗑️ Limpiar ahora", key="clean_btn"):
        cleanup_old_scrapes(days=hist_ttl)
        st.success(f"Eliminados scrapes con más de {hist_ttl} días.")
        st.rerun()

    scrapes = list_scrapes()
    if not scrapes:
        st.markdown(
            """<div style="text-align:center;padding:3rem;color:#94A3B8">
                <div style="font-size:3rem">📭</div>
                <p>El historial está vacío.<br>Los raspados se guardan solos al completarse.</p>
            </div>""",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(f"**{len(scrapes)} raspado(s) guardado(s)**", unsafe_allow_html=False)
        st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

        for s in scrapes:
            with st.container():
                c1, c2, c3, c4 = st.columns([1.8, 3.5, 1, 1])
                c1.markdown(
                    f"<span style='color:#64748B;font-size:.82rem'>🕐 {s['timestamp']}</span>",
                    unsafe_allow_html=True,
                )
                c2.markdown(
                    f"<span style='color:#334155;font-size:.88rem'>🏷️ <b>{s['filters_txt'] or '—'}</b> &nbsp;·&nbsp; {s['num_results']:,} anuncios</span>",
                    unsafe_allow_html=True,
                )

                if c3.button("📂 Cargar", key=f"load_{s['id']}", use_container_width=True):
                    with st.spinner("Cargando…"):
                        df_hist = load_scrape(s["id"])
                    if df_hist is not None:
                        st.session_state["last_scrape_id"]     = s["id"]
                        st.session_state[f"scrape_{s['id']}"]  = df_hist
                        st.success("✅ Cargado. Ve a la pestaña **🔍 Scraper** para verlo.")
                    else:
                        st.error("No se pudo cargar.")

                if c4.button("🗑️", key=f"del_{s['id']}", use_container_width=True):
                    delete_scrape(s["id"])
                    st.session_state.pop(f"scrape_{s['id']}", None)
                    if st.session_state.get("last_scrape_id") == s["id"]:
                        st.session_state.pop("last_scrape_id", None)
                    st.rerun()

                st.markdown("<hr style='margin:6px 0;border-color:#F1F5F9'>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# TAB 3 — CRM
# ─────────────────────────────────────────────
with tab_crm:
    st.markdown("### 👥 CRM — Registro de contactos")
    st.caption(
        "Edita directamente en la tabla. Marca ✅ la columna **Borrar** en las filas que quieras eliminar y pulsa **Eliminar marcadas**. "
        "El timestamp de creación se asigna automáticamente."
    )

    crm_df = get_crm()

    # ── Toolbar ───────────────────────────────────────────────────────────
    ta, tb, tc, td = st.columns([1.2, 1.2, 1.2, 1])

    add_empty = ta.button("➕ Nuevo contacto", use_container_width=True)
    save_btn  = tb.button("💾 Guardar cambios", type="primary", use_container_width=True)
    del_btn   = tc.button("🗑️ Eliminar marcadas", use_container_width=True)

    if add_empty:
        new_row = pd.DataFrame([{
            "id": None, "nombre": "", "telefono": "", "email": "",
            "url_anuncio": "", "titulo_vehiculo": "", "precio": "",
            "estado": "Pendiente", "fecha_contacto": "", "fecha_seguimiento": "",
            "notas": "", "created_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }])
        crm_df = pd.concat([crm_df, new_row], ignore_index=True)
        st.session_state["crm_draft"] = crm_df

    # Usar draft si existe
    edit_source = st.session_state.get("crm_draft", crm_df)

    # Añadir columna de borrado al DataFrame que se edita
    edit_with_del = edit_source.copy()
    edit_with_del.insert(0, "_borrar", False)

    # ── Tabla editable ────────────────────────────────────────────────────
    edited = st.data_editor(
        edit_with_del,
        use_container_width=True,
        height=540,
        num_rows="dynamic",
        column_config={
            "_borrar":          st.column_config.CheckboxColumn("🗑️ Borrar", default=False, width="small"),
            "id":               st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "nombre":           st.column_config.TextColumn("Nombre", width="medium"),
            "telefono":         st.column_config.TextColumn("Teléfono", width="medium"),
            "email":            st.column_config.TextColumn("Email", width="medium"),
            "url_anuncio":      st.column_config.LinkColumn("Anuncio", display_text="Ver 🔗", width="small"),
            "titulo_vehiculo":  st.column_config.TextColumn("Vehículo", width="large"),
            "precio":           st.column_config.TextColumn("Precio", width="small"),
            "estado":           st.column_config.SelectboxColumn("Estado", options=CRM_ESTADOS, width="medium"),
            "fecha_contacto":   st.column_config.TextColumn("Contactado", width="medium"),
            "fecha_seguimiento":st.column_config.TextColumn("Seguimiento", width="medium"),
            "notas":            st.column_config.TextColumn("Notas", width="large"),
            "created_at":       st.column_config.TextColumn("Creado", disabled=True, width="medium"),
        },
        column_order=[
            "_borrar", "id", "nombre", "estado", "telefono", "email",
            "titulo_vehiculo", "precio", "url_anuncio",
            "fecha_contacto", "fecha_seguimiento", "notas", "created_at",
        ],
        hide_index=True,
        key="crm_editor",
    )

    # Botón guardar
    if save_btn:
        clean = edited.drop(columns=["_borrar"], errors="ignore")
        save_crm(clean)
        st.session_state.pop("crm_draft", None)
        st.success("✅ CRM guardado correctamente.")
        st.rerun()

    # Botón eliminar marcadas
    if del_btn:
        rows_to_keep = edited[~edited["_borrar"]].drop(columns=["_borrar"], errors="ignore")
        n_deleted = len(edited) - len(rows_to_keep)
        if n_deleted == 0:
            st.warning("No hay filas marcadas para borrar.")
        else:
            save_crm(rows_to_keep)
            st.session_state.pop("crm_draft", None)
            st.success(f"✅ {n_deleted} fila(s) eliminada(s).")
            st.rerun()

    # Exportar CSV
    st.divider()
    exp_buf = io.StringIO()
    crm_df.to_csv(exp_buf, index=False, encoding="utf-8-sig")
    td.download_button(
        "⬇️ CSV",
        data=exp_buf.getvalue(),
        file_name=f"crm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )
