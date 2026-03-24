"""
app.py — CochesNet Pro
Scraper + Historial persistente + CRM de contactos.
"""
import io
import logging
from datetime import datetime

import pandas as pd
import streamlit as st

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

# ── CSS ───────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

/* ── Hide default Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.5rem !important; padding-bottom: 2rem !important; }

/* ── Background ── */
.stApp { background: #F0F4F8; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0F2544 0%, #1B3A6B 100%);
    border-right: none;
}
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span:not(.st-emotion-cache-10trblm),
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stMultiSelect label,
section[data-testid="stSidebar"] .stNumberInput label,
section[data-testid="stSidebar"] .stSlider label {
    color: #CBD5E1 !important;
}
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: #FFFFFF !important;
    font-weight: 700 !important;
}
section[data-testid="stSidebar"] [data-testid="stExpander"] {
    background: rgba(255,255,255,0.07) !important;
    border: 1px solid rgba(255,255,255,0.12) !important;
    border-radius: 10px !important;
    margin-bottom: 6px !important;
}
section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.15) !important;
}
section[data-testid="stSidebar"] .stCaption {
    color: #94A3B8 !important;
}

/* ── Header card ── */
.app-header {
    background: linear-gradient(135deg, #E8420F 0%, #C0320A 100%);
    border-radius: 14px;
    padding: 1.2rem 2rem;
    margin-bottom: 1.5rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 4px 20px rgba(232,66,15,0.35);
}
.app-header h1 { color: white; margin: 0; font-size: 1.6rem; font-weight: 800; }
.app-header p  { color: rgba(255,255,255,0.85); margin: 0; font-size: 0.85rem; }
.header-user   { color: rgba(255,255,255,0.9); font-size: 0.9rem; text-align: right; }

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: white !important;
    border-radius: 12px !important;
    padding: 1.2rem 1.5rem !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.07) !important;
    border: 1px solid #E2E8F0 !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 1.7rem !important;
    font-weight: 700 !important;
    color: #0F2544 !important;
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] {
    color: #64748B !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}

/* ── Buttons ── */
.stButton > button {
    border-radius: 8px !important;
    font-weight: 600 !important;
    transition: all 0.2s ease !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #E8420F, #C0320A) !important;
    border: none !important;
    color: white !important;
    box-shadow: 0 2px 10px rgba(232,66,15,0.3) !important;
}
.stButton > button[kind="primary"]:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 18px rgba(232,66,15,0.45) !important;
}
.stButton > button[kind="secondary"] {
    background: white !important;
    border: 1.5px solid #CBD5E1 !important;
    color: #334155 !important;
}
.stButton > button[kind="secondary"]:hover {
    border-color: #E8420F !important;
    color: #E8420F !important;
    background: #FFF5F2 !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: transparent;
    border-bottom: 2px solid #E2E8F0;
    padding-bottom: 0;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0 !important;
    padding: 0.6rem 1.5rem !important;
    font-weight: 600 !important;
    color: #64748B !important;
    background: transparent !important;
    border: none !important;
}
.stTabs [aria-selected="true"] {
    background: white !important;
    color: #E8420F !important;
    border-bottom: 3px solid #E8420F !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: white;
    border-radius: 0 12px 12px 12px;
    padding: 1.5rem !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    border: 1px solid #E2E8F0;
}

/* ── Filter box ── */
.filter-box {
    background: #F8FAFC;
    border: 1px solid #E2E8F0;
    border-radius: 12px;
    padding: 1rem 1.25rem;
    margin-bottom: 1rem;
}

/* ── History rows ── */
.history-row {
    background: white;
    border: 1px solid #E2E8F0;
    border-radius: 10px;
    padding: 0.9rem 1.2rem;
    margin-bottom: 0.5rem;
    display: flex;
    align-items: center;
    transition: box-shadow 0.2s;
}
.history-row:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.08); }

/* ── Login page ── */
.login-wrapper {
    min-height: 75vh;
    display: flex;
    align-items: center;
    justify-content: center;
}
.login-card {
    background: white;
    border-radius: 20px;
    padding: 3rem;
    width: 100%;
    max-width: 420px;
    box-shadow: 0 20px 60px rgba(0,0,0,0.12);
    text-align: center;
}
.login-card h2 { color: #0F2544; font-size: 1.6rem; font-weight: 800; margin-bottom: 0.3rem; }
.login-card p  { color: #94A3B8; font-size: 0.88rem; margin-bottom: 1.5rem; }
.login-logo    { font-size: 3rem; margin-bottom: 1rem; }

/* ── Info / success / warning pills ── */
.stAlert { border-radius: 10px !important; }

/* ── DataFrames / tables ── */
[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }
</style>
""",
    unsafe_allow_html=True,
)

# ── DB init + cleanup ──────────────────────────────────────────────────────
init_db()
if "db_cleaned" not in st.session_state:
    cleanup_old_scrapes(days=st.session_state.get("history_ttl_days", 7))
    st.session_state.db_cleaned = True

# ═══════════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════════

def _check_credentials(username: str, password: str) -> bool:
    try:
        return (
            username == st.secrets["credentials"]["username"]
            and password == st.secrets["credentials"]["password"]
        )
    except Exception:
        return username == "admin" and password == "cochesnet2024"


if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "auth_user" not in st.session_state:
    st.session_state.auth_user = ""

if not st.session_state.authenticated:
    # ── Login page ────────────────────────────────────────────────────────
    _, col, _ = st.columns([1, 1.4, 1])
    with col:
        st.markdown('<div class="login-logo">🚗</div>', unsafe_allow_html=True)
        st.markdown("## CochesNet Pro")
        st.markdown("Introduce tus credenciales para acceder")
        st.divider()
        username = st.text_input("Usuario", placeholder="admin", key="login_user")
        password = st.text_input("Contraseña", type="password", placeholder="••••••••", key="login_pass")
        if st.button("Entrar", type="primary", use_container_width=True):
            if _check_credentials(username, password):
                st.session_state.authenticated = True
                st.session_state.auth_user = username
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos.")
        st.caption("💡 Edita `.streamlit/secrets.toml` para cambiar las credenciales.")
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════
# MAIN APP  (authenticated)
# ═══════════════════════════════════════════════════════════════════════════

# ── Header ────────────────────────────────────────────────────────────────
col_title, col_user = st.columns([5, 1])
with col_title:
    st.markdown(
        f"""<div class="app-header">
            <div>
                <h1>🚗 CochesNet Pro</h1>
                <p>Scraper · Historial · CRM de contactos</p>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )
with col_user:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f"👤 **{st.session_state.auth_user}**")
    if st.button("Cerrar sesión", key="logout"):
        st.session_state.authenticated = False
        st.rerun()

# ── Cached resources ──────────────────────────────────────────────────────
@st.cache_resource
def get_scraper() -> CochesNetScraper:
    return CochesNetScraper(delay=1.5)


@st.cache_data(ttl=3600, show_spinner="Cargando marcas…")
def fetch_makes() -> list:
    return get_scraper().get_makes()


scraper = get_scraper()
makes = fetch_makes()
make_label_to_obj = {m["label"]: m for m in makes}

# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR — filtros de búsqueda
# ═══════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 🔍 Filtros de búsqueda")

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
        price_from = c1.number_input("Mínimo", 0, 2_000_000, 0, 500, help="0 = sin límite")
        price_to   = c2.number_input("Máximo", 0, 2_000_000, 0, 500, help="0 = sin límite")

    with st.expander("📅 Año"):
        year_opts = ["(Cualquiera)"] + [str(y) for y in range(2025, 1989, -1)]
        c1, c2 = st.columns(2)
        year_from_sel = c1.selectbox("Desde", year_opts, key="year_from")
        year_to_sel   = c2.selectbox("Hasta", year_opts, key="year_to")

    with st.expander("🛣️ Kilómetros"):
        c1, c2 = st.columns(2)
        km_from = c1.number_input("Mín km", 0, 1_000_000, 0, 5_000, help="0 = sin límite")
        km_to   = c2.number_input("Máx km", 0, 1_000_000, 0, 5_000, help="0 = sin límite")

    with st.expander("⚡ Potencia (CV)"):
        c1, c2 = st.columns(2)
        hp_from = c1.number_input("Mín CV", 0, 2_000, 0, 10, help="0 = sin límite")
        hp_to   = c2.number_input("Máx CV", 0, 2_000, 0, 10, help="0 = sin límite")

    with st.expander("⛽ Combustible y Carrocería"):
        selected_fuels  = st.multiselect("Combustible", list(FUEL_TYPES.keys()), default=[])
        selected_bodies = st.multiselect("Carrocería",  list(BODY_TYPES.keys()), default=[])

    with st.expander("⚙️ Más opciones"):
        transmission = st.selectbox("Transmisión", list(TRANSMISSIONS.keys()))
        seller_type  = st.selectbox("Tipo de vendedor", ["Todos", "Particular", "Profesional"])
        has_warranty = st.checkbox("Solo con garantía")

    st.divider()
    st.markdown("### ⚙️ Opciones de raspado")
    sort_by   = st.selectbox("Ordenar por", list(SORT_OPTIONS.keys()))
    max_pages = st.slider("Páginas a raspar", 1, 200, 5, help="~30 anuncios/página")
    st.caption(f"Máximo ~{max_pages * 30:,} anuncios")

    scrape_btn = st.button("🚀 Iniciar Raspado", type="primary", use_container_width=True)

    st.divider()
    st.markdown("### 🗂️ Resumen")
    n_hist = count_scrapes()
    n_crm  = count_crm()
    st.metric("Scrapes guardados", n_hist)
    st.metric("Contactos CRM", n_crm)


# ── Build filters ─────────────────────────────────────────────────────────
def build_filters() -> dict:
    f: dict = {}
    if selected_make:
        f["make_slug"] = selected_make["slug"]
        if selected_model_label != "(Todos los modelos)":
            model_obj = next(
                (m for m in selected_make.get("models", []) if m["label"] == selected_model_label),
                None,
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
    """Human-readable one-liner of active filters."""
    parts = []
    if f.get("make_slug"): parts.append(f.get("make_slug", "").replace("-", " ").title())
    if f.get("model_slug"): parts.append(f.get("model_slug", "").replace("-", " ").title())
    if f.get("price_from") or f.get("price_to"):
        parts.append(f"€{f.get('price_from',0):,}–{f.get('price_to','∞')}")
    if f.get("year_from") or f.get("year_to"):
        parts.append(f"{f.get('year_from','?')}–{f.get('year_to','?')}")
    if f.get("seller_type"):
        parts.append(f.get("seller_type", "").capitalize())
    if not parts:
        parts.append("Todos los coches")
    return " · ".join(parts)


# ── Helper: display results with filters ─────────────────────────────────
def show_results(df: pd.DataFrame, scrape_id: int | None = None):
    """Render the results DataFrame with post-scraping filters."""
    if df.empty:
        st.warning("No hay anuncios en este resultado.")
        return

    # ── Post-scraping result filters ──────────────────────────────────────
    with st.expander("🔧 Filtrar resultados", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        fc4, fc5, fc6 = st.columns(3)

        # Price filter
        prices_raw = pd.to_numeric(
            df.get("precio_€", pd.Series(dtype=float)).astype(str).str.replace(r"[^\d]", "", regex=True),
            errors="coerce",
        ).fillna(0)
        p_min = int(prices_raw.min()) if not prices_raw.empty else 0
        p_max = int(prices_raw.max()) if not prices_raw.empty else 200_000
        if p_min == p_max: p_max = p_min + 1
        f_price = fc1.slider("💶 Precio (€)", p_min, p_max, (p_min, p_max), step=500, key=f"fp_{scrape_id}")

        # Year filter
        years_raw = pd.to_numeric(df.get("año", pd.Series(dtype=float)), errors="coerce").fillna(2000)
        y_min = int(years_raw.min()) if not years_raw.empty else 1990
        y_max = int(years_raw.max()) if not years_raw.empty else 2025
        if y_min == y_max: y_max = y_min + 1
        f_year = fc2.slider("📅 Año", y_min, y_max, (y_min, y_max), key=f"fy_{scrape_id}")

        # KM filter
        km_raw = pd.to_numeric(
            df.get("kilometros", pd.Series(dtype=float)).astype(str).str.replace(r"[^\d]", "", regex=True),
            errors="coerce",
        ).fillna(0)
        k_min = int(km_raw.min()) if not km_raw.empty else 0
        k_max = int(km_raw.max()) if not km_raw.empty else 300_000
        if k_min == k_max: k_max = k_min + 1
        f_km = fc3.slider("🛣️ Kilómetros", k_min, k_max, (k_min, k_max), step=5_000, key=f"fk_{scrape_id}")

        # Combustible
        combust_vals = sorted(df["combustible"].dropna().unique().tolist()) if "combustible" in df.columns else []
        f_fuel = fc4.multiselect("⛽ Combustible", combust_vals, default=[], key=f"ff_{scrape_id}")

        # Seller type (post-filter on vendedor_profesional field)
        f_seller = fc5.selectbox(
            "👤 Vendedor", ["Todos", "Particular", "Profesional"], key=f"fs_{scrape_id}"
        )

        # Province filter
        prov_vals = sorted(df["provincia"].dropna().unique().tolist()) if "provincia" in df.columns else []
        f_prov = fc6.multiselect("📍 Provincia", prov_vals, default=[], key=f"fprov_{scrape_id}")

        # Text search in title
        f_txt = st.text_input("🔎 Buscar en título", placeholder="ej: cambio de aceite, techo panorámico…", key=f"ftxt_{scrape_id}")

    # ── Apply filters ─────────────────────────────────────────────────────
    mask = pd.Series([True] * len(df), index=df.index)

    if "precio_€" in df.columns:
        p_col = pd.to_numeric(
            df["precio_€"].astype(str).str.replace(r"[^\d]", "", regex=True), errors="coerce"
        ).fillna(0)
        mask &= p_col.between(f_price[0], f_price[1])

    if "año" in df.columns:
        y_col = pd.to_numeric(df["año"], errors="coerce").fillna(0)
        mask &= y_col.between(f_year[0], f_year[1])

    if "kilometros" in df.columns:
        k_col = pd.to_numeric(
            df["kilometros"].astype(str).str.replace(r"[^\d]", "", regex=True), errors="coerce"
        ).fillna(0)
        mask &= k_col.between(f_km[0], f_km[1])

    if f_fuel and "combustible" in df.columns:
        mask &= df["combustible"].isin(f_fuel)

    if f_seller != "Todos" and "vendedor_profesional" in df.columns:
        is_pro = df["vendedor_profesional"].apply(
            lambda x: str(x).lower() in ("true", "1", "yes", "sí")
        )
        mask &= is_pro if f_seller == "Profesional" else ~is_pro

    if f_prov and "provincia" in df.columns:
        mask &= df["provincia"].isin(f_prov)

    if f_txt.strip() and "titulo" in df.columns:
        mask &= df["titulo"].astype(str).str.contains(f_txt.strip(), case=False, na=False)

    filtered = df[mask].copy()

    # ── Metrics ───────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Anuncios", f"{len(filtered):,}")

    prices_f = pd.to_numeric(
        filtered.get("precio_€", pd.Series()).astype(str).str.replace(r"[^\d]", "", regex=True),
        errors="coerce",
    ).dropna()
    if not prices_f.empty:
        c2.metric("Precio medio", f"{prices_f.mean():,.0f} €")

    years_f = pd.to_numeric(filtered.get("año", pd.Series()), errors="coerce").dropna()
    if not years_f.empty:
        c3.metric("Año medio", f"{years_f.mean():.0f}")

    km_f = pd.to_numeric(
        filtered.get("kilometros", pd.Series()).astype(str).str.replace(r"[^\d]", "", regex=True),
        errors="coerce",
    ).dropna()
    if not km_f.empty:
        c4.metric("KM medios", f"{km_f.mean():,.0f}")

    st.markdown(f"<small style='color:#64748B'>Mostrando **{len(filtered):,}** de **{len(df):,}** anuncios</small>", unsafe_allow_html=True)

    # ── Column selector ───────────────────────────────────────────────────
    all_cols = list(filtered.columns)
    default_cols = [c for c in ["titulo", "precio_€", "año", "kilometros", "combustible",
                                 "carroceria", "ciudad", "provincia", "vendedor_profesional",
                                 "telefono", "url"] if c in all_cols]
    shown_cols = st.multiselect(
        "Columnas visibles", all_cols, default=default_cols, key=f"cols_{scrape_id}"
    )
    if not shown_cols:
        shown_cols = default_cols

    st.dataframe(
        filtered[shown_cols],
        use_container_width=True,
        height=480,
        column_config={
            "url": st.column_config.LinkColumn("URL", display_text="Ver anuncio"),
            "precio_€": st.column_config.NumberColumn("Precio €", format="%d €"),
            "año": st.column_config.NumberColumn("Año"),
            "kilometros": st.column_config.NumberColumn("Kilómetros"),
            "vendedor_profesional": st.column_config.CheckboxColumn("Profesional"),
        },
    )

    # ── Actions ───────────────────────────────────────────────────────────
    col_dl, col_crm = st.columns([2, 1])
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    buf = io.StringIO()
    filtered.to_csv(buf, index=False, encoding="utf-8-sig")
    col_dl.download_button(
        "⬇️ Descargar CSV",
        data=buf.getvalue(),
        file_name=f"cochesnet_{ts}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    if col_crm.button("➕ Añadir selección al CRM", use_container_width=True, key=f"addcrm_{scrape_id}"):
        added = 0
        for _, row in filtered.head(50).iterrows():
            add_crm_from_car(row.to_dict())
            added += 1
        st.success(f"✅ {added} anuncios añadidos al CRM.")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════════════════
tab_scraper, tab_historial, tab_crm = st.tabs(["🔍 Scraper", "📋 Historial", "👥 CRM"])

# ──────────────────────────────────────────────────────────────────────────
# TAB 1 — SCRAPER
# ──────────────────────────────────────────────────────────────────────────
with tab_scraper:
    if scrape_btn:
        filters = build_filters()
        ftxt = filters_summary(filters)

        progress_bar = st.progress(0.0)
        status = st.empty()

        def on_progress(page: int, total: int, found: int):
            progress_bar.progress(page / total)
            status.info(f"Raspando página {page}/{total} — {found} anuncios encontrados…")

        status.info("⏳ Iniciando raspado…")
        df = scraper.scrape(filters, max_pages=max_pages, progress_callback=on_progress)
        progress_bar.progress(1.0)

        if df.empty:
            status.warning("No se encontraron anuncios con los filtros seleccionados.")
        else:
            status.success(f"✅ Completado: **{len(df):,} anuncios** encontrados.")
            # Auto-save to history
            sid = save_scrape(ftxt, df)
            st.session_state["last_scrape_id"] = sid
            st.session_state[f"scrape_{sid}"] = df
            st.info(f"💾 Guardado en historial (ID #{sid}) — *'{ftxt}'*")
            show_results(df, scrape_id=sid)

    elif "last_scrape_id" in st.session_state:
        sid = st.session_state["last_scrape_id"]
        df_cached = st.session_state.get(f"scrape_{sid}")
        if df_cached is not None and not df_cached.empty:
            st.info("📌 Mostrando los resultados del último raspado. Pulsa **Iniciar Raspado** para buscar de nuevo.")
            show_results(df_cached, scrape_id=sid)
        else:
            st.info("Configura los filtros en la barra lateral y pulsa **🚀 Iniciar Raspado**.")
    else:
        st.markdown(
            """
            <div style="text-align:center; padding: 3rem 1rem; color:#94A3B8;">
                <div style="font-size:4rem; margin-bottom:1rem;">🚗</div>
                <h3 style="color:#1B3A6B; margin-bottom:0.5rem;">Listo para buscar</h3>
                <p>Configura los filtros en la barra lateral y pulsa <strong>🚀 Iniciar Raspado</strong>.</p>
                <p style="font-size:0.8rem;">Los resultados se guardan automáticamente en el historial.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ──────────────────────────────────────────────────────────────────────────
# TAB 2 — HISTORIAL
# ──────────────────────────────────────────────────────────────────────────
with tab_historial:
    st.markdown("### 📋 Historial de raspados")

    col_ttl, col_refresh = st.columns([3, 1])
    history_ttl = col_ttl.slider(
        "Borrar automáticamente después de (días)", 1, 30, 7, key="history_ttl_days"
    )
    if col_refresh.button("🗑️ Limpiar ahora", key="clean_hist"):
        cleanup_old_scrapes(days=history_ttl)
        st.success(f"Eliminados scrapes con más de {history_ttl} días.")
        st.rerun()

    scrapes = list_scrapes()
    if not scrapes:
        st.markdown(
            """<div style="text-align:center;padding:3rem;color:#94A3B8;">
                <div style="font-size:3rem">📭</div>
                <p>El historial está vacío.<br>Los raspados se guardan automáticamente al completarse.</p>
            </div>""",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(f"**{len(scrapes)} raspado(s) guardado(s)**")
        for s in scrapes:
            c1, c2, c3, c4 = st.columns([2.5, 3, 1, 1])
            c1.markdown(f"🕐 `{s['timestamp']}`")
            c2.markdown(f"🏷️ {s['filters_txt'] or '—'} · **{s['num_results']:,} anuncios**")

            load_key   = f"load_{s['id']}"
            delete_key = f"del_{s['id']}"

            if c3.button("📂 Cargar", key=load_key, use_container_width=True):
                with st.spinner("Cargando…"):
                    df_hist = load_scrape(s["id"])
                if df_hist is not None:
                    st.session_state["last_scrape_id"] = s["id"]
                    st.session_state[f"scrape_{s['id']}"] = df_hist
                    st.success("✅ Resultado cargado. Ve a la pestaña **Scraper** para verlo.")
                else:
                    st.error("No se pudo cargar el scrape.")

            if c4.button("🗑️", key=delete_key, use_container_width=True):
                delete_scrape(s["id"])
                # Remove from session too
                st.session_state.pop(f"scrape_{s['id']}", None)
                if st.session_state.get("last_scrape_id") == s["id"]:
                    st.session_state.pop("last_scrape_id", None)
                st.rerun()

            st.markdown("<hr style='margin:4px 0; border-color:#F0F4F8'>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────
# TAB 3 — CRM
# ──────────────────────────────────────────────────────────────────────────
with tab_crm:
    st.markdown("### 👥 CRM — Registro de contactos")
    st.caption("Aquí puedes llevar un seguimiento de todas las personas que contactas. Edita directamente en la tabla y guarda los cambios.")

    # Load CRM from DB
    crm_df = get_crm()

    # ── Actions toolbar ───────────────────────────────────────────────────
    ca, cb, cc = st.columns([1, 1, 1])

    if ca.button("➕ Añadir contacto vacío", use_container_width=True):
        new_row = pd.DataFrame([{
            "id": None, "nombre": "", "telefono": "", "email": "",
            "url_anuncio": "", "titulo_vehiculo": "", "precio": "",
            "estado": "Pendiente", "fecha_contacto": "", "fecha_seguimiento": "", "notas": "",
        }])
        crm_df = pd.concat([crm_df, new_row], ignore_index=True)
        st.session_state["crm_unsaved"] = crm_df

    if cc.button("⬇️ Exportar CSV", use_container_width=True):
        buf = io.StringIO()
        crm_df.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button(
            "Descargar CRM.csv",
            data=buf.getvalue(),
            file_name=f"crm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

    # Use unsaved df if exists
    edit_df = st.session_state.get("crm_unsaved", crm_df)

    # ── Editable table ────────────────────────────────────────────────────
    edited = st.data_editor(
        edit_df,
        use_container_width=True,
        height=520,
        num_rows="dynamic",
        column_config={
            "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "nombre": st.column_config.TextColumn("Nombre", width="medium"),
            "telefono": st.column_config.TextColumn("Teléfono", width="medium"),
            "email": st.column_config.TextColumn("Email", width="medium"),
            "url_anuncio": st.column_config.LinkColumn("Anuncio", display_text="Ver", width="small"),
            "titulo_vehiculo": st.column_config.TextColumn("Vehículo", width="large"),
            "precio": st.column_config.TextColumn("Precio", width="small"),
            "estado": st.column_config.SelectboxColumn(
                "Estado", options=CRM_ESTADOS, width="medium"
            ),
            "fecha_contacto": st.column_config.TextColumn("Contactado", width="medium"),
            "fecha_seguimiento": st.column_config.TextColumn("Seguimiento", width="medium"),
            "notas": st.column_config.TextColumn("Notas", width="large"),
        },
        column_order=[
            "id", "nombre", "estado", "telefono", "email",
            "titulo_vehiculo", "precio", "url_anuncio",
            "fecha_contacto", "fecha_seguimiento", "notas",
        ],
        hide_index=True,
        key="crm_editor",
    )

    # ── Save button ───────────────────────────────────────────────────────
    if cb.button("💾 Guardar cambios", type="primary", use_container_width=True):
        save_crm(edited)
        st.session_state.pop("crm_unsaved", None)
        st.success("✅ CRM guardado correctamente.")
        st.rerun()
