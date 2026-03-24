"""
app.py — CochesNet Pro
Scraper · Historial · CRM · Notas
"""
from __future__ import annotations

import hashlib
import io
import logging
from datetime import datetime

import pandas as pd
import streamlit as st

from database import (
    CRM_ESTADOS,
    CRM_TIPOS_VENDEDOR,
    add_crm_from_car,
    cleanup_old_scrapes,
    count_crm,
    count_scrapes,
    delete_note,
    delete_scrape,
    get_crm,
    get_note,
    init_db,
    list_notes,
    list_scrapes,
    load_scrape,
    save_crm,
    save_note,
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
# CSS — solo decoración (sombras, radios, tabs)
# Los COLORES los gestiona config.toml [theme] — nunca aquí
# ══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
#MainMenu, footer, header[data-testid="stHeader"] { display: none !important; }
.block-container { padding: 1.6rem 2.2rem 3rem !important; max-width: 1440px !important; }

/* ── Sidebar ── solo fondo oscuro; el texto lo gestiona el tema */
section[data-testid="stSidebar"] { background: #1E293B !important; }
section[data-testid="stSidebar"] * { color: #CBD5E1 !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] strong { color: #F8FAFC !important; }
section[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.1) !important; }
section[data-testid="stSidebar"] [data-testid="stExpander"] {
    background: rgba(255,255,255,0.06) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important; margin-bottom: 6px !important;
}
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background: rgba(255,255,255,0.09) !important;
    border-color: rgba(255,255,255,0.18) !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 3px; border-bottom: 2px solid #CBD5E1; padding-bottom: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0 !important;
    padding: 0.6rem 1.5rem !important; font-weight: 600 !important;
    border: 1px solid #CBD5E1 !important; border-bottom: none !important;
    background: #E2E8F0 !important; margin-bottom: -2px !important;
}
.stTabs [aria-selected="true"] { background: white !important; border-bottom: 2px solid white !important; }
.stTabs [data-baseweb="tab-panel"] {
    background: white !important; border-radius: 0 12px 12px 12px !important;
    padding: 1.8rem !important; border: 1px solid #CBD5E1 !important; border-top: none !important;
    box-shadow: 0 2px 10px rgba(0,0,0,0.05) !important;
}

/* ── Cards de métrica ── */
[data-testid="metric-container"] {
    background: white !important; border-radius: 12px !important;
    border: 1px solid #E2E8F0 !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06), 0 4px 12px rgba(0,0,0,0.04) !important;
    padding: 1rem 1.4rem !important;
}

/* ── Botón primario ── */
.stButton > button[kind="primary"] {
    border-radius: 8px !important; font-weight: 600 !important;
    box-shadow: 0 2px 8px rgba(232,66,15,0.3) !important;
}
.stButton > button[kind="primary"]:hover { transform: translateY(-1px) !important; }
.stButton > button { border-radius: 8px !important; }

/* ── Progress bar ── */
.stProgress > div > div { border-radius: 99px; }
.stProgress > div { border-radius: 99px; }

/* ── Expanders ── */
[data-testid="stExpander"] { border-radius: 10px !important; }

/* ── Header card ── */
.app-header {
    background: linear-gradient(135deg, #E8420F, #9B2406);
    border-radius: 14px; padding: 1rem 1.8rem;
    box-shadow: 0 4px 20px rgba(232,66,15,0.3); margin-bottom: .5rem;
    display: flex; align-items: center; gap: 1rem;
}
.app-header-title { color: white !important; font-size: 1.4rem; font-weight: 800; }
.app-header-sub   { color: rgba(255,255,255,.75) !important; font-size: .8rem; }

/* ── Notes editor ── */
.note-card {
    background: white; border: 1px solid #E2E8F0; border-radius: 10px;
    padding: .7rem 1rem; margin-bottom: .4rem; cursor: pointer;
}
.note-card:hover { border-color: #E8420F; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# DB
# ══════════════════════════════════════════════════════════════════════════
init_db()
if "db_cleaned" not in st.session_state:
    cleanup_old_scrapes(days=7)
    st.session_state.db_cleaned = True

# ══════════════════════════════════════════════════════════════════════════
# AUTH — token determinista en query param (persiste como bookmark)
# ══════════════════════════════════════════════════════════════════════════
def _get_creds():
    try:
        return st.secrets["credentials"]["username"], st.secrets["credentials"]["password"]
    except Exception:
        return "admin", "cochesnet2024"


def _make_token(user: str, pw: str) -> str:
    seed = f"{user}:{pw}:CochesNetPro2024"
    return hashlib.sha256(seed.encode()).hexdigest()[:40]


def _is_authenticated() -> bool:
    if st.session_state.get("authenticated"):
        return True
    user, pw = _get_creds()
    valid = _make_token(user, pw)
    return st.query_params.get("auth") == valid


def _login(username: str, password: str) -> bool:
    user, pw = _get_creds()
    if username == user and password == pw:
        token = _make_token(user, pw)
        st.query_params["auth"] = token
        st.session_state.authenticated = True
        return True
    return False


def _logout():
    st.session_state.authenticated = False
    st.query_params.clear()
    st.rerun()


# ── Login page ─────────────────────────────────────────────────────────────
if not _is_authenticated():
    st.session_state.authenticated = False
    _, col, _ = st.columns([1, 1.1, 1])
    with col:
        st.markdown("<div style='text-align:center;padding:2rem 0 .5rem'>"
                    "<div style='font-size:3.5rem'>🚗</div>"
                    "<h2 style='margin:.3rem 0 .2rem'>CochesNet Pro</h2>"
                    "<p style='color:#64748B;font-size:.9rem'>Panel de scraping y CRM</p>"
                    "</div>", unsafe_allow_html=True)
        with st.form("login_form"):
            username = st.text_input("Usuario", placeholder="admin")
            password = st.text_input("Contraseña", type="password", placeholder="••••••••")
            submitted = st.form_submit_button("Entrar →", type="primary", use_container_width=True)
            if submitted:
                if _login(username, password):
                    st.rerun()
                else:
                    st.error("Usuario o contraseña incorrectos.")
        st.caption("💡 Tras iniciar sesión, guarda la URL — incluye un token que te mantendrá conectado.")
    st.stop()

# ══════════════════════════════════════════════════════════════════════════
# MAIN APP
# ══════════════════════════════════════════════════════════════════════════
auth_user, _ = _get_creds()

# ── Cached resources ──────────────────────────────────────────────────────
@st.cache_resource
def get_scraper() -> CochesNetScraper:
    return CochesNetScraper(delay=1.5)

@st.cache_data(ttl=3600, show_spinner="Cargando marcas…")
def fetch_makes() -> list:
    return get_scraper().get_makes()

scraper = get_scraper()
makes   = fetch_makes()
make_label_to_obj = {m["label"]: m for m in makes}

# ── Header ─────────────────────────────────────────────────────────────────
hc1, hc2 = st.columns([5, 1])
with hc1:
    st.markdown(
        '<div class="app-header">'
        '<div style="font-size:2rem">🚗</div>'
        '<div><div class="app-header-title">CochesNet Pro</div>'
        '<div class="app-header-sub">Scraper · Historial · CRM · Notas</div></div>'
        '</div>', unsafe_allow_html=True)
with hc2:
    st.markdown(f"<div style='padding-top:1.1rem;text-align:right'>👤 <b>{auth_user}</b></div>",
                unsafe_allow_html=True)
    if st.button("Salir", key="logout_btn"):
        _logout()

# ══════════════════════════════════════════════════════════════════════════
# SIDEBAR
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
        selected_model_label = st.selectbox("Modelo", model_options, key="model",
                                             disabled=(selected_make is None))

    with st.expander("💶 Precio (€)"):
        c1, c2 = st.columns(2)
        price_from = c1.number_input("Mín", 0, 2_000_000, 0, 500, help="0=sin límite", key="pf")
        price_to   = c2.number_input("Máx", 0, 2_000_000, 0, 500, help="0=sin límite", key="pt")

    with st.expander("📅 Año"):
        year_opts = ["(Cualquiera)"] + [str(y) for y in range(2025, 1989, -1)]
        c1, c2 = st.columns(2)
        year_from_sel = c1.selectbox("Desde", year_opts, key="yf")
        year_to_sel   = c2.selectbox("Hasta", year_opts, key="yt")

    with st.expander("🛣️ Kilómetros"):
        c1, c2 = st.columns(2)
        km_from = c1.number_input("Mín", 0, 1_000_000, 0, 5_000, help="0=sin límite", key="kmf")
        km_to   = c2.number_input("Máx", 0, 1_000_000, 0, 5_000, help="0=sin límite", key="kmt")

    with st.expander("⚡ Potencia (CV)"):
        c1, c2 = st.columns(2)
        hp_from = c1.number_input("Mín", 0, 2_000, 0, 10, help="0=sin límite", key="hpf")
        hp_to   = c2.number_input("Máx", 0, 2_000, 0, 10, help="0=sin límite", key="hpt")

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
            obj = next((m for m in selected_make.get("models", [])
                        if m["label"] == selected_model_label), None)
            if obj:
                f["model_slug"] = obj["slug"]
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
    if has_warranty: f["has_warranty"] = True
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


def _num(series: pd.Series, strip: bool = False) -> pd.Series:
    s = series.astype(str)
    if strip:
        s = s.str.replace(r"[^\d]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


# ══════════════════════════════════════════════════════════════════════════
# COMPONENTE: resultados con filtros
# ══════════════════════════════════════════════════════════════════════════
def show_results(df: pd.DataFrame, scrape_id: int):
    if df.empty:
        st.warning("No hay anuncios en este resultado.")
        return
    uid = str(scrape_id)

    with st.expander("🔧 Filtrar resultados", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        fc4, fc5, fc6 = st.columns(3)

        prices_raw = _num(df.get("precio_€", pd.Series(dtype=float)), strip=True).fillna(0)
        p_min, p_max = int(prices_raw.min()), int(prices_raw.max())
        if p_min == p_max: p_max += 1
        f_price = fc1.slider("💶 Precio €", p_min, p_max, (p_min, p_max), step=500, key=f"fp{uid}")

        years_raw = _num(df.get("año", pd.Series(dtype=float))).fillna(2000)
        y_min, y_max = int(years_raw.min()), int(years_raw.max())
        if y_min == y_max: y_max += 1
        f_year = fc2.slider("📅 Año", y_min, y_max, (y_min, y_max), key=f"fy{uid}")

        km_raw = _num(df.get("kilometros", pd.Series(dtype=float)), strip=True).fillna(0)
        k_min, k_max = int(km_raw.min()), int(km_raw.max())
        if k_min == k_max: k_max += 1
        f_km = fc3.slider("🛣️ Km", k_min, k_max, (k_min, k_max), step=5_000, key=f"fk{uid}")

        combust = sorted(df["combustible"].dropna().unique()) if "combustible" in df.columns else []
        f_fuel = fc4.multiselect("⛽ Combustible", combust, default=[], key=f"ff{uid}")

        f_seller = fc5.selectbox("👤 Vendedor", ["Todos", "Particular", "Profesional"], key=f"fs{uid}")

        provs = sorted(df["provincia"].dropna().unique()) if "provincia" in df.columns else []
        f_prov = fc6.multiselect("📍 Provincia", provs, default=[], key=f"fprov{uid}")

        f_txt = st.text_input("🔎 Buscar en título", placeholder="ej: automático, techo solar…", key=f"ftxt{uid}")

    # Aplicar filtros
    mask = pd.Series(True, index=df.index)
    if "precio_€" in df.columns:
        mask &= _num(df["precio_€"], strip=True).fillna(0).between(f_price[0], f_price[1])
    if "año" in df.columns:
        mask &= _num(df["año"]).fillna(0).between(f_year[0], f_year[1])
    if "kilometros" in df.columns:
        mask &= _num(df["kilometros"], strip=True).fillna(0).between(f_km[0], f_km[1])
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

    # Métricas
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Anuncios", f"{len(filtered):,}")
    p_f = _num(filtered.get("precio_€", pd.Series()), strip=True).dropna()
    if not p_f.empty: mc2.metric("Precio medio", f"{p_f.mean():,.0f} €")
    y_f = _num(filtered.get("año", pd.Series())).dropna()
    if not y_f.empty: mc3.metric("Año medio", f"{y_f.mean():.0f}")
    k_f = _num(filtered.get("kilometros", pd.Series()), strip=True).dropna()
    if not k_f.empty: mc4.metric("KM medios", f"{k_f.mean():,.0f}")

    if len(filtered) < len(df):
        st.caption(f"Mostrando {len(filtered):,} de {len(df):,} anuncios")

    # Selector columnas
    all_cols = list(filtered.columns)
    default_cols = [c for c in ["titulo", "precio_€", "año", "kilometros", "combustible",
                                 "carroceria", "ciudad", "provincia", "vendedor_profesional",
                                 "telefono", "url"] if c in all_cols]
    shown_cols = st.multiselect("Columnas visibles", all_cols, default=default_cols, key=f"cols{uid}")
    if not shown_cols: shown_cols = default_cols

    st.dataframe(
        filtered[shown_cols], use_container_width=True, height=480,
        column_config={
            "url":                  st.column_config.LinkColumn("URL", display_text="Ver"),
            "precio_€":             st.column_config.NumberColumn("Precio €", format="%d €"),
            "vendedor_profesional": st.column_config.CheckboxColumn("Profesional"),
        },
    )

    ba, bb = st.columns(2)
    buf = io.StringIO()
    filtered.to_csv(buf, index=False, encoding="utf-8-sig")
    ba.download_button("⬇️ Descargar CSV", data=buf.getvalue(),
                       file_name=f"cochesnet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                       mime="text/csv", use_container_width=True, key=f"dl{uid}")

    if bb.button(f"➕ Añadir {len(filtered):,} al CRM", use_container_width=True, key=f"addcrm{uid}"):
        for _, row in filtered.iterrows():
            add_crm_from_car(row.to_dict())
        st.success(f"✅ {len(filtered):,} anuncios añadidos al CRM.")
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════
tab_scraper, tab_historial, tab_crm, tab_notas = st.tabs(
    ["🔍 Scraper", "📋 Historial", "👥 CRM", "📝 Notas"]
)

# ─────────────────────────────────────────────
# TAB 1 — SCRAPER
# ─────────────────────────────────────────────
with tab_scraper:
    if scrape_btn:
        filters = build_filters()
        ftxt    = filters_summary(filters)
        prog    = st.progress(0.0)
        status  = st.empty()

        def on_progress(page: int, total: int, found: int):
            prog.progress(page / total)
            status.info(f"Raspando página {page}/{total} — {found} anuncios…")

        status.info("⏳ Iniciando…")
        df = scraper.scrape(filters, max_pages=max_pages, progress_callback=on_progress)
        prog.progress(1.0)

        if df.empty:
            status.warning("No se encontraron anuncios.")
        else:
            status.success(f"✅ {len(df):,} anuncios encontrados.")
            sid = save_scrape(ftxt, df)
            st.session_state["last_scrape_id"] = sid
            st.session_state[f"scrape_{sid}"]  = df
            st.info(f"💾 Guardado en historial #{sid} — *{ftxt}*")
            show_results(df, sid)

    elif "last_scrape_id" in st.session_state:
        sid      = st.session_state["last_scrape_id"]
        df_cache = st.session_state.get(f"scrape_{sid}")
        if df_cache is not None and not df_cache.empty:
            st.info("📌 Último raspado. Pulsa **🚀 Iniciar Raspado** para buscar de nuevo.")
            show_results(df_cache, sid)
    else:
        st.markdown(
            "<div style='text-align:center;padding:4rem 1rem'>"
            "<div style='font-size:4rem'>🔍</div>"
            "<h3>Listo para buscar</h3>"
            "<p>Configura los filtros y pulsa <b>🚀 Iniciar Raspado</b>.</p>"
            "</div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# TAB 2 — HISTORIAL
# ─────────────────────────────────────────────
with tab_historial:
    st.markdown("### 📋 Historial de raspados")
    hc1, hc2 = st.columns([3, 1])
    hist_ttl = hc1.slider("Auto-borrar después de (días)", 1, 60, 7, key="hist_ttl")
    if hc2.button("🗑️ Limpiar ahora"):
        cleanup_old_scrapes(days=hist_ttl)
        st.success(f"Eliminados scrapes con más de {hist_ttl} días.")
        st.rerun()

    scrapes = list_scrapes()
    if not scrapes:
        st.info("El historial está vacío. Los raspados se guardan automáticamente.")
    else:
        st.caption(f"{len(scrapes)} raspado(s) guardado(s)")
        for s in scrapes:
            c1, c2, c3, c4 = st.columns([1.8, 3.5, 1, 1])
            c1.caption(f"🕐 {s['timestamp']}")
            c2.markdown(f"**{s['filters_txt'] or '—'}** · {s['num_results']:,} anuncios")
            if c3.button("📂 Cargar", key=f"load_{s['id']}", use_container_width=True):
                with st.spinner("Cargando…"):
                    df_h = load_scrape(s["id"])
                if df_h is not None:
                    st.session_state["last_scrape_id"]    = s["id"]
                    st.session_state[f"scrape_{s['id']}"] = df_h
                    st.success("Cargado. Ve a **🔍 Scraper**.")
                else:
                    st.error("No se pudo cargar.")
            if c4.button("🗑️", key=f"del_{s['id']}", use_container_width=True):
                delete_scrape(s["id"])
                st.session_state.pop(f"scrape_{s['id']}", None)
                if st.session_state.get("last_scrape_id") == s["id"]:
                    st.session_state.pop("last_scrape_id", None)
                st.rerun()
            st.divider()

# ─────────────────────────────────────────────
# TAB 3 — CRM
# ─────────────────────────────────────────────
with tab_crm:
    st.markdown("### 👥 CRM — Registro de contactos")

    crm_df = get_crm()
    n_rows = len(crm_df)

    # ── Filters ───────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns(3)
    f_nombre = fc1.text_input("🔎 Buscar nombre / vehículo", key="crm_f_nombre",
                               placeholder="ej: García, Golf…")
    f_estado = fc2.multiselect("Estado", CRM_ESTADOS, key="crm_f_estado",
                                placeholder="Todos los estados")
    f_tipo   = fc3.selectbox("Tipo vendedor", ["Todos"] + CRM_TIPOS_VENDEDOR, key="crm_f_tipo")

    # ── Toolbar ───────────────────────────────────────────────────────────
    ta, tb, tc, td, te = st.columns([1.1, 1.1, 1.1, 1.1, 1])

    add_btn    = ta.button("➕ Nuevo",           use_container_width=True)
    save_btn   = tb.button("💾 Guardar",          type="primary", use_container_width=True)
    del_btn    = tc.button("🗑️ Borrar marcadas", use_container_width=True)
    selall_btn = td.button("☑️ Seleccionar todo", use_container_width=True)

    # Selector de rango
    with te.expander("Rango"):
        r1, r2 = st.columns(2)
        rng_from = r1.number_input("De fila", 1, max(n_rows, 1), 1, key="rng_from")
        rng_to   = r2.number_input("A fila",  1, max(n_rows, 1), max(n_rows, 1), key="rng_to")
        if st.button("Marcar rango", use_container_width=True, key="sel_range_btn"):
            st.session_state.pop("crm_editor", None)
            st.session_state["crm_range"] = (int(rng_from) - 1, int(rng_to))
            st.rerun()

    # ── Añadir fila nueva ─────────────────────────────────────────────────
    if add_btn:
        new_row = pd.DataFrame([{
            "id": None, "nombre": "", "telefono": "", "email": "",
            "url_anuncio": "", "titulo_vehiculo": "", "precio": "",
            "tipo_vendedor": "Desconocido", "estado": "Pendiente",
            "fecha_contacto": "", "fecha_seguimiento": "",
            "notas": "", "created_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }])
        crm_df = pd.concat([crm_df, new_row], ignore_index=True)
        st.session_state["crm_draft"] = crm_df

    edit_source = st.session_state.get("crm_draft", crm_df)

    # ── Aplicar filtros — separa visibles de ocultos ───────────────────────
    full_df = edit_source.copy()
    fmask   = pd.Series(True, index=full_df.index)
    if f_nombre.strip():
        fmask &= (
            full_df["nombre"].astype(str).str.contains(f_nombre.strip(), case=False, na=False)
            | full_df["titulo_vehiculo"].astype(str).str.contains(f_nombre.strip(), case=False, na=False)
        )
    if f_estado:
        fmask &= full_df["estado"].isin(f_estado)
    if f_tipo != "Todos" and "tipo_vendedor" in full_df.columns:
        fmask &= full_df["tipo_vendedor"] == f_tipo

    hidden_df  = full_df[~fmask].drop(columns=["_borrar"], errors="ignore").copy()
    visible_df = full_df[fmask].copy()

    edit_with_del = visible_df.drop(columns=["_borrar"], errors="ignore").copy()
    edit_with_del.insert(0, "_borrar", False)

    # ── Seleccionar todo: limpiar caché del editor y relanzar ─────────────
    if selall_btn:
        st.session_state.pop("crm_editor", None)
        st.session_state["crm_sel_all"] = True
        st.rerun()

    # Marcar rango (tras rerun, el editor ya no tiene caché)
    rng = st.session_state.pop("crm_range", None)
    if rng:
        lo, hi = rng
        edit_with_del["_borrar"] = False
        edit_with_del.loc[edit_with_del.index[lo:hi], "_borrar"] = True

    # Aplicar seleccionar-todo (tras rerun con editor limpio)
    if st.session_state.pop("crm_sel_all", False):
        edit_with_del["_borrar"] = True

    # ── Caption ───────────────────────────────────────────────────────────
    is_filtered = int(fmask.sum()) < len(full_df)
    if is_filtered:
        st.caption(
            f"Mostrando {int(fmask.sum()):,} de {len(full_df):,} contactos (filtro activo). "
            "Guardar/Borrar solo afecta a los contactos visibles."
        )
    else:
        st.caption(
            "💡 Marca ☑ la columna **Borrar** en las filas a eliminar. "
            "Usa **Seleccionar todo** o **Rango** para marcar varias de golpe."
        )

    # ── Tabla editable ────────────────────────────────────────────────────
    edited = st.data_editor(
        edit_with_del,
        use_container_width=True,
        height=540,
        num_rows="dynamic",
        column_config={
            "_borrar":           st.column_config.CheckboxColumn("🗑️ Borrar", default=False, width="small"),
            "id":                st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "nombre":            st.column_config.TextColumn("Nombre", width="medium"),
            "telefono":          st.column_config.TextColumn("Teléfono", width="medium"),
            "email":             st.column_config.TextColumn("Email", width="medium"),
            "url_anuncio":       st.column_config.LinkColumn("Anuncio", display_text="Ver 🔗", width="small"),
            "titulo_vehiculo":   st.column_config.TextColumn("Vehículo", width="large"),
            "precio":            st.column_config.TextColumn("Precio", width="small"),
            "tipo_vendedor":     st.column_config.SelectboxColumn("Tipo vendedor", options=CRM_TIPOS_VENDEDOR, width="medium"),
            "estado":            st.column_config.SelectboxColumn("Estado", options=CRM_ESTADOS, width="medium"),
            "fecha_contacto":    st.column_config.TextColumn("Contactado", width="medium"),
            "fecha_seguimiento": st.column_config.TextColumn("Seguimiento", width="medium"),
            "notas":             st.column_config.TextColumn("Notas", width="large"),
            "created_at":        st.column_config.TextColumn("Creado", disabled=True, width="medium"),
        },
        column_order=["_borrar", "id", "nombre", "tipo_vendedor", "estado", "telefono", "email",
                      "titulo_vehiculo", "precio", "url_anuncio",
                      "fecha_contacto", "fecha_seguimiento", "notas", "created_at"],
        hide_index=True,
        key="crm_editor",
    )

    if save_btn:
        edited_clean  = edited.drop(columns=["_borrar"], errors="ignore")
        full_to_save  = pd.concat([hidden_df, edited_clean], ignore_index=True)
        save_crm(full_to_save)
        st.session_state.pop("crm_draft", None)
        st.success("✅ CRM guardado.")
        st.rerun()

    if del_btn:
        to_keep_vis = edited[~edited["_borrar"]].drop(columns=["_borrar"], errors="ignore")
        n_del       = len(edited) - len(to_keep_vis)
        if n_del == 0:
            st.warning("No hay filas marcadas para borrar.")
        else:
            full_to_save = pd.concat([hidden_df, to_keep_vis], ignore_index=True)
            save_crm(full_to_save)
            st.session_state.pop("crm_draft", None)
            st.success(f"✅ {n_del} fila(s) eliminada(s).")
            st.rerun()

    st.divider()
    exp_buf = io.StringIO()
    crm_df.to_csv(exp_buf, index=False, encoding="utf-8-sig")
    st.download_button("⬇️ Exportar CRM CSV", data=exp_buf.getvalue(),
                       file_name=f"crm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                       mime="text/csv")

# ─────────────────────────────────────────────
# TAB 4 — NOTAS
# ─────────────────────────────────────────────
with tab_notas:
    st.markdown("### 📝 Notas")
    st.caption("Escribe notas libres con soporte Markdown. Se guardan automáticamente en la base de datos local.")

    notes = list_notes()

    left, right = st.columns([1, 3], gap="medium")

    with left:
        st.markdown("**Mis notas**")
        if st.button("➕ Nueva nota", use_container_width=True, key="new_note_btn"):
            new_id = save_note(None, "Nueva nota", "")
            st.session_state["active_note_id"] = new_id
            st.rerun()

        st.markdown("---")
        notes = list_notes()
        for n in notes:
            active = st.session_state.get("active_note_id") == n["id"]
            label  = f"{'📄' if active else '📃'} {n['title']}"
            if st.button(label, key=f"note_btn_{n['id']}", use_container_width=True,
                         type="primary" if active else "secondary"):
                st.session_state["active_note_id"] = n["id"]
                st.rerun()

    with right:
        active_id = st.session_state.get("active_note_id")
        if not active_id:
            st.markdown(
                "<div style='text-align:center;padding:4rem 1rem;color:#94A3B8'>"
                "<div style='font-size:3rem'>📝</div>"
                "<p>Selecciona una nota o crea una nueva</p>"
                "</div>", unsafe_allow_html=True)
        else:
            note = get_note(active_id)
            if note is None:
                st.warning("Nota no encontrada.")
                st.session_state.pop("active_note_id", None)
            else:
                # Título
                new_title = st.text_input("Título", value=note["title"], key=f"note_title_{active_id}")

                # Modo edición / vista
                edit_mode = st.toggle("✏️ Modo edición", value=True, key=f"note_mode_{active_id}")

                if edit_mode:
                    new_content = st.text_area(
                        "Contenido (Markdown)",
                        value=note["content"],
                        height=480,
                        placeholder="Escribe aquí… admite **negrita**, *cursiva*, listas, etc.",
                        key=f"note_content_{active_id}",
                        label_visibility="collapsed",
                    )
                else:
                    new_content = note["content"]
                    st.markdown(
                        f"<div style='background:white;border:1px solid #E2E8F0;"
                        f"border-radius:10px;padding:1.5rem;min-height:480px'>"
                        f"{new_content or '<em style=\"color:#94A3B8\">Nota vacía</em>'}"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                    # Render proper markdown preview
                    if new_content:
                        st.markdown(new_content)

                nt1, nt2 = st.columns([1, 1])
                if nt1.button("💾 Guardar nota", type="primary", use_container_width=True, key=f"save_note_{active_id}"):
                    save_note(active_id, new_title, new_content if edit_mode else note["content"])
                    st.success("Nota guardada.")
                    st.rerun()

                if nt2.button("🗑️ Eliminar nota", use_container_width=True, key=f"del_note_{active_id}"):
                    delete_note(active_id)
                    st.session_state.pop("active_note_id", None)
                    st.success("Nota eliminada.")
                    st.rerun()

                st.caption(f"Creada: {note['created_at']} · Modificada: {note['updated_at']}")
