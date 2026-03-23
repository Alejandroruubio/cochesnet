import io
import logging
from datetime import datetime

import pandas as pd
import streamlit as st

from scraper import (
    BODY_TYPES,
    FUEL_TYPES,
    SORT_OPTIONS,
    TRANSMISSIONS,
    CochesNetScraper,
)

logging.basicConfig(level=logging.INFO)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Raspador Coches.net",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Raspador de Coches.net")
st.caption("Extrae anuncios de coches de segunda mano con los filtros que elijas y exporta a CSV.")

# ── Cached resources ───────────────────────────────────────────────────────────

@st.cache_resource
def get_scraper() -> CochesNetScraper:
    return CochesNetScraper(delay=1.5)


@st.cache_data(ttl=3600, show_spinner="Cargando marcas...")
def fetch_makes() -> list:
    return get_scraper().get_makes()


scraper = get_scraper()
makes = fetch_makes()
make_label_to_obj = {m["label"]: m for m in makes}

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtros")

    # ── Marca / Modelo ─────────────────────────────────────────────────────────
    with st.expander("Marca y Modelo", expanded=True):
        make_options = ["(Todas las marcas)"] + [m["label"] for m in makes]
        selected_make_label = st.selectbox("Marca", make_options, key="make")

        selected_make = make_label_to_obj.get(selected_make_label)

        model_options = ["(Todos los modelos)"]
        if selected_make:
            model_options += [m["label"] for m in selected_make.get("models", [])]

        selected_model_label = st.selectbox(
            "Modelo",
            model_options,
            key="model",
            disabled=(selected_make is None),
        )

    # ── Precio ─────────────────────────────────────────────────────────────────
    with st.expander("Precio (€)"):
        col1, col2 = st.columns(2)
        with col1:
            price_from = st.number_input(
                "Mínimo", min_value=0, max_value=2_000_000,
                value=0, step=500, help="0 = sin límite",
            )
        with col2:
            price_to = st.number_input(
                "Máximo", min_value=0, max_value=2_000_000,
                value=0, step=500, help="0 = sin límite",
            )

    # ── Año ────────────────────────────────────────────────────────────────────
    with st.expander("Año"):
        year_options = ["(Cualquiera)"] + [str(y) for y in range(2025, 1989, -1)]
        col1, col2 = st.columns(2)
        with col1:
            year_from_sel = st.selectbox("Desde", year_options, key="year_from")
        with col2:
            year_to_sel = st.selectbox("Hasta", year_options, key="year_to")

    # ── Kilómetros ─────────────────────────────────────────────────────────────
    with st.expander("Kilómetros"):
        col1, col2 = st.columns(2)
        with col1:
            km_from = st.number_input(
                "Mínimo km", min_value=0, max_value=1_000_000,
                value=0, step=5_000, help="0 = sin límite",
            )
        with col2:
            km_to = st.number_input(
                "Máximo km", min_value=0, max_value=1_000_000,
                value=0, step=5_000, help="0 = sin límite",
            )

    # ── Potencia ───────────────────────────────────────────────────────────────
    with st.expander("Potencia (CV)"):
        col1, col2 = st.columns(2)
        with col1:
            hp_from = st.number_input(
                "Mínimo CV", min_value=0, max_value=2_000,
                value=0, step=10, help="0 = sin límite",
            )
        with col2:
            hp_to = st.number_input(
                "Máximo CV", min_value=0, max_value=2_000,
                value=0, step=10, help="0 = sin límite",
            )

    # ── Combustible / Carrocería ───────────────────────────────────────────────
    with st.expander("Combustible y Carrocería"):
        selected_fuels = st.multiselect(
            "Combustible", options=list(FUEL_TYPES.keys()), default=[]
        )
        selected_bodies = st.multiselect(
            "Carrocería", options=list(BODY_TYPES.keys()), default=[]
        )

    # ── Transmisión / Vendedor / Garantía ──────────────────────────────────────
    with st.expander("Más opciones"):
        transmission = st.selectbox("Transmisión", list(TRANSMISSIONS.keys()))
        seller_type = st.selectbox(
            "Tipo de vendedor", ["Todos", "Particular", "Profesional"]
        )
        has_warranty = st.checkbox("Solo con garantía")

    # ── Opciones de raspado ────────────────────────────────────────────────────
    st.divider()
    st.subheader("Opciones de raspado")
    sort_by = st.selectbox("Ordenar por", list(SORT_OPTIONS.keys()))
    max_pages = st.slider(
        "Páginas a raspar", min_value=1, max_value=200, value=5,
        help="Cada página contiene ~30 anuncios",
    )
    st.caption(f"Máximo ~{max_pages * 30} anuncios")

    scrape_btn = st.button("Iniciar Raspado", type="primary", use_container_width=True)


# ── Build filters dict ─────────────────────────────────────────────────────────
def build_filters() -> dict:
    f: dict = {}

    if selected_make:
        f["make_slug"] = selected_make["slug"]
        if selected_model_label != "(Todos los modelos)":
            model_obj = next(
                (m for m in selected_make.get("models", [])
                 if m["label"] == selected_model_label),
                None,
            )
            if model_obj:
                f["model_slug"] = model_obj["slug"]

    if price_from > 0:
        f["price_from"] = price_from
    if price_to > 0:
        f["price_to"] = price_to

    if year_from_sel != "(Cualquiera)":
        f["year_from"] = int(year_from_sel)
    if year_to_sel != "(Cualquiera)":
        f["year_to"] = int(year_to_sel)

    if km_from > 0:
        f["km_from"] = km_from
    if km_to > 0:
        f["km_to"] = km_to

    if hp_from > 0:
        f["hp_from"] = hp_from
    if hp_to > 0:
        f["hp_to"] = hp_to

    if selected_fuels:
        f["fuel_type_ids"] = [FUEL_TYPES[x] for x in selected_fuels]
    if selected_bodies:
        f["body_type_ids"] = [BODY_TYPES[x] for x in selected_bodies]

    trans_id = TRANSMISSIONS.get(transmission)
    if trans_id:
        f["transmission_id"] = trans_id

    if seller_type != "Todos":
        f["seller_type"] = seller_type.lower()
    if has_warranty:
        f["has_warranty"] = True

    f["sort"] = SORT_OPTIONS[sort_by]
    return f


# ── Main area ──────────────────────────────────────────────────────────────────
if scrape_btn:
    filters = build_filters()

    progress_bar = st.progress(0.0)
    status = st.empty()

    def on_progress(page: int, total: int, found: int):
        progress_bar.progress(page / total)
        status.info(f"Raspando página {page} de {total} — {found} anuncios encontrados...")

    status.info("Iniciando raspado...")

    df = scraper.scrape(filters, max_pages=max_pages, progress_callback=on_progress)

    progress_bar.progress(1.0)

    if df.empty:
        status.warning("No se encontraron anuncios con los filtros seleccionados.")
        st.stop()

    status.success(f"Completado: **{len(df)} anuncios** encontrados.")

    # ── Metrics ────────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total anuncios", f"{len(df):,}")

    with col2:
        if "precio_€" in df.columns:
            prices = pd.to_numeric(
                df["precio_€"].astype(str).str.replace(r"[^\d]", "", regex=True),
                errors="coerce",
            ).dropna()
            if not prices.empty:
                st.metric("Precio medio", f"{prices.mean():,.0f} €")

    with col3:
        if "año" in df.columns:
            years = pd.to_numeric(df["año"], errors="coerce").dropna()
            if not years.empty:
                st.metric("Año medio", f"{years.mean():.0f}")

    with col4:
        if "kilometros" in df.columns:
            kms = pd.to_numeric(
                df["kilometros"].astype(str).str.replace(r"[^\d]", "", regex=True),
                errors="coerce",
            ).dropna()
            if not kms.empty:
                st.metric("KM medios", f"{kms.mean():,.0f} km")

    # ── Table ──────────────────────────────────────────────────────────────────
    st.dataframe(df, use_container_width=True, height=500)

    # ── Download ───────────────────────────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"cochesnet_{timestamp}.csv"

    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")

    st.download_button(
        label="Descargar CSV",
        data=buf.getvalue(),
        file_name=fname,
        mime="text/csv",
        type="primary",
    )

else:
    st.info("Configura los filtros en la barra lateral y pulsa **Iniciar Raspado**.")

    with st.expander("Como usar esta herramienta"):
        st.markdown(
            """
            1. **Marca y Modelo** — déjalos en blanco para buscar en todo el catálogo.
            2. **Precio, Año, KM, CV** — pon `0` para no aplicar ese límite.
            3. **Combustible y Carrocería** — selección múltiple; vacío = todos.
            4. **Páginas a raspar** — cada página tiene ~30 anuncios.
            5. Pulsa **Iniciar Raspado** y espera a que termine.
            6. **Descarga el CSV** con todos los resultados.

            > El raspador respeta pausas de 1,5 s entre páginas para no saturar el servidor.
            """
        )
