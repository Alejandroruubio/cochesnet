from __future__ import annotations

import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from typing import Callable, Optional
from urllib.parse import urlencode

import pandas as pd
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

BASE_URL    = "https://www.coches.net"
SEARCH_PATH = "/segunda-mano/"

FUEL_TYPES = {
    "Gasolina":  "2",
    "Diésel":    "3",
    "Híbrido":   "15",
    "Eléctrico": "16",
    "GLP":       "6",
    "GNC":       "5",
}

BODY_TYPE_LABELS = {
    1: "Berlina", 2: "Familiar",    3: "Monovolumen", 4: "SUV/4x4",
    5: "Descapotable", 6: "Coupé", 7: "Furgoneta",   8: "Pickup",
}
BODY_TYPES = {v: str(k) for k, v in BODY_TYPE_LABELS.items()}

TRANSMISSIONS = {
    "Todos":      None,
    "Manual":     "1",
    "Automático": "2",
}

SORT_OPTIONS = {
    "Relevancia":             "relevance",
    "Precio ascendente":      "price-asc",
    "Precio descendente":     "price-desc",
    "Año más reciente":       "year-desc",
    "Kilómetros ascendentes": "km-asc",
}


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^\w\s.-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


class CochesNetScraper:
    def __init__(self, delay: float = 1.5):
        self.delay = delay
        self._makes_cache: list = []

    # ── Chromium path ─────────────────────────────────────────────────────

    @staticmethod
    def _chromium_executable() -> Optional[str]:
        if sys.platform.startswith("linux"):
            for path in ("/usr/bin/chromium-browser", "/usr/bin/chromium"):
                if os.path.exists(path):
                    return path
        return None

    # ── Playwright fetch ──────────────────────────────────────────────────

    def _fetch_url(self, url: str) -> str:
        executable    = self._chromium_executable()
        launch_kwargs: dict = {"headless": True}
        if executable:
            launch_kwargs["executable_path"] = executable

        with sync_playwright() as pw:
            browser = pw.chromium.launch(**launch_kwargs)
            ctx = browser.new_context(
                locale="es-ES",
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            )
            page = ctx.new_page()
            page.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,otf}",
                lambda r: r.abort(),
            )
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            try:
                page.wait_for_function(
                    "() => typeof window.__INITIAL_PROPS__ !== 'undefined' "
                    "|| document.body.innerText.includes('INITIAL_PROPS')",
                    timeout=15_000,
                )
            except Exception:
                pass
            html = page.content()
            browser.close()
        return html

    # ── JSON extraction ───────────────────────────────────────────────────

    def _extract_initial_props(self, html: str) -> dict:
        match = re.search(
            r'__INITIAL_PROPS__\s*=\s*JSON\.parse\("(.+?)"\);\s*',
            html, re.DOTALL,
        )
        if match:
            try:
                decoded = json.loads('"' + match.group(1) + '"')
                return json.loads(decoded)
            except (json.JSONDecodeError, ValueError) as e:
                logger.error("JSON parse error: %s", e)
        return {}

    # ── Makes / Models ────────────────────────────────────────────────────

    def get_makes(self) -> list:
        if self._makes_cache:
            return self._makes_cache
        try:
            html  = self._fetch_url(BASE_URL + SEARCH_PATH)
            props = self._extract_initial_props(html)
            options = (
                props.get("listFiltersOptions", {})
                     .get("vehicles", {})
                     .get("options", [])
            )
            if options:
                makes = []
                for m in options:
                    if not isinstance(m, dict) or "label" not in m:
                        continue
                    makes.append({
                        "id":     str(m.get("id", "")),
                        "label":  m["label"],
                        "slug":   slugify(m["label"]),
                        "models": [
                            {"id": str(mod.get("id", "")), "label": mod["label"],
                             "slug": slugify(mod["label"])}
                            for mod in m.get("models", []) if "label" in mod
                        ],
                    })
                if makes:
                    self._makes_cache = makes
                    return makes
        except Exception as e:
            logger.error("Error fetching makes: %s", e)

        self._makes_cache = self._hardcoded_makes()
        return self._makes_cache

    # ── URL building ──────────────────────────────────────────────────────

    def _build_url(self, filters: dict, page: int) -> str:
        make_slug  = filters.get("make_slug",  "").strip()
        model_slug = filters.get("model_slug", "").strip()

        if make_slug and model_slug:
            path = f"/{make_slug}/{model_slug}/segunda-mano/"
        elif make_slug:
            path = f"/{make_slug}/segunda-mano/"
        else:
            path = SEARCH_PATH

        params: dict = {"pg": page}

        def add(key, val):
            if val is not None and val != 0 and val != "":
                params[key] = val

        add("price.from", filters.get("price_from"))
        add("price.to",   filters.get("price_to"))
        add("year.from",  filters.get("year_from"))
        add("year.to",    filters.get("year_to"))
        add("kms.from",   filters.get("km_from"))
        add("kms.to",     filters.get("km_to"))
        add("hp.from",    filters.get("hp_from"))
        add("hp.to",      filters.get("hp_to"))

        if filters.get("fuel_type_ids"):
            params["fuelTypeIds"] = ",".join(filters["fuel_type_ids"])
        if filters.get("body_type_ids"):
            params["bodyTypeIds"] = ",".join(filters["body_type_ids"])
        if filters.get("transmission_id"):
            params["transmissionTypeId"] = filters["transmission_id"]
        if filters.get("has_warranty"):
            params["hasWarranty"] = "true"

        seller = filters.get("seller_type", "")
        if seller and seller not in ("todos", ""):
            params["dealerType"] = seller

        if filters.get("sort"):
            params["sortBy"] = filters["sort"]

        return BASE_URL + path + "?" + urlencode(params)

    # ── Listing parser ────────────────────────────────────────────────────

    def _parse_listings(self, html: str) -> list:
        props = self._extract_initial_props(html)
        items = props.get("initialResults", {}).get("items", [])
        return [self._normalize_car(c) for c in items] if items else []

    def _normalize_car(self, car: dict) -> dict:
        url      = car.get("url", "")
        if url.startswith("/"):
            url = BASE_URL + url
        location = car.get("location") or {}
        seller   = car.get("seller")   or {}
        return {
            "id":                   car.get("id", ""),
            "titulo":               car.get("title", ""),
            "marca":                car.get("make", ""),
            "modelo":               car.get("model", ""),
            "año":                  car.get("year", ""),
            "precio_€":             car.get("price", ""),
            "precio_financiado_€":  car.get("financedPrice", ""),
            "kilometros":           car.get("km", ""),
            "potencia_cv":          car.get("hp", ""),
            "combustible":          car.get("fuelType", ""),
            "carroceria":           BODY_TYPE_LABELS.get(car.get("bodyTypeId"), ""),
            "etiqueta_dgt":         car.get("environmentalLabel", ""),
            "garantia":             car.get("hasWarranty", ""),
            "meses_garantia":       car.get("warrantyMonths", ""),
            "certificado":          car.get("isCertified", ""),
            "tipo_oferta":          (car.get("offerType") or {}).get("literal", ""),
            "ciudad":               location.get("cityLiteral", ""),
            "provincia":            location.get("mainProvince", ""),
            "vendedor":             seller.get("name", ""),
            "telefono":             car.get("phone", ""),
            "vendedor_profesional": bool(car.get("isProfessional", False)),
            "valoracion_vendedor":  (seller.get("ratings") or {}).get("average", ""),
            "num_fotos":            len(car.get("photos") or []),
            "fecha_publicacion":    car.get("publicationDate", ""),
            "url":                  url,
        }

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(
        self,
        filters: dict,
        max_pages: int = 10,
        progress_callback: Optional[Callable] = None,
    ) -> pd.DataFrame:
        all_cars: list = []

        for page in range(1, max_pages + 1):
            if progress_callback:
                progress_callback(page, max_pages, len(all_cars))
            try:
                url  = self._build_url(filters, page)
                html = self._fetch_url(url)
                cars = self._parse_listings(html)

                if not cars:
                    logger.info("No listings on page %d — stopping.", page)
                    break

                all_cars.extend(cars)
                logger.info("Page %d: %d cars (total %d)", page, len(cars), len(all_cars))

                if page < max_pages:
                    time.sleep(self.delay + random.uniform(0.5, 1.5))

            except Exception as e:
                logger.error("Error on page %d: %s", page, e)
                break

        if not all_cars:
            return pd.DataFrame()

        df = pd.DataFrame(all_cars)
        if "url" in df.columns:
            df = df.drop_duplicates(subset=["url"], keep="first")
        return df.fillna("").reset_index(drop=True)

    # ── Hardcoded fallback ────────────────────────────────────────────────

    @staticmethod
    def _hardcoded_makes() -> list:
        data = [
            ("Abarth", []),
            ("Alfa Romeo",   ["Giulia", "Giulietta", "Stelvio", "Tonale"]),
            ("Aston Martin", []),
            ("Audi",         ["A1","A2","A3","A4","A5","A6","A7","A8","Q2","Q3","Q5","Q7","Q8","TT","R8","e-tron"]),
            ("BMW",          ["Serie 1","Serie 2","Serie 3","Serie 4","Serie 5","Serie 7","X1","X2","X3","X4","X5","X6","X7","Z4","i3","i4","iX"]),
            ("Chevrolet",    ["Camaro","Corvette","Equinox","Spark","Trax"]),
            ("Citroën",      ["C1","C2","C3","C3 Aircross","C4","C5","C5 Aircross","Berlingo","Jumpy"]),
            ("Cupra",        ["Ateca","Born","Formentor","Leon"]),
            ("Dacia",        ["Duster","Jogger","Logan","Sandero","Spring"]),
            ("Ferrari",      []),
            ("Fiat",         ["500","500X","Doblo","Ducato","Panda","Punto","Tipo"]),
            ("Ford",         ["EcoSport","Fiesta","Focus","Kuga","Mondeo","Mustang","Puma","Ranger"]),
            ("Honda",        ["Accord","Civic","CR-V","HR-V","Jazz"]),
            ("Hyundai",      ["i10","i20","i30","Ioniq 5","Ioniq 6","Kona","Santa Fe","Tucson"]),
            ("Jaguar",       ["E-Pace","F-Pace","F-Type","I-Pace","XE","XF"]),
            ("Jeep",         ["Cherokee","Compass","Grand Cherokee","Renegade","Wrangler"]),
            ("Kia",          ["Ceed","EV6","Niro","Picanto","Sorento","Sportage","Stonic","XCeed"]),
            ("Lamborghini",  []),
            ("Land Rover",   ["Defender","Discovery","Freelander","Range Rover","Range Rover Evoque","Range Rover Sport"]),
            ("Lexus",        ["CT","IS","LC","LS","NX","RC","RX","UX"]),
            ("Mazda",        ["2","3","6","CX-3","CX-30","CX-5","MX-5"]),
            ("Mercedes-Benz",["Clase A","Clase B","Clase C","Clase CLA","Clase E","Clase G","Clase GLA","Clase GLC","Clase GLE","Clase S","EQA","EQB","EQC","EQS"]),
            ("MINI",         ["Cabrio","Clubman","Countryman","Cooper","One"]),
            ("Mitsubishi",   ["ASX","Eclipse Cross","L200","Outlander"]),
            ("Nissan",       ["Juke","Leaf","Micra","Qashqai","X-Trail"]),
            ("Opel",         ["Astra","Corsa","Crossland","Grandland","Insignia","Mokka"]),
            ("Peugeot",      ["208","308","508","2008","3008","5008"]),
            ("Porsche",      ["718 Boxster","718 Cayman","911","Cayenne","Macan","Panamera","Taycan"]),
            ("Renault",      ["Arkana","Captur","Clio","Kadjar","Megane","Scenic","Twingo","Zoe"]),
            ("SEAT",         ["Arona","Ateca","Ibiza","Leon","Tarraco"]),
            ("Škoda",        ["Fabia","Kamiq","Karoq","Kodiaq","Octavia","Superb"]),
            ("Suzuki",       ["Ignis","Jimny","S-Cross","Swift","Vitara"]),
            ("Tesla",        ["Model 3","Model S","Model X","Model Y"]),
            ("Toyota",       ["Aygo","C-HR","Corolla","Land Cruiser","Prius","RAV4","Yaris","bZ4X"]),
            ("Volkswagen",   ["Golf","ID.3","ID.4","ID.5","Passat","Polo","T-Cross","T-Roc","Tiguan","Touareg","Touran","up!"]),
            ("Volvo",        ["C40","S60","V60","V90","XC40","XC60","XC90"]),
        ]
        return [
            {
                "id":     "",
                "label":  name,
                "slug":   slugify(name),
                "models": [{"id": "", "label": m, "slug": slugify(m)} for m in models],
            }
            for name, models in data
        ]
