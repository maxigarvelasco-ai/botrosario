import os
import json
import time
import re
import logging
import base64
import random
import unicodedata
from typing import Any, Dict, List, Tuple, Optional, Union
from datetime import date as _date
import requests
from google.cloud import storage

try:
    import pytesseract
except Exception:
    pytesseract = None

try:
    from google.cloud import firestore
except Exception:
    firestore = None

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

psycopg2 = None
Json = None
try:
    import importlib
    psycopg2 = importlib.import_module("psycopg2")
    _psycopg2_extras = importlib.import_module("psycopg2.extras")
    Json = getattr(_psycopg2_extras, "Json", None)
except Exception:
    pass

logging.basicConfig(level=logging.INFO)

# =========================
# ENV / CONFIG
# =========================
OUT_BUCKET = os.environ.get("OUT_BUCKET", "flyers-out").strip()
PARSED_PREFIX = os.environ.get("PARSED_PREFIX", "parsed/incoming").rstrip("/")
EVENTS_PREFIX = os.environ.get("EVENTS_PREFIX", "events/incoming").rstrip("/")
CONSOLIDADO_NAME = os.environ.get("CONSOLIDADO_NAME", "eventos.json").strip()
SUPABASE_DATABASE_URL = os.environ.get("SUPABASE_DATABASE_URL", os.environ.get("DATABASE_URL", "")).strip()
SUPABASE_EVENTS_TABLE = os.environ.get("SUPABASE_EVENTS_TABLE", "events").strip()
SUPABASE_UPSERT_STRICT = os.environ.get("SUPABASE_UPSERT_STRICT", "1").strip().lower() not in ("0", "false", "no")
DATA_BACKEND = os.environ.get("DATA_BACKEND", "firestore").strip().lower()
FIRESTORE_EVENTS_COLLECTION = os.environ.get("FIRESTORE_EVENTS_COLLECTION", "events").strip()
FIRESTORE_CATEGORIZED_COLLECTION = os.environ.get("FIRESTORE_CATEGORIZED_COLLECTION", "events_by_category").strip()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", os.getenv("groq_api_key", "")).strip()
GROQ_MODEL_EXTRACT = os.getenv("GROQ_MODEL_EXTRACT", "meta-llama/llama-4-scout-17b-16e-instruct").strip()
GROQ_MODEL_FALLBACK_TEXT = os.getenv("GROQ_MODEL_FALLBACK_TEXT", GROQ_MODEL_EXTRACT).strip()
GROQ_MODEL_FALLBACK_TEXT_HARD = os.getenv("GROQ_MODEL_FALLBACK_TEXT_HARD", GROQ_MODEL_EXTRACT).strip()
GROQ_FALLBACK_STRICT_JSON = os.getenv("GROQ_FALLBACK_STRICT_JSON", "1").strip().lower() not in ("0", "false", "no")
GROQ_HARD_FALLBACK_ENABLED = os.getenv("GROQ_HARD_FALLBACK_ENABLED", "1").strip().lower() not in ("0", "false", "no")
HARD_FALLBACK_MIN_ROWS = int(os.getenv("HARD_FALLBACK_MIN_ROWS", "18"))
HARD_FALLBACK_MIN_DATE_HITS = int(os.getenv("HARD_FALLBACK_MIN_DATE_HITS", "4"))
HARD_FALLBACK_MIN_BLOCKS = int(os.getenv("HARD_FALLBACK_MIN_BLOCKS", "4"))
HARD_FALLBACK_MIN_QUALITY = float(os.getenv("HARD_FALLBACK_MIN_QUALITY", "2.4"))
HARD_FALLBACK_MAX_EVENTS_OK = int(os.getenv("HARD_FALLBACK_MAX_EVENTS_OK", "2"))
HARD_FALLBACK_MAX_BLOCKS = int(os.getenv("HARD_FALLBACK_MAX_BLOCKS", "3"))
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "").strip()
TESSERACT_LANG = os.getenv("TESSERACT_LANG", "spa+eng").strip() or "spa+eng"
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".heic", ".webp")

storage_client = storage.Client()
_supabase_schema_ready = False
firestore_client = None
if firestore is not None:
    try:
        firestore_client = firestore.Client()
    except Exception as exc:
        logging.warning("firestore client no disponible: %s", exc)

if pytesseract is not None and TESSERACT_CMD:
    try:
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
    except Exception as exc:
        logging.warning("No pude setear TESSERACT_CMD: %s", exc)

# Regex básicos
DAY_RE = re.compile(r"\b(LUNES|MARTES|MI[EÉ]RCOLES|JUEVES|VIERNES|S[ÁA]BADO|DOMINGO)\b", re.IGNORECASE)
DM_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")

KNOWN_CITIES_UPPER = {
    "ROSARIO", "FUNES", "VILLA GOBERNADOR GÁLVEZ", "VILLA GOBERNADOR GALVEZ",
    "SAN LORENZO", "PÉREZ", "PEREZ", "GRANADERO BAIGORRIA",
    "CAPITÁN BERMÚDEZ", "CAPITAN BERMUDEZ", "SAN NICOLÁS", "SAN NICOLAS",
    "ROLDÁN", "ROLDAN", "ARROYO SECO", "VILLA CONSTITUCIÓN", "VILLA CONSTITUCION",
    "CASILDA", "CAÑADA DE GÓMEZ", "CANADA DE GOMEZ", "PUEBLO ESTHER",
    "FIGHIERA", "ALVEAR", "SOLDINI", "IBARLUCEA", "ZAVALLA", "VICTORIA",
    "PUERTO SAN MARTÍN", "PUERTO SAN MARTIN", "TIMBÚES", "TIMBUES",
}
CITY_MAP = {"VGG": "Villa Gobernador Gálvez"}

_VENUE_CONTEXT_RE = re.compile(
    r"\b(?:museo|teatro|bar|sala|centro cultural|anfiteatro|auditorio|biblioteca|club|cine|"
    r"espacio|galp[oó]n|planetario|plataforma|lavard[ée]n|broadway|fontanarrosa|castagnino|"
    r"metropolitano|mercado)\b",
    re.IGNORECASE,
)
_AGGREGATOR_RE_META = re.compile(r"disfruta|rosario turismo|agenda|guia|portal|what.?s.?up", re.IGNORECASE)

_WEEKDAY_MAP = {
    "lunes": 0, "martes": 1, "miercoles": 2, "miércoles": 2,
    "jueves": 3, "viernes": 4, "sabado": 5, "sábado": 5, "domingo": 6,
}
_WEEKDAY_NAMES_ES = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
_WEEKDAY_FECHA_RE = re.compile(
    r'(lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)\s+(\d{1,2})\s+de\s+(\w+)',
    re.IGNORECASE,
)
_MONTH_TO_NUM = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}
_TIME_ONLY_FECHA_RE = re.compile(r'^(?:\d{1,2}[:.]\d{2}\s*(?:hs?)?|\d{1,2}\s*hs?)$', re.IGNORECASE)
_NOISE_ANNOUNCEMENT_RE = re.compile(
    r'^(?:nuevos?\s+horarios?|horarios?\s+de\s+(?:ingreso|apertura|atenci[oó]n))\b',
    re.IGNORECASE,
)
_BARE_SCHEDULE_RE = re.compile(
    r"^(?:(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bados?|domingos?|feriados?)"
    r"(?:\s*[,ya]\s*(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bados?|domingos?|feriados?))*"
    r"[\s,]*(?:(?:a\s+las?\s+)?(?:\d{1,2}[:.h]?\d{0,2}\s*(?:hs?)?\.?)[\s,y]*)*)+$",
    re.IGNORECASE,
)
_BARE_VENUE_RE = re.compile(
    r"^(?:(?:el|la|los|las)\s+)?(?:anfiteatro|teatro|museo|bar|sala|club|centro cultural|cine|espacio|auditorio|biblioteca|planetario)"
    r"\s+[A-ZÁÉÍÓÚÑ][\w\s]*$",
    re.IGNORECASE,
)
_PEOPLE_LIST_OCR_RE = re.compile(
    r"(?:desaparecid[oa]s?|ced\.?\s*\d|\bd\.?n\.?i\.?\b|legajo|expediente|fecha de|nacid[oa]|secuestrad[oa])",
    re.IGNORECASE,
)
_PERSON_NAME_RE = re.compile(r"\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)+\b")
_MULTI_DAY_RE = re.compile(
    r'((?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)\s+\d{1,2}'
    r'(?:\s*[,]\s*(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)\s+\d{1,2})*'
    r'\s+y\s+(?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)\s+\d{1,2}'
    r'(?:\s+de\s+\w+)?)',
    re.IGNORECASE,
)
_MULTI_NUM_RE = re.compile(
    r'(\d{1,2}(?:\s*[,]\s*\d{1,2})*\s+y\s+\d{1,2}\s+de\s+\w+)',
    re.IGNORECASE,
)


# =========================
# HELPERS
# =========================
def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _ms(start: float, end: float) -> float:
    return round((end - start) * 1000.0, 2)


def _log_timing_summary(object_name: str, timings: Dict[str, float]) -> None:
    total = timings.get("10_total_por_imagen", 0.0) or 0.0
    ordered = [
        ("1. descarga/lectura de imagen", "1_descarga_lectura_imagen"),
        ("2. primera pasada vision", "2_primera_pasada_vision"),
        ("3. decisión de fallback", "3_decision_fallback"),
        ("4. OCR fallback", "4_ocr_fallback"),
        ("5. extracción LLM post OCR", "5_extraccion_llm_post_ocr"),
        ("6. normalización del evento", "6_normalizacion_evento"),
        ("7. escritura sidecar GCS", "7_escritura_sidecar_gcs"),
        ("8. update consolidado", "8_update_consolidado"),
        ("9. upsert Supabase", "9_upsert_supabase"),
        ("10. total por imagen", "10_total_por_imagen"),
    ]
    lines = [
        f"[TIMING] Summary for {object_name}",
        "[TIMING] +--------------------------------------+------------+---------+",
        "[TIMING] | Etapa                                | ms         | % total |",
        "[TIMING] +--------------------------------------+------------+---------+",
    ]
    for label, key in ordered:
        value = float(timings.get(key, 0.0) or 0.0)
        pct = (value * 100.0 / total) if total > 0 else 0.0
        lines.append(f"[TIMING] | {label:<36} | {value:>10.2f} | {pct:>6.2f}% |")
    lines.append("[TIMING] +--------------------------------------+------------+---------+")
    logging.info("\n".join(lines))


def _supabase_enabled() -> bool:
    return bool(SUPABASE_DATABASE_URL and psycopg2 is not None)


def _supabase_status() -> Dict[str, Any]:
    return {
        "url_configured": bool(SUPABASE_DATABASE_URL),
        "driver_available": psycopg2 is not None,
        "table": SUPABASE_EVENTS_TABLE,
        "enabled": _supabase_enabled(),
    }


def _firestore_enabled() -> bool:
    return bool(firestore_client is not None and FIRESTORE_EVENTS_COLLECTION)


def _firestore_status() -> Dict[str, Any]:
    return {
        "enabled": _firestore_enabled(),
        "client_available": firestore_client is not None,
        "collection": FIRESTORE_EVENTS_COLLECTION,
        "categorized_collection": FIRESTORE_CATEGORIZED_COLLECTION,
    }


def _category_doc_id(category: str) -> str:
    normalized = _strip_accents(_norm_text(category or ""))
    normalized = normalized.replace("+", " plus ")
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = normalized.strip("_")
    return normalized or "sin_categoria"


def _compact_event_for_category(event_hash: str, ev: Dict[str, Any], enriched: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_hash": event_hash,
        "artista_o_show": str(ev.get("artista_o_show") or "").strip(),
        "lugar": str(ev.get("lugar") or "").strip(),
        "fecha": str(ev.get("fecha") or "").strip(),
        "hora": str(ev.get("hora") or "").strip(),
        "ciudad": str(ev.get("ciudad") or "").strip(),
        "tipo": str(ev.get("tipo") or "evento").strip(),
        "categoria": str(ev.get("categoria") or "").strip(),
        "descripcion": str(ev.get("descripcion") or "").strip(),
        "is_free": bool(enriched.get("is_free")),
        "tags": list(enriched.get("tags") or []),
        "event_date": _parse_event_date_for_db(ev),
        "updated_at": time.time(),
    }


def _event_categories_for_index(ev: Dict[str, Any], enriched: Dict[str, Any]) -> List[str]:
    categories: List[str] = []
    direct = [
        ev.get("categoria"),
        ev.get("category_norm"),
        enriched.get("category_norm"),
    ]
    for item in direct:
        clean = _norm_text(str(item or ""))
        if clean:
            categories.append(clean)

    for tag in (enriched.get("tags") or []):
        clean = _norm_text(str(tag or ""))
        if clean:
            categories.append(clean)

    if not categories:
        categories.append("sin_categoria")
    return list(dict.fromkeys(categories))


def _sync_firestore_category_index(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not _firestore_enabled() or not FIRESTORE_CATEGORIZED_COLLECTION:
        return {
            "ok": False,
            "reason": "categorized_collection_unavailable",
            "categories_upserted": 0,
            "items_upserted": 0,
        }

    grouped: Dict[str, Dict[str, Any]] = {}
    for ev in events:
        if not isinstance(ev, dict):
            continue

        enriched = _enrich_event_for_sql(ev)
        event_hash = _event_hash(ev)
        compact = _compact_event_for_category(event_hash, ev, enriched)
        for cat in _event_categories_for_index(ev, enriched):
            cat_doc_id = _category_doc_id(cat)
            slot = grouped.setdefault(
                cat_doc_id,
                {
                    "category": cat,
                    "items": {},
                },
            )
            slot["items"][event_hash] = compact

    root = firestore_client.collection(FIRESTORE_CATEGORIZED_COLLECTION)
    grouped_ids = set(grouped.keys())
    categories_upserted = 0
    items_upserted = 0
    items_deleted = 0
    categories_deleted = 0

    existing_categories = [doc.id for doc in root.stream()]

    for cat_id, payload in grouped.items():
        items_map = payload.get("items") or {}
        cat_ref = root.document(cat_id)
        cat_ref.set(
            {
                "category": payload.get("category") or cat_id,
                "items_count": len(items_map),
                "pipeline_model": GROQ_MODEL_EXTRACT,
                "updated_at": time.time(),
            },
            merge=True,
        )
        categories_upserted += 1

        items_ref = cat_ref.collection("items")
        existing_item_ids = {doc.id for doc in items_ref.stream()}
        target_item_ids = set(items_map.keys())

        for event_hash, compact in items_map.items():
            items_ref.document(event_hash).set(compact, merge=True)
            items_upserted += 1

        for stale_id in sorted(existing_item_ids - target_item_ids):
            items_ref.document(stale_id).delete()
            items_deleted += 1

    stale_categories = [cat for cat in existing_categories if cat not in grouped_ids]
    for cat_id in stale_categories:
        cat_ref = root.document(cat_id)
        for item_doc in cat_ref.collection("items").stream():
            item_doc.reference.delete()
            items_deleted += 1
        cat_ref.delete()
        categories_deleted += 1

    return {
        "ok": True,
        "reason": "ok",
        "categories_upserted": categories_upserted,
        "categories_deleted": categories_deleted,
        "items_upserted": items_upserted,
        "items_deleted": items_deleted,
    }


def _parse_event_date_for_db(ev: Dict[str, Any]) -> Optional[str]:
    text = f"{ev.get('fecha', '')} {ev.get('descripcion', '')}".strip().lower()
    if not text:
        return None

    current_year = time.localtime().tm_year
    month_map = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }

    def _safe_date(y: int, mo: int, d: int) -> Optional[str]:
        try:
            dt = _date(y, mo, d)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    # 1) ISO exacta: 2026-03-25
    m = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b", text)
    if m:
        return _safe_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # 2) dd/mm o dd/mm/yyyy, pero evitando agarrar pedazos de 1982/1983
    m = re.search(r"(?<!\d)(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?(?!\d)", text)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3)) if m.group(3) else current_year
        if y < 100:
            y += 2000
        return _safe_date(y, mo, d)

    # 3) "18 de abril" / "18 de abril de 2026"
    m = re.search(r"\b(\d{1,2})\s+de\s+([a-záéíóúñ]+)(?:\s+de\s+(\d{4}))?\b", text)
    if m:
        d = int(m.group(1))
        mo = month_map.get(m.group(2))
        y = int(m.group(3)) if m.group(3) else current_year
        if mo:
            return _safe_date(y, mo, d)

    # 4) Todo lo ambiguo o tipo rango: no guardar DATE
    return None

def _event_hash(ev: Dict[str, Any]) -> str:
    sig = "|".join([
        (ev.get("artista_o_show") or "").strip().lower(),
        (ev.get("fecha") or "").strip().lower(),
        (ev.get("hora") or "").strip().lower(),
        (ev.get("lugar") or "").strip().lower(),
        (ev.get("tipo") or "").strip().lower(),
        (ev.get("categoria") or "").strip().lower(),
    ])
    import hashlib
    return hashlib.sha1(sig.encode("utf-8")).hexdigest()


def _merge_dedup_key(ev: Dict[str, Any]) -> str:
    """Clave de dedup local para merge MiniCPM+fallback.
    Incluye artista para evitar duplicados por descripciones levemente distintas."""
    sig = "|".join([
        (ev.get("artista_o_show") or "").strip().lower(),
        (ev.get("fecha") or "").strip().lower(),
        (ev.get("hora") or "").strip().lower(),
        (ev.get("lugar") or "").strip().lower(),
        (ev.get("categoria") or "").strip().lower(),
    ])
    import hashlib
    return hashlib.sha1(sig.encode("utf-8")).hexdigest()


def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text or "") if not unicodedata.combining(c))


def _norm_text(text: str) -> str:
    return _strip_accents((text or "").lower()).strip()


def _infer_tags_from_event(ev: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    fields = [
        (ev.get("categoria") or ""),
        (ev.get("tipo") or ""),
        (ev.get("descripcion") or ""),
        (ev.get("artista_o_show") or ""),
        (ev.get("lugar") or ""),
    ]
    haystack = _norm_text(" ".join(fields))
    match_map = {
        "teatro": ["teatro", "obra", "stand up", "comedia"],
        "cine": ["cine", "pelicula", "proyeccion"],
        "arte": ["arte", "muestra", "exposicion", "museo"],
        "feria": ["feria", "mercado", "artesan"],
        "familiar": ["familiar", "infantil", "chicos", "ninos", "niños"],
        "aire_libre": ["aire libre", "plaza", "parque", "costanera"],
        "musica": ["musica", "recital", "concierto", "banda", "show"],
        "cultural": ["cultural", "centro cultural", "biblioteca", "charla", "taller"],
    }
    for tag, keywords in match_map.items():
        for kw in keywords:
            kw_norm = _norm_text(kw)
            if " " in kw_norm:
                if kw_norm in haystack:
                    tags.append(tag)
                    break
            elif re.search(rf"\b{re.escape(kw_norm)}\b", haystack):
                tags.append(tag)
                break
    return list(dict.fromkeys(tags))


def _enrich_event_for_sql(ev: Dict[str, Any]) -> Dict[str, Any]:
    categoria = _norm_text(ev.get("categoria") or "") or None
    tipo = _norm_text(ev.get("tipo") or "") or None
    ciudad = _norm_text(ev.get("ciudad") or "") or None
    lugar = _norm_text(ev.get("lugar") or "") or None
    artista = _norm_text(ev.get("artista_o_show") or "") or None
    gratis_raw = str(ev.get("gratis") or "").strip().lower()
    is_free = bool(ev.get("gratis")) or gratis_raw in {"si", "sí", "true", "1", "gratis"}
    tags = _infer_tags_from_event(ev)
    if categoria and categoria not in tags:
        tags.insert(0, categoria)
    return {
        "category_norm": categoria,
        "tipo_norm": tipo,
        "ciudad_norm": ciudad,
        "lugar_norm": lugar,
        "artista_norm": artista,
        "is_free": is_free,
        "tags": tags,
    }


def _ensure_events_table(cur) -> None:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", SUPABASE_EVENTS_TABLE):
        raise ValueError(f"Invalid SUPABASE_EVENTS_TABLE: {SUPABASE_EVENTS_TABLE}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SUPABASE_EVENTS_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            event_hash TEXT UNIQUE NOT NULL,
            categoria TEXT,
            category_norm TEXT,
            fecha_text TEXT,
            hora TEXT,
            ciudad TEXT,
            ciudad_norm TEXT,
            lugar TEXT,
            lugar_norm TEXT,
            tipo_norm TEXT,
            artista_norm TEXT,
            is_free BOOLEAN NOT NULL DEFAULT FALSE,
            tags TEXT[] NOT NULL DEFAULT '{{}}',
            event_date DATE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            payload JSONB NOT NULL
        )
        """
    )
    cur.execute(f"ALTER TABLE {SUPABASE_EVENTS_TABLE} ADD COLUMN IF NOT EXISTS category_norm TEXT")
    cur.execute(f"ALTER TABLE {SUPABASE_EVENTS_TABLE} ADD COLUMN IF NOT EXISTS ciudad_norm TEXT")
    cur.execute(f"ALTER TABLE {SUPABASE_EVENTS_TABLE} ADD COLUMN IF NOT EXISTS lugar_norm TEXT")
    cur.execute(f"ALTER TABLE {SUPABASE_EVENTS_TABLE} ADD COLUMN IF NOT EXISTS tipo_norm TEXT")
    cur.execute(f"ALTER TABLE {SUPABASE_EVENTS_TABLE} ADD COLUMN IF NOT EXISTS artista_norm TEXT")
    cur.execute(f"ALTER TABLE {SUPABASE_EVENTS_TABLE} ADD COLUMN IF NOT EXISTS is_free BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute(f"ALTER TABLE {SUPABASE_EVENTS_TABLE} ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{{}}'")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{SUPABASE_EVENTS_TABLE}_categoria ON {SUPABASE_EVENTS_TABLE}(categoria)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{SUPABASE_EVENTS_TABLE}_category_norm ON {SUPABASE_EVENTS_TABLE}(category_norm)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{SUPABASE_EVENTS_TABLE}_event_date ON {SUPABASE_EVENTS_TABLE}(event_date)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{SUPABASE_EVENTS_TABLE}_is_free ON {SUPABASE_EVENTS_TABLE}(is_free)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{SUPABASE_EVENTS_TABLE}_tags_gin ON {SUPABASE_EVENTS_TABLE} USING GIN(tags)")


def _rebuild_stats_trending(cur) -> int:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stats_trending (
            category TEXT PRIMARY KEY,
            count BIGINT NOT NULL DEFAULT 0,
            last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("TRUNCATE TABLE stats_trending")
    cur.execute(
        f"""
        INSERT INTO stats_trending(category, count, last_updated_at)
        SELECT
            COALESCE(NULLIF(lower(trim(categoria)), ''), 'sin_categoria') AS category,
            COUNT(*)::BIGINT AS count,
            NOW()
        FROM {SUPABASE_EVENTS_TABLE}
        GROUP BY COALESCE(NULLIF(lower(trim(categoria)), ''), 'sin_categoria')
        """
    )
    return int(cur.rowcount or 0)


def _upsert_events_to_supabase(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    global _supabase_schema_ready
    status = _supabase_status()
    if not events:
        return {"ok": True, "upserted": 0, "reason": "no_events", **status}
    if not status["enabled"]:
        reason = "missing_database_url" if not status["url_configured"] else "missing_psycopg2"
        return {"ok": False, "upserted": 0, "reason": reason, **status}

    conn = None
    upserted = 0
    categorized = 0
    try:
        conn = psycopg2.connect(SUPABASE_DATABASE_URL, connect_timeout=6)
        with conn.cursor() as cur:
            if not _supabase_schema_ready:
                _ensure_events_table(cur)
                _supabase_schema_ready = True
            for ev in events:
                enriched = _enrich_event_for_sql(ev)
                payload_value = Json(ev) if Json is not None else json.dumps(ev, ensure_ascii=False)
                cur.execute(
                    f"""
                    INSERT INTO {SUPABASE_EVENTS_TABLE}
                    (event_hash, categoria, category_norm, fecha_text, hora, ciudad, ciudad_norm, lugar, lugar_norm,
                     tipo_norm, artista_norm, is_free, tags, event_date, payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (event_hash) DO UPDATE SET
                        categoria = EXCLUDED.categoria,
                        category_norm = EXCLUDED.category_norm,
                        fecha_text = EXCLUDED.fecha_text,
                        hora = EXCLUDED.hora,
                        ciudad = EXCLUDED.ciudad,
                        ciudad_norm = EXCLUDED.ciudad_norm,
                        lugar = EXCLUDED.lugar,
                        lugar_norm = EXCLUDED.lugar_norm,
                        tipo_norm = EXCLUDED.tipo_norm,
                        artista_norm = EXCLUDED.artista_norm,
                        is_free = EXCLUDED.is_free,
                        tags = EXCLUDED.tags,
                        event_date = EXCLUDED.event_date,
                        payload = EXCLUDED.payload
                    """,
                    (
                        _event_hash(ev),
                        ((ev.get("categoria") or "").strip() or None),
                        enriched.get("category_norm"),
                        ((ev.get("fecha") or "").strip() or None),
                        ((ev.get("hora") or "").strip() or None),
                        ((ev.get("ciudad") or "").strip() or None),
                        enriched.get("ciudad_norm"),
                        ((ev.get("lugar") or "").strip() or None),
                        enriched.get("lugar_norm"),
                        enriched.get("tipo_norm"),
                        enriched.get("artista_norm"),
                        enriched.get("is_free"),
                        enriched.get("tags") or [],
                        _parse_event_date_for_db(ev),
                        payload_value,
                    ),
                )
                upserted += 1
            categorized = _rebuild_stats_trending(cur)
        conn.commit()
        return {"ok": True, "upserted": upserted, "categorized": categorized, "reason": "ok", **status}
    except Exception as e:
        logging.warning("Supabase upsert events failed: %s", e)
        return {"ok": False, "upserted": upserted, "categorized": categorized, "reason": f"exception: {e}", **status}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _upsert_events_to_firestore(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    status = _firestore_status()
    if not events:
        return {"ok": True, "upserted": 0, "reason": "no_events", **status}
    if not status["enabled"]:
        return {"ok": False, "upserted": 0, "reason": "firestore_unavailable", **status}

    upserted = 0
    try:
        collection = firestore_client.collection(FIRESTORE_EVENTS_COLLECTION)
        for ev in events:
            if not isinstance(ev, dict):
                continue
            enriched = _enrich_event_for_sql(ev)
            event_hash = _event_hash(ev)
            payload = dict(ev)
            payload["event_hash"] = event_hash
            payload["category_norm"] = enriched.get("category_norm")
            payload["tipo_norm"] = enriched.get("tipo_norm")
            payload["ciudad_norm"] = enriched.get("ciudad_norm")
            payload["lugar_norm"] = enriched.get("lugar_norm")
            payload["artista_norm"] = enriched.get("artista_norm")
            payload["is_free"] = enriched.get("is_free")
            payload["tags"] = enriched.get("tags") or []
            payload["event_date"] = _parse_event_date_for_db(ev)
            payload["updated_at"] = time.time()
            collection.document(event_hash).set(payload, merge=True)
            upserted += 1

        category_index = _sync_firestore_category_index(events)
        return {
            "ok": True,
            "upserted": upserted,
            "reason": "ok",
            "category_index": category_index,
            **status,
        }
    except Exception as e:
        logging.warning("Firestore upsert events failed: %s", e)
        return {"ok": False, "upserted": upserted, "reason": f"exception: {e}", **status}


def _is_image(name: str) -> bool:
    return any((name or "").lower().endswith(ext) for ext in IMAGE_EXTS)


# --- Filtro rápido PRE-LLM: descarta imágenes que no son flyers de eventos ---

# Palabras clave que sugieren que hay un evento (alcanza con 1)
_EVENT_SIGNAL_RE = re.compile(
    r'(?:'
    r'\d{1,2}[/.]\d{1,2}'
    r'|\d{1,2}\s*(?:de\s+)?(?:ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)'
    r'|\d{1,2}\s*(?:hs?|hrs?)\b'
    r'|\d{1,2}:\d{2}'
    r'|entrada|entradas|gratis|gratuita?|bonificad[ao]'
    r'|boletería|boletera|localidades|antic\w+'
    r'|presenta|en\s+vivo|live|show|festival|fest\b'
    r'|teatro|museo|galería|galeria|anfiteatro|sala|auditorio'
    r'|feria|exposici[oó]n|muestra|inauguraci[oó]n'
    r'|stand\s*up|comedia|humor|recital|concierto'
    r'|dj\b|after|pre\s*venta|early\s*bird'
    r'|lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo'
    r')',
    re.IGNORECASE,
)

# Palabras en caption de IG que sugieren evento incluso sin OCR
_CAPTION_EVENT_RE = re.compile(
    r'(?:'
    r'\d{1,2}[/.]\d{1,2}'
    r'|\d{1,2}\s*hs?\b'
    r'|entradas|tickets|presenta|en\s+vivo|live|show'
    r'|festival|fest\b|feria|exposici[oó]n|muestra'
    r'|teatro|museo|recital|concierto'
    r'|dj\b|stand\s*up|comedia'
    r'|lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo'
    r')',
    re.IGNORECASE,
)

_DENSE_FLYER_SIGNAL_RE = re.compile(
    r'(?:\b\d{1,2}/\d{1,2}\b|\b\d{1,2}\s*hs?\b|\b\d{1,2}:\d{2}\b)',
    re.IGNORECASE,
)

_OCR_BLOCK_NOISE_RE = re.compile(
    r'(?:sponsor|patrocina|auspicia|qr\b|mercadopago|transferencia|alias|cvu|'
    r'instagram|facebook|tiktok|youtube|whatsapp|@\w+|www\.|\.com|'
    r'entrada[s]?\s*\$?\d+)',
    re.IGNORECASE,
)

_EVENTS_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["events"],
    "properties": {
        "events": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "artista_o_show",
                    "lugar",
                    "fecha",
                    "hora",
                    "ciudad",
                    "tipo",
                    "gratis",
                    "categoria",
                    "descripcion",
                ],
                "properties": {
                    "artista_o_show": {"type": "string"},
                    "lugar": {"type": "string"},
                    "fecha": {"type": "string"},
                    "hora": {"type": "string"},
                    "ciudad": {"type": "string"},
                    "tipo": {"type": "string", "enum": ["evento", "agenda"]},
                    "gratis": {"type": "boolean"},
                    "categoria": {
                        "type": "string",
                        "enum": [
                            "boliche",
                            "movido",
                            "movido +30",
                            "tranquilo",
                            "familiar",
                            "al aire libre",
                            "mixto",
                        ],
                    },
                    "descripcion": {"type": "string"},
                },
            },
        }
    },
}


def _events_response_format(strict: bool) -> Dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "events_extraction",
            "schema": _EVENTS_JSON_SCHEMA,
            "strict": bool(strict),
        },
    }


def _quick_skip_non_event(full_text: str, ig_meta: Dict[str, Any]) -> bool:
    """True si la imagen NO parece ser un flyer de evento.
    Se ejecuta después del OCR (barato) y antes de Groq (caro).
    Conservador: ante la duda, NO skipea."""
    ocr = (full_text or "").strip()
    caption = (ig_meta.get("caption") or "").strip()
    location = (ig_meta.get("locationName") or "").strip()

    # Si hay señales de evento en el OCR → procesar
    if ocr and _EVENT_SIGNAL_RE.search(ocr):
        return False

    # Si hay señales de evento en el caption de IG → procesar
    if caption and _CAPTION_EVENT_RE.search(caption):
        return False

    # Si tiene ubicación de IG (venue tag) + algo de texto → procesar
    if location and (len(ocr) > 20 or len(caption) > 30):
        return False

    # OCR muy largo (>100 chars) probablemente tiene info → procesar
    if len(ocr) > 100:
        return False

    # Si no hay OCR Y no hay caption → skip (foto sin texto)
    if not ocr and not caption:
        return True

    # OCR muy corto (< 15 chars) sin señales en caption → skip
    if len(ocr) < 15 and not caption:
        return True

    # OCR corto + caption corto sin señales → skip
    if len(ocr) < 15 and len(caption) < 20:
        return True

    # Caso conservador: no skipear
    return False


def _event_completeness_score(ev: Dict[str, Any]) -> float:
    score = 0.0
    if (ev.get("artista_o_show") or "").strip():
        score += 1.0
    if (ev.get("fecha") or "").strip():
        score += 1.0
    if (ev.get("hora") or "").strip():
        score += 0.5
    if (ev.get("lugar") or "").strip():
        score += 0.5
    return score


def _avg_event_completeness(events: List[Dict[str, Any]]) -> float:
    if not events:
        return 0.0
    valid = [ev for ev in events if isinstance(ev, dict)]
    if not valid:
        return 0.0
    return sum(_event_completeness_score(ev) for ev in valid) / len(valid)


def _has_fecha_y_lugar(ev: Dict[str, Any]) -> bool:
    return bool((ev.get("fecha") or "").strip()) and bool((ev.get("lugar") or "").strip())


def _is_good_enough(events: List[Dict[str, Any]]) -> bool:
    count = len(events or [])
    quality = _avg_event_completeness(events or [])
    if count >= 3:
        return True
    if quality >= 2.6:
        return True
    if count >= 2 and quality >= 2.4:
        good_with_fecha_lugar = sum(1 for ev in (events or []) if isinstance(ev, dict) and _has_fecha_y_lugar(ev))
        if good_with_fecha_lugar >= 2:
            return True
    return False


def _pick_more_complete(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    a_tuple = (
        _event_completeness_score(a),
        len((a.get("descripcion") or "").strip()),
        len((a.get("artista_o_show") or "").strip()),
    )
    b_tuple = (
        _event_completeness_score(b),
        len((b.get("descripcion") or "").strip()),
        len((b.get("artista_o_show") or "").strip()),
    )
    return b if b_tuple > a_tuple else a


def _merge_events_prefer_complete(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for ev in (primary or []):
        if isinstance(ev, dict):
            merged[_merge_dedup_key(ev)] = ev
    for ev in (secondary or []):
        if not isinstance(ev, dict):
            continue
        k = _merge_dedup_key(ev)
        if k in merged:
            merged[k] = _pick_more_complete(merged[k], ev)
        else:
            merged[k] = ev
    return list(merged.values())


def _should_use_hard_fallback(rows_count: int, date_hits: int, block_count: int, events_count: int, avg_quality: float) -> bool:
    if not GROQ_HARD_FALLBACK_ENABLED:
        return False
    dense = (
        rows_count >= HARD_FALLBACK_MIN_ROWS
        or date_hits >= HARD_FALLBACK_MIN_DATE_HITS
        or block_count >= HARD_FALLBACK_MIN_BLOCKS
    )
    still_poor = (events_count <= HARD_FALLBACK_MAX_EVENTS_OK) or (avg_quality < HARD_FALLBACK_MIN_QUALITY)
    return dense and still_poor


def _parsed_out_name(object_name: str) -> str:
    return f"{PARSED_PREFIX}/{object_name}.json"


def _events_out_name(object_name: str) -> str:
    base = object_name.replace("/", "_")
    return f"{EVENTS_PREFIX}/{base}.events.json"


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _upper_norm(s: str) -> str:
    return _norm_space(s).upper()


def _maybe_city_fix(city: str) -> str:
    c = _upper_norm(city)
    if not c:
        return ""
    if c in CITY_MAP:
        return CITY_MAP[c]
    if c in KNOWN_CITIES_UPPER:
        return _norm_space(city)
    return ""


def _extract_paren_city(text: str) -> Tuple[str, str]:
    for m in re.finditer(r"\(\s*([^)]+?)\s*\)", text):
        candidate = _norm_space(m.group(1))
        city = _maybe_city_fix(candidate)
        if city:
            cleaned = text[:m.start()] + text[m.end():]
            return city, _norm_space(cleaned)
    return "", text


def _convert_to_jpeg_if_needed(object_name: str, image_bytes: bytes) -> Tuple[bytes, str, bool]:
    lower = (object_name or "").lower()
    if lower.endswith((".jpg", ".jpeg")):
        return image_bytes, "image/jpeg", False
    if lower.endswith(".png"):
        return image_bytes, "image/png", False
    try:
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=92, optimize=True)
        return out.getvalue(), "image/jpeg", True
    except Exception as e:
        raise RuntimeError(f"convert_failed: {e}")


# =========================
# OCR (Tesseract)
# =========================
def _run_tesseract_ocr(image_bytes: bytes) -> Tuple[str, List[Dict[str, Any]]]:
    if pytesseract is None:
        logging.warning("pytesseract no disponible")
        return "", []

    try:
        from PIL import Image
        import io
    except Exception:
        logging.warning("Pillow no disponible para OCR")
        return "", []

    try:
        image = Image.open(io.BytesIO(image_bytes))
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")

        data = pytesseract.image_to_data(
            image,
            lang=TESSERACT_LANG,
            config="--oem 1 --psm 6",
            output_type=pytesseract.Output.DICT,
        )
    except Exception as exc:
        logging.warning("Tesseract OCR fallo: %s", exc)
        return "", []

    lines_data: List[Dict[str, Any]] = []
    texts = data.get("text") or []
    for i, text in enumerate(texts):
        raw_text = str(text or "").strip()
        if not raw_text:
            continue

        conf_str = str((data.get("conf") or ["-1"])[i])
        try:
            conf = float(conf_str)
        except Exception:
            conf = -1.0
        if conf < 0:
            continue

        left = int((data.get("left") or [0])[i] or 0)
        top = int((data.get("top") or [0])[i] or 0)
        width = int((data.get("width") or [0])[i] or 0)
        height = int((data.get("height") or [0])[i] or 0)
        if width <= 0 or height <= 0:
            continue

        cx = left + (width / 2.0)
        cy = top + (height / 2.0)
        lines_data.append(
            {
                "text": raw_text,
                "cx": cx,
                "cy": cy,
                "h": float(height),
                "w": float(width),
                "x_min": float(left),
                "x_max": float(left + width),
            }
        )

    if not lines_data:
        return "", []

    lines_data.sort(key=lambda a: a["cy"])
    full_text = "\n".join(ld["text"] for ld in lines_data)

    # Agrupar por proximidad vertical para reconstruir filas de agenda.
    Y_TOL = 16
    grouped: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    last_cy: Optional[float] = None
    for ld in lines_data:
        if last_cy is None:
            cur = [ld]
            last_cy = ld["cy"]
            continue
        if abs(ld["cy"] - last_cy) <= Y_TOL:
            cur.append(ld)
        else:
            grouped.append(cur)
            cur = [ld]
            last_cy = ld["cy"]
    if cur:
        grouped.append(cur)

    max_x = max(ld["x_max"] for ld in lines_data)
    canvas_width = max_x if max_x > 0 else 1
    left_cut = canvas_width * 0.22
    right_cut = canvas_width * 0.75

    rows: List[Dict[str, Any]] = []
    for group in grouped:
        group.sort(key=lambda a: a["cx"])
        left_parts = [g["text"] for g in group if g["cx"] <= left_cut]
        right_parts = [g["text"] for g in group if g["cx"] >= right_cut]
        mid_parts = [g["text"] for g in group if left_cut < g["cx"] < right_cut]
        rows.append(
            {
                "y": sum(g["cy"] for g in group) / len(group),
                "h": max(g["h"] for g in group),
                "span": sum(g["w"] for g in group),
                "left": _norm_space(" ".join(left_parts)),
                "mid": _norm_space(" ".join(mid_parts)),
                "right": _norm_space(" ".join(right_parts)),
            }
        )

    rows = [r for r in rows if (r["left"] or r["mid"] or r["right"])]
    rows.sort(key=lambda r: r["y"])
    return full_text, rows


# =========================
# HEADER (fecha) DETECTION
# =========================
def _detect_header(rows: List[Dict[str, Any]]) -> str:
    for r in rows[:18]:
        txt = _norm_space(f"{r.get('left','')} {r.get('mid','')} {r.get('right','')}")
        mdm = DM_RE.search(txt)
        if mdm:
            dd, mm = mdm.group(1), mdm.group(2)
            return f"{dd.zfill(2)}/{mm.zfill(2)}"
    for r in rows[:18]:
        txt = _norm_space(f"{r.get('left','')} {r.get('mid','')} {r.get('right','')}")
        mday = DAY_RE.search(txt)
        if mday:
            header_day = mday.group(1).upper()
            nums = re.findall(r"\b\d{1,2}\b", txt)
            header_num = nums[0] if nums else ""
            return _norm_space(f"{header_day} {header_num}".strip())
    return ""


# =========================
# FORMAT ROWS FOR LLM
# =========================
def _format_rows_for_llm(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    gaps = [rows[i + 1].get("y", 0) - rows[i].get("y", 0) for i in range(len(rows) - 1)]
    sorted_gaps = sorted(g for g in gaps if g > 0) if gaps else []
    if sorted_gaps:
        median_gap = sorted_gaps[len(sorted_gaps) // 2]
        block_threshold = max(40, median_gap * 1.6)
    else:
        block_threshold = 40

    lines: List[str] = []
    for i, r in enumerate(rows):
        if i > 0 and gaps[i - 1] > block_threshold:
            lines.append("---")
        left = (r.get("left", "") or "").strip()
        mid = (r.get("mid", "") or "").strip()
        right = (r.get("right", "") or "").strip()
        parts = []
        if left:
            parts.append(f"[L] {left}")
        if mid:
            parts.append(f"[M] {mid}")
        if right:
            parts.append(f"[R] {right}")
        if parts:
            y = int(round(float(r.get("y") or 0)))
            h = int(round(float(r.get("h") or 0)))
            lines.append(f"[y={y} h={h}] " + "  ".join(parts))
    return "\n".join(lines)


def _split_ocr_rows_into_blocks(rows: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda r: float(r.get("y") or 0.0))
    gaps: List[float] = []
    for i in range(len(sorted_rows) - 1):
        y1 = float(sorted_rows[i].get("y") or 0.0)
        y2 = float(sorted_rows[i + 1].get("y") or 0.0)
        if y2 > y1:
            gaps.append(y2 - y1)

    if gaps:
        sorted_gaps = sorted(gaps)
        median_gap = sorted_gaps[len(sorted_gaps) // 2]
        big_gap = max(34.0, median_gap * 1.8)
    else:
        big_gap = 40.0

    blocks: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = [sorted_rows[0]]
    for i in range(1, len(sorted_rows)):
        prev = sorted_rows[i - 1]
        cur = sorted_rows[i]
        prev_y = float(prev.get("y") or 0.0)
        cur_y = float(cur.get("y") or 0.0)
        if (cur_y - prev_y) > big_gap:
            blocks.append(current)
            current = [cur]
        else:
            current.append(cur)
    if current:
        blocks.append(current)

    if not blocks:
        return [sorted_rows]
    return blocks


def _score_ocr_block(block: List[Dict[str, Any]]) -> float:
    if not block:
        return -999.0
    txt = _format_rows_for_llm(block)
    date_hits = len(_DENSE_FLYER_SIGNAL_RE.findall(txt))
    has_venue = bool(_VENUE_CONTEXT_RE.search(txt))
    has_signal = bool(_EVENT_SIGNAL_RE.search(txt))
    noise_hits = len(_OCR_BLOCK_NOISE_RE.findall(txt))
    density = min(len(block) / 6.0, 2.0)
    length_bonus = min(len(txt) / 220.0, 1.0)
    return (date_hits * 1.4) + (1.0 if has_venue else 0.0) + (1.0 if has_signal else 0.0) + density + length_bonus - (noise_hits * 0.7)


def _pick_top_ocr_blocks(blocks: List[List[Dict[str, Any]]], limit: int) -> List[List[Dict[str, Any]]]:
    if not blocks:
        return []
    scored = [(b, _score_ocr_block(b)) for b in blocks if b]
    scored.sort(key=lambda x: x[1], reverse=True)
    out = [b for b, score in scored[:max(1, limit)] if score > -100.0]
    return out


# =========================
# FORMAT IG METADATA
# =========================
def _format_ig_metadata(ig_meta: Dict[str, Any]) -> str:
    if not ig_meta:
        return ""
    parts: List[str] = []
    caption = (ig_meta.get("caption") or "").strip()
    if caption:
        parts.append(f"Caption: {caption}")
    location = (ig_meta.get("locationName") or "").strip()
    if location:
        parts.append(f"Ubicación: {location}")
    owner = (ig_meta.get("ownerFullName") or "").strip()
    owner_user = (ig_meta.get("ownerUsername") or "").strip()
    if owner:
        parts.append(f"Cuenta: {owner}" + (f" (@{owner_user})" if owner_user else ""))
    mentions = ig_meta.get("mentions") or []
    if mentions:
        parts.append(f"Mencionados: {', '.join('@' + m for m in mentions)}")
    tagged = ig_meta.get("taggedUsers") or []
    if tagged:
        tag_strs = []
        for t in tagged:
            name = (t.get("full_name") or "").strip()
            user = (t.get("username") or "").strip()
            if name and user:
                tag_strs.append(f"{name} (@{user})")
            elif user:
                tag_strs.append(f"@{user}")
        if tag_strs:
            parts.append(f"Etiquetados: {', '.join(tag_strs)}")
    coauthors = ig_meta.get("coauthorProducers") or []
    if coauthors:
        co_strs = []
        for c in coauthors:
            name = (c.get("full_name") or "").strip()
            user = (c.get("username") or "").strip()
            if name and user:
                co_strs.append(f"{name} (@{user})")
            elif user:
                co_strs.append(f"@{user}")
        if co_strs:
            parts.append(f"Coautores del post: {', '.join(co_strs)}")
    alt = (ig_meta.get("alt") or "").strip()
    if alt:
        parts.append(f"Alt text: {alt}")
    if not parts:
        return ""
    return "\n".join(parts)


# =========================
# METADATA EXTRACTION HELPERS
# =========================
def _metadata_extract_venue(ig_meta: Optional[Dict[str, Any]]) -> str:
    ig_meta = ig_meta or {}
    for key in ("taggedUsers", "coauthorProducers"):
        for item in (ig_meta.get(key) or []):
            name = _norm_space(str(item.get("full_name") or ""))
            user = _norm_space(str(item.get("username") or ""))
            if name and _VENUE_CONTEXT_RE.search(name):
                return name
            if user and _VENUE_CONTEXT_RE.search(user):
                return name or user
    owner = _norm_space(str(ig_meta.get("ownerFullName", "") or ""))
    owner_user = _norm_space(str(ig_meta.get("ownerUsername", "") or ""))
    if owner and _VENUE_CONTEXT_RE.search(owner):
        return owner
    if owner_user and _VENUE_CONTEXT_RE.search(owner_user):
        return owner or owner_user
    return ""


def _metadata_extract_artist(ig_meta: Optional[Dict[str, Any]]) -> str:
    ig_meta = ig_meta or {}
    owner = _norm_space(str(ig_meta.get("ownerFullName", "") or ""))
    owner_user = _norm_space(str(ig_meta.get("ownerUsername", "") or ""))
    if owner and _VENUE_CONTEXT_RE.search(owner):
        return ""
    if owner_user and _VENUE_CONTEXT_RE.search(owner_user):
        return ""
    if owner_user and _AGGREGATOR_RE_META.search(owner_user):
        return ""
    if owner and _AGGREGATOR_RE_META.search(owner):
        return ""
    return owner or ""


def _metadata_extract_ciudad(ig_meta: Optional[Dict[str, Any]]) -> str:
    location = _norm_space(str((ig_meta or {}).get("locationName", "") or ""))
    if not location:
        return ""
    for p in (pp.strip() for pp in location.split(",")):
        if _upper_norm(p) in KNOWN_CITIES_UPPER or _upper_norm(p) in CITY_MAP:
            return p
        fixed = _maybe_city_fix(p)
        if fixed:
            return fixed
    return ""


def _infer_venue_from_rows(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""

    for row in rows[:48]:
        left = _norm_space(str(row.get("left") or ""))
        mid = _norm_space(str(row.get("mid") or ""))
        right = _norm_space(str(row.get("right") or ""))
        text = _norm_space(" ".join(part for part in [left, mid, right] if part))
        if not text:
            continue

        lower_text = _strip_accents(text).lower()
        if _BARE_SCHEDULE_RE.match(text) or _TIME_ONLY_FECHA_RE.match(text):
            continue
        if _NOISE_ANNOUNCEMENT_RE.match(text):
            continue
        if "@" in text and len(text.split()) <= 3:
            continue

        if _VENUE_CONTEXT_RE.search(lower_text):
            return text

    return ""


def _enrich_events_with_metadata_context(events: List[Dict[str, Any]], ig_meta: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not events or not ig_meta:
        return events
    meta_venue = _metadata_extract_venue(ig_meta)
    meta_artist = _metadata_extract_artist(ig_meta)
    meta_ciudad = _metadata_extract_ciudad(ig_meta)
    out: List[Dict[str, Any]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        item = dict(ev)
        if not _norm_space(str(item.get("lugar", "") or "")) and meta_venue:
            item["lugar"] = meta_venue
        if not _norm_space(str(item.get("artista_o_show", "") or "")) and meta_artist:
            item["artista_o_show"] = meta_artist
        if not _norm_space(str(item.get("ciudad", "") or "")) and meta_ciudad:
            item["ciudad"] = meta_ciudad
        out.append(item)
    return out


# =========================
# WEEKDAY / FECHA HELPERS
# =========================
def _fix_weekday_in_fecha(fecha: str) -> str:
    if not fecha:
        return fecha
    from datetime import datetime as _dt

    def _fix_match(m):
        day_name = m.group(1).lower().replace('á', 'a').replace('é', 'e')
        day = int(m.group(2))
        month_name = m.group(3).lower()
        month = _MONTH_TO_NUM.get(month_name)
        if not month:
            return m.group(0)
        try:
            year = _dt.now().year
            actual = _dt(year, month, day)
            expected = _WEEKDAY_MAP.get(day_name)
            if expected is not None and actual.weekday() != expected:
                correct = _WEEKDAY_NAMES_ES[actual.weekday()]
                return correct + m.group(0)[len(m.group(1)):]
        except ValueError:
            pass
        return m.group(0)

    return _WEEKDAY_FECHA_RE.sub(_fix_match, fecha)


def _maybe_fix_fecha(fecha: str, descripcion: str, tipo: str) -> str:
    if tipo != "evento" or not descripcion:
        return fecha
    for pat in (_MULTI_DAY_RE, _MULTI_NUM_RE):
        m = pat.search(descripcion)
        if m and len(m.group(1)) > len(fecha or ""):
            return m.group(1).strip()
    return fecha


# =========================
# NOISE / QUALITY FILTERS
# =========================
def _looks_like_bare_schedule(text: str) -> bool:
    return bool(_BARE_SCHEDULE_RE.match(_norm_space(text))) if text else False


def _looks_like_bare_venue(text: str) -> bool:
    return bool(_BARE_VENUE_RE.match(_norm_space(text))) if text else False


def _looks_like_photo_ocr_garbage(text: str) -> bool:
    text = _norm_space(text)
    if not text:
        return False
    if _PEOPLE_LIST_OCR_RE.search(text):
        return True
    names = _PERSON_NAME_RE.findall(text)
    if len(names) >= 3 and not _VENUE_CONTEXT_RE.search(text):
        desc_up = _upper_norm(text)
        if not any(kw in desc_up for kw in ("PRESENTA", "SHOW", "RECITAL", "TRIBUTO", "COVER", "OBRA", "STAND")):
            return True
    return False


# =========================
# GROQ SYSTEM PROMPT
# =========================
_GROQ_EXTRACT_SYSTEM = """\
Extraé eventos de este flyer de Rosario, Argentina.
Estás VIENDO LA IMAGEN del flyer directamente. Leela como la leería una persona.

TU FUENTE PRINCIPAL ES LA IMAGEN. Leé el texto que ves en la imagen.
Opcionalmente tenés texto OCR auxiliar y/o metadata de Instagram como contexto extra.

CÓMO LEER LA IMAGEN:
1. Mirá la imagen completa. El texto MÁS GRANDE suele ser el artista o nombre del show.
2. Buscá QUÉ hay (artista, obra, show, banda, muestra, recorrido, actividad).
3. Buscá DÓNDE (venue, lugar — leé el nombre exacto como aparece).
4. Buscá CUÁNDO (fecha, día, hora).
5. Si hay texto artístico, distorsionado o con tipografías raras, leelo con atención — es lo más importante del flyer.
6. Ignorá: sponsors, ticketeras, URLs, logos, texto decorativo, nombres leídos de fotos/remeras/cuadros.

TEXTO OCR AUXILIAR (si hay):
- Es un OCR automático que puede tener errores. Usalo para CONFIRMAR lo que ves en la imagen, no como fuente primaria.
- El OCR puede partir palabras ("DESCA RILADOS" = "Descarrilados") o pegarlas ("LARISA" = "LA RISA"). Vos leé de la imagen directamente.

METADATA DE INSTAGRAM (si hay "=== METADATA INSTAGRAM ==="):
- "Caption": Contiene fecha ("Este 7/3"), precio, artista, lugar. Extraé TODA la info útil.
- "Cuenta" / ownerUsername: Si tiene "teatro"/"sala"/"museo"/"bar"/"cultural" → es LUGAR. Si no → posible ARTISTA.
- "Etiquetados" / taggedUsers: Si nombre tiene "sala"/"teatro"/"museo"/"anfiteatro" → LUGAR.
- "Coautores del post": Suelen ser la sala/venue.
- "Ubicación": Ciudad o barrio → para "ciudad", NO para "lugar".
- "Alt text": Puede tener info clave.

POSTS CON VARIAS IMÁGENES (CAROUSEL):
- Esta imagen puede ser UNA de varias del mismo post de Instagram. El caption describe TODAS las imágenes.
- Extraé SOLO el/los eventos que se ven EN ESTA IMAGEN. No repitas eventos de otras imágenes que no ves.
- Si esta imagen es una portada/título general sin evento específico (ej: "Actividades de marzo"), igual extraé lo que puedas, priorizando la info del caption que corresponda a esta imagen.
- Si el caption menciona varias actividades pero esta imagen muestra UNA sola, extraé solo ESA.

REGLAS DE VALIDACIÓN:
- Un evento tipo "evento" necesita como mínimo: artista/show/actividad + fecha. Si además tiene lugar, mejor. Si NO tiene lugar, incluilo igual con lugar="".
- Un evento tipo "agenda" es para horarios regulares o programación fija (ej: "Martes a viernes 14 a 18h"). Necesita como mínimo el nombre del lugar o institución.
- Si un dato no está ni en la imagen ni en la metadata, NO lo inventes.
- Fragmentos sueltos (solo un nombre aislado sin ningún otro dato) = NO son eventos.

REGLAS DE CONTENIDO:
- El texto más GRANDE del flyer es el artista o nombre del show. Si hay dos textos grandes, incluí AMBOS.
- Si hay GRILLA / LINEUP con varios artistas y horarios (o bloques claramente separados), creá UN evento por cada artista/bloque.
- Si hay varios artistas en el mismo lugar y fecha PERO no hay forma confiable de separarlos por bloque/horario, podés agruparlos en un solo evento.
- Teatro/bar/sala/museo/centro cultural = es un LUGAR, no un artista.
- Plazas, parques, intersecciones de calles, puntos de encuentro = son LUGARES válidos (ej: "Plaza Rodolfo Walsh", "Maestro Santafesino y D. Isola").
- No inventes nada. Solo extraé lo que ves en la imagen + metadata.
- Si el caption habla en pasado ("estuvo increíble", "gracias por venir") = ya pasó, devolvé {"events": []}.
- Si el flyer muestra una GIRA/TOUR con varias ciudades y fechas, creá UN evento por cada ciudad (cada uno con su fecha y ciudad correspondiente).
- Cambios de horario, nuevos horarios, programación de un museo/centro cultural = tipo "agenda".
- Recorridos mediados, caminatas guiadas, visitas guiadas = son eventos válidos. La actividad es el artista_o_show (ej: "Barrio Explorado - Recorrido mediado").

CAMPOS (todos obligatorios en el JSON):
- "artista_o_show": Nombre del artista, banda, obra, show o actividad del evento puntual. Si hay artista + nombre de show, poné ambos: "Pablito Castillo - La Risa Que Me Parió". Para recorridos/caminatas/visitas, usá el nombre de la actividad. En grillas/lineups, cada evento debe llevar su artista o bloque correspondiente (no mezclar todos en uno).
- "lugar": Nombre del venue exacto como aparece. NUNCA "Rosario" como lugar — eso es ciudad. Si no hay venue visible, dejá "".
- "fecha": Fecha/día del evento (ej: "viernes 15 de mayo"). Para agenda: "martes a viernes" o "a partir de marzo".
- "hora": Horario (ej: "21 hs"). Si no hay hora explícita, "".
- "ciudad": localidad real o "".
- "tipo": "evento" o "agenda".
- "gratis": true/false.
- "categoria": una de estas opciones según el tipo de evento:
    - "boliche": fiesta electrónica, rave, eventos con 3+ DJs, pool party, after party, formato boliche/discoteca
    - "movido": música en vivo, recital, show musical, cumbia, rock, bandas, carnaval
    - "movido +30": igual que movido pero orientado a público adulto/+30 (tango, jazz, folclore, trova)
    - "tranquilo": teatro, museo, exposición, cine, muestra, galería
    - "familiar": actividades para toda la familia, talleres infantiles, eventos para chicos
    - "al aire libre": caminatas, recorridos, ferias al aire libre, actividades en plazas/parques
    - "mixto": stand up, karaoke, varieté, eventos que mezclan géneros o no encajan en los anteriores
- "descripcion": Línea de cartelera natural. Ej: "'La Risa Que Me Parió' con Pablito Castillo en Teatro Broadway, jueves 2, viernes 3 y domingo 5 de abril"

Razoná 2-3 líneas y después:
```json
{"events": [...]}
```\
"""


_GROQ_HARD_BLOCK_SYSTEM = """\
Convertí este bloque OCR de agenda/eventos en JSON estructurado.
Reglas:
- Creá un evento por artista, show o bloque distinguible.
- No mezcles dos shows distintos en un solo evento.
- Si una fecha, hora o lugar aparece como encabezado del bloque, aplicala solo a ese bloque.
- Ignorá sponsors, redes, precios, QR, medios de pago y texto promocional.
- Si hay lineup con horarios separados, devolvé múltiples eventos si realmente son shows distinguibles; si es una sola fiesta con lineup interno, devolvé un evento principal con la mejor descripcion posible.
- No inventes datos.
- Devolvé solo JSON válido siguiendo exactamente el schema provisto.
"""


# =========================
# GROQ CALL
# =========================
def _groq_call(
    messages: List[Dict[str, Any]],
    dbg: Dict[str, Any],
    model: str = GROQ_MODEL_EXTRACT,
    api_url: str = GROQ_URL,
    api_key: str = GROQ_API_KEY,
    response_format: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.0,
    }
    if response_format:
        payload["response_format"] = response_format
    rf_disabled = False
    requested_model = model
    downgraded_hard_model = False
    max_attempts = 2 if requested_model == GROQ_MODEL_FALLBACK_TEXT_HARD else 3
    for attempt in range(max_attempts):
        try:
            r = requests.post(
                api_url,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=60,
            )
            dbg["http_status"] = r.status_code
            if r.status_code == 429:
                dbg["error"] = (r.text or "")[:800]

                rl_limit = r.headers.get("x-ratelimit-limit")
                rl_remaining = r.headers.get("x-ratelimit-remaining")
                rl_reset = r.headers.get("x-ratelimit-reset")
                if rl_limit or rl_remaining or rl_reset:
                    logging.warning(
                        "[GROQ] 429 model=%s limit=%s remaining=%s reset=%s",
                        payload.get("model"), rl_limit, rl_remaining, rl_reset,
                    )

                retry_after_raw = r.headers.get("retry-after")
                retry_after = 0.0
                if retry_after_raw:
                    try:
                        retry_after = float(retry_after_raw)
                    except Exception:
                        retry_after = 0.0

                if payload.get("model") == GROQ_MODEL_FALLBACK_TEXT_HARD and not downgraded_hard_model:
                    payload["model"] = GROQ_MODEL_FALLBACK_TEXT
                    downgraded_hard_model = True
                    dbg["hard_downgraded_to"] = GROQ_MODEL_FALLBACK_TEXT

                backoff = max(retry_after, min(10.0, (1.3 ** attempt) + random.uniform(0.2, 0.9)))
                time.sleep(backoff)
                continue
            if r.status_code >= 400:
                dbg["error"] = (r.text or "")[:800]
                if (not rf_disabled) and response_format and "response_format" in (r.text or "").lower():
                    payload.pop("response_format", None)
                    rf_disabled = True
                    continue
                return None
            content = r.json()["choices"][0]["message"]["content"]
            return content
        except Exception as e:
            dbg["error"] = str(e)
            time.sleep(1.0 + attempt)
    return None


def _extract_balanced_json(s: str) -> str:
    depth = 0
    in_str = False
    escape = False
    for i, ch in enumerate(s):
        if escape:
            escape = False
            continue
        if ch == '\\' and in_str:
            escape = True
            continue
        if ch == '"' and not escape:
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return s[:i + 1]
    return s


# =========================
# GROQ EXTRACT EVENTS
# =========================
def _groq_extract_events(full_text: str, header: str, rows: Optional[List[Dict[str, Any]]] = None,
                         ig_meta: Optional[Dict[str, Any]] = None,
                         image_bytes: Optional[bytes] = None,
                         model: str = GROQ_MODEL_EXTRACT,
                         api_url: str = GROQ_URL,
                         api_key: str = GROQ_API_KEY,
                         response_format: Optional[Dict[str, Any]] = None,
                         system_prompt: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    dbg = {"attempted": False, "http_status": None, "error": None, "model": model}
    if not api_key:
        return [], dbg

    ig_text = _format_ig_metadata(ig_meta or {})
    has_ocr = bool(full_text.strip())
    has_meta = bool(ig_text)
    has_image = bool(image_bytes)

    if not has_image and not has_ocr and not has_meta:
        return [], dbg

    dbg["attempted"] = True

    t_llm_start = 0.0
    t_llm_end = 0.0
    t_norm_start = 0.0
    t_norm_end = 0.0

    # Armar texto auxiliar
    text_parts: List[str] = []
    if has_ocr:
        if rows:
            text_parts.append("=== TEXTO OCR AUXILIAR (puede tener errores, confirmá con la imagen) ===\n" + _format_rows_for_llm(rows))
        else:
            text_parts.append("=== TEXTO OCR AUXILIAR ===\n" + full_text.strip())
    if header:
        text_parts.insert(0, f"[Fecha detectada en el header: {header}]")
    if ig_text:
        text_parts.append(f"=== METADATA INSTAGRAM ===\n{ig_text}")
    if not has_image and not has_ocr:
        text_parts.append("[Sin texto OCR legible ni imagen. Extraé el evento usando SOLO la metadata de Instagram.]")

    user_text = "\n\n".join(text_parts) if text_parts else "Extraé los eventos de la imagen."

    # Mensaje multimodal o solo texto
    if has_image:
        img_b64 = base64.b64encode(image_bytes).decode("ascii")
        if image_bytes[:8].startswith(b'\x89PNG'):
            mime = "image/png"
        elif image_bytes[:2] == b'\xff\xd8':
            mime = "image/jpeg"
        else:
            mime = "image/jpeg"
        user_content: Union[str, List[Dict[str, Any]]] = [
            {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}},
            {"type": "text", "text": user_text},
        ]
        dbg["vision"] = True
    else:
        user_content = user_text
        dbg["vision"] = False

    today_str = time.strftime("%Y-%m-%d")
    base_system = system_prompt or _GROQ_EXTRACT_SYSTEM
    effective_system = base_system + f"\n\nFecha de hoy: {today_str}. Usá el año actual ({time.strftime('%Y')}) para las fechas de los eventos."

    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": effective_system},
        {"role": "user", "content": user_content},
    ]

    t_llm_start = time.perf_counter()
    content = _groq_call(messages, dbg, model=model, api_url=api_url, api_key=api_key, response_format=response_format)
    t_llm_end = time.perf_counter()
    dbg["llm_call_ms"] = _ms(t_llm_start, t_llm_end)
    if not content:
        dbg["normalize_ms"] = 0.0
        return [], dbg

    # Parse JSON response
    try:
        cleaned = content.strip()
        fence_open = re.search(r"```(?:json)?\s*\n?", cleaned)
        if fence_open:
            rest = cleaned[fence_open.end():]
            fence_close = rest.find("```")
            json_str = rest[:fence_close].strip() if fence_close != -1 else rest.strip()
            reasoning = cleaned[:fence_open.start()].strip()[:500]
        else:
            brace_start = cleaned.find("{")
            if brace_start != -1:
                json_str = _extract_balanced_json(cleaned[brace_start:])
                reasoning = cleaned[:brace_start].strip()[:500]
            else:
                dbg["error"] = f"no_json_found | raw: {cleaned[:500]}"
                return [], dbg

        if reasoning:
            dbg["reasoning"] = reasoning

        obj = json.loads(json_str)
        out = obj.get("events", [])
        if isinstance(out, list):
            t_norm_start = time.perf_counter()
            normalized = _normalize_groq_events(out, header, rows=rows, ig_meta=ig_meta)
            enriched = _enrich_events_with_metadata_context(normalized, ig_meta)
            t_norm_end = time.perf_counter()
            dbg["normalize_ms"] = _ms(t_norm_start, t_norm_end)
            return enriched, dbg
    except json.JSONDecodeError:
        try:
            last_bracket = json_str.rfind("]")
            if last_bracket > 0:
                json_str_fixed = json_str[:last_bracket + 1] + "}"
                obj = json.loads(json_str_fixed)
                out = obj.get("events", [])
                if isinstance(out, list):
                    t_norm_start = time.perf_counter()
                    normalized = _normalize_groq_events(out, header, rows=rows, ig_meta=ig_meta)
                    enriched = _enrich_events_with_metadata_context(normalized, ig_meta)
                    t_norm_end = time.perf_counter()
                    dbg["normalize_ms"] = _ms(t_norm_start, t_norm_end)
                    return enriched, dbg
        except Exception:
            pass
        dbg["error"] = f"json_parse_failed | raw: {content[:500]}"
    except Exception as e:
        dbg["error"] = f"json_parse: {e} | raw: {content[:500]}"

    dbg["normalize_ms"] = 0.0
    return [], dbg


def _vision_extract_events(
    image_bytes: Optional[bytes],
    ig_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    # Mantiene la misma firma para no romper llamadas existentes, pero usa siempre el extractor visual principal.
    return _groq_extract_events(
        full_text="",
        header="",
        rows=None,
        ig_meta=ig_meta,
        image_bytes=image_bytes,
        model=GROQ_MODEL_EXTRACT,
        api_url=GROQ_URL,
        api_key=GROQ_API_KEY,
    )


# =========================
# NORMALIZE GROQ EVENTS
# =========================
def _normalize_groq_events(
    events: List[Dict[str, Any]],
    header: str,
    rows: Optional[List[Dict[str, Any]]] = None,
    ig_meta: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    _VALID_CATS = {
        "boliche",
        "movido",
        "movido +30",
        "tranquilo",
        "familiar",
        "al aire libre",
        "mixto",
    }

    weekday_only_re = re.compile(
        r"^(lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)$",
        re.IGNORECASE,
    )
    header_full_weekday_re = re.compile(
        r"^(lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)\s+\d{1,2}\s+de\s+\w+(?:\s+de\s+\d{4})?$",
        re.IGNORECASE,
    )

    def _clean_weekday(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip().lower())

    def _weekday_matches_header(fecha_txt: str, header_txt: str) -> bool:
        fecha_norm = _clean_weekday(fecha_txt)
        header_norm = _clean_weekday(header_txt)
        if not weekday_only_re.match(fecha_norm):
            return False
        if not header_full_weekday_re.match(header_norm):
            return False

        fecha_day = _strip_accents(fecha_norm)
        header_day = _strip_accents(header_norm).split()[0]
        return fecha_day == header_day

    def _upgrade_partial_fecha(fecha_txt: str, header_txt: str) -> str:
        fecha_txt = _norm_space(fecha_txt or "")
        header_txt = _norm_space(header_txt or "")

        if not fecha_txt:
            return header_txt or ""

        # Si el evento vino solo con "miércoles" y el header tiene "miércoles 25 de marzo",
        # usar la fecha completa del header.
        if header_txt and _weekday_matches_header(fecha_txt, header_txt):
            return header_txt

        return fecha_txt

    for ev in events:
        if not isinstance(ev, dict):
            continue

        artista_o_show = _norm_space(str(ev.get("artista_o_show", "") or ""))
        lugar = _norm_space(str(ev.get("lugar", "") or ""))
        fecha = _norm_space(str(ev.get("fecha", "") or ""))
        hora = _norm_space(str(ev.get("hora", "") or ""))
        descripcion = _norm_space(str(ev.get("descripcion", "") or ""))
        ciudad = _norm_space(str(ev.get("ciudad", "") or ""))
        tipo = str(ev.get("tipo", "evento") or "evento").strip().lower()
        categoria = str(ev.get("categoria", "mixto") or "mixto").strip().lower()
        gratis = ev.get("gratis", False)

        if isinstance(gratis, str):
            gratis = gratis.strip().lower() in ("true", "si", "sí", "1")

        if tipo not in ("evento", "agenda"):
            tipo = "evento"

        if categoria not in _VALID_CATS:
            categoria = "mixto"

        # Enriquecer desde metadata
        if ig_meta:
            if not lugar:
                lugar = _metadata_extract_venue(ig_meta)
            if not artista_o_show:
                artista_o_show = _metadata_extract_artist(ig_meta)
            if not ciudad:
                ciudad = _metadata_extract_ciudad(ig_meta)

        # Limpiar fechas basura
        if fecha.lower() in (
            "no determinada",
            "no disponible",
            "a confirmar",
            "por confirmar",
            "sin fecha",
            "fecha no determinada",
            "fecha no disponible",
            "tbd",
            "soon",
        ):
            fecha = ""

        # Si viene vacía, usar header
        if tipo == "evento" and not fecha and header:
            fecha = _norm_space(header)

        # Si viene parcial tipo "miércoles", subirla a la completa usando el header
        fecha = _upgrade_partial_fecha(fecha, header)

        # Si vino solo una hora como "20 hs" en fecha por error, limpiar
        if fecha and _TIME_ONLY_FECHA_RE.match(fecha):
            fecha = _upgrade_partial_fecha("", header)

        # Si la fecha es una lista de horarios o algo claramente no-fecha, vaciar
        if fecha and (
            _NOISE_ANNOUNCEMENT_RE.match(fecha)
            or _BARE_SCHEDULE_RE.match(fecha)
        ):
            fecha = _upgrade_partial_fecha("", header)

        # Si el lugar es sospechoso de agenda/portal genérico, vaciarlo
        if lugar and _AGGREGATOR_RE_META.search(lugar):
            lugar = ""

        # Intento de venue desde OCR rows si todavía no hay lugar
        if not lugar and rows:
            venue_candidate = _infer_venue_from_rows(rows)
            if venue_candidate:
                lugar = _norm_space(venue_candidate)

        # Intento de ciudad desde lugar o metadata
        if not ciudad and lugar:
            upper_lugar = _strip_accents(lugar).upper()
            if upper_lugar in KNOWN_CITIES_UPPER:
                ciudad = CITY_MAP.get(upper_lugar, lugar)

        # Si no hay artista, intentar usar primera parte útil de descripción
        if not artista_o_show and descripcion:
            first_line = descripcion.split("\n")[0].strip()
            if first_line and len(first_line) <= 120:
                artista_o_show = first_line

        # Normalización final mínima
        normalized = {
            "artista_o_show": artista_o_show,
            "lugar": lugar,
            "fecha": fecha,
            "hora": hora,
            "descripcion": descripcion,
            "ciudad": ciudad,
            "tipo": tipo,
            "categoria": categoria,
            "gratis": bool(gratis),
        }

        # Descartes básicos
        if not any([
            normalized["artista_o_show"],
            normalized["descripcion"],
            normalized["lugar"],
        ]):
            continue

        out.append(normalized)

    return out

# =========================
# ENTRY POINT (Gen2 CloudEvent)
# =========================
def extract(data, context):
    t_total_start = time.perf_counter()
    timings: Dict[str, float] = {
        "1_descarga_lectura_imagen": 0.0,
        "2_primera_pasada_vision": 0.0,
        "3_decision_fallback": 0.0,
        "4_ocr_fallback": 0.0,
        "5_extraccion_llm_post_ocr": 0.0,
        "6_normalizacion_evento": 0.0,
        "7_escritura_sidecar_gcs": 0.0,
        "8_update_consolidado": 0.0,
        "9_upsert_supabase": 0.0,
        "10_total_por_imagen": 0.0,
    }

    bucket_name = data.get("bucket")
    object_name = data.get("name")

    if not bucket_name or not object_name:
        logging.warning("Missing bucket/name in event: %s", data)
        return

    if not _is_image(object_name):
        logging.info("Skip not image: %s", object_name)
        return

    try:
        in_bucket = storage_client.bucket(bucket_name)
        blob = in_bucket.blob(object_name)
        if not blob.exists():
            logging.info("Missing object: gs://%s/%s", bucket_name, object_name)
            return

        t_read_start = time.perf_counter()
        image_bytes = blob.download_as_bytes()
        t_read_end = time.perf_counter()
        timings["1_descarga_lectura_imagen"] = _ms(t_read_start, t_read_end)
        if len(image_bytes) < 5000:
            logging.info("Skip too small: %s", object_name)
            return

        # Convertir si hace falta
        converted = False
        try:
            image_bytes, _ct, converted = _convert_to_jpeg_if_needed(object_name, image_bytes)
        except Exception as conv_err:
            logging.warning("convert failed for %s: %s (trying raw)", object_name, conv_err)

        # Metadata de Instagram
        ig_meta: Dict[str, Any] = {}
        meta_blob_name = re.sub(r"\.[^.]+$", ".meta.json", object_name)
        try:
            meta_blob = in_bucket.blob(meta_blob_name)
            if meta_blob.exists():
                ig_meta = json.loads(meta_blob.download_as_text(encoding="utf-8"))
                logging.info("Found IG metadata: %s (%d keys)", meta_blob_name, len(ig_meta))
        except Exception as meta_err:
            logging.warning("Error reading IG sidecar %s: %s", meta_blob_name, meta_err)

        # 1) Primera pasada visual sobre la imagen
        t_vision_start = time.perf_counter()
        events_final, vision_debug = _vision_extract_events(image_bytes=image_bytes, ig_meta=ig_meta)
        t_vision_end = time.perf_counter()
        timings["2_primera_pasada_vision"] = _ms(t_vision_start, t_vision_end)
        events_final = _enrich_events_with_metadata_context(events_final, ig_meta)
        timings["6_normalizacion_evento"] += float(vision_debug.get("normalize_ms", 0.0) or 0.0)

        full_text = ""
        rows: List[Dict[str, Any]] = []
        blocks: List[List[Dict[str, Any]]] = []
        header = ""
        groq_debug: Dict[str, Any] = {"attempted": False, "http_status": None, "error": "not_needed", "model": GROQ_MODEL_EXTRACT}

        primary_count = len(events_final)
        primary_quality = _avg_event_completeness(events_final)
        logging.info("[EXTRACT] primary_count=%d primary_quality=%.2f", primary_count, primary_quality)

        t_fallback_decision_start = time.perf_counter()
        mini_quality = primary_quality
        should_probe_ocr = not _is_good_enough(events_final)
        needs_fallback = False
        sparse_minicpm = bool(events_final) and len(events_final) <= 2
        rows_count = 0
        date_hits = 0
        block_count = 0

        if should_probe_ocr:
            # 2) OCR solo cuando hay señales de extracción floja o potencial flyer denso.
            t_ocr_start = time.perf_counter()
            full_text, rows = _run_tesseract_ocr(image_bytes)
            t_ocr_end = time.perf_counter()
            timings["4_ocr_fallback"] = _ms(t_ocr_start, t_ocr_end)
            header = _detect_header(rows)
            blocks = _split_ocr_rows_into_blocks(rows)

            rows_count = len(rows)
            date_hits = len(_DENSE_FLYER_SIGNAL_RE.findall(full_text or ""))
            block_count = len(blocks)
            dense_flyer = rows_count >= HARD_FALLBACK_MIN_ROWS or date_hits >= HARD_FALLBACK_MIN_DATE_HITS
            low_quality = mini_quality < 2.1
            needs_fallback = (not bool(events_final)) or (dense_flyer and (sparse_minicpm or low_quality))

        t_fallback_decision_end = time.perf_counter()
        timings["3_decision_fallback"] = _ms(t_fallback_decision_start, t_fallback_decision_end)
        logging.info(
            "[EXTRACT] should_probe_ocr=%s rows_count=%d date_hits=%d block_count=%d",
            should_probe_ocr, rows_count, date_hits, block_count,
        )

        if not should_probe_ocr or not needs_fallback:
            extraction_mode = "groq_vision" if events_final else "none"
        else:
            # --- Filtro rápido: descartar imágenes que no parecen flyers ---
            if _quick_skip_non_event(full_text, ig_meta):
                if events_final:
                    # Si MiniCPM ya encontró eventos, conservarlos y evitar vaciar por quick-skip.
                    extraction_mode = "groq_vision"
                else:
                    logging.info("Quick-skip non-event: %s (OCR=%d chars, meta=%s)",
                                 object_name, len(full_text), bool(ig_meta.get("caption")))
                    events_final = []
                    extraction_mode = "quick_skip"
            else:
                t_llm_post_ocr_start = time.perf_counter()
                fallback_events, groq_debug = _groq_extract_events(
                    full_text, header, rows,
                    ig_meta=ig_meta,
                    image_bytes=None,
                    model=GROQ_MODEL_FALLBACK_TEXT,
                    response_format=_events_response_format(GROQ_FALLBACK_STRICT_JSON),
                )
                t_llm_post_ocr_end = time.perf_counter()
                timings["5_extraccion_llm_post_ocr"] = float(groq_debug.get("llm_call_ms", _ms(t_llm_post_ocr_start, t_llm_post_ocr_end)) or 0.0)
                timings["6_normalizacion_evento"] += float(groq_debug.get("normalize_ms", 0.0) or 0.0)

                events_final = _merge_events_prefer_complete(events_final or [], fallback_events or [])
                fallback_normal_count = len(fallback_events or [])
                fallback_normal_quality = _avg_event_completeness(fallback_events or [])
                merged_count = len(events_final)
                merged_quality = _avg_event_completeness(events_final)

                if _is_good_enough(events_final):
                    hard_fallback_used = False
                    hard_count = 0
                    logging.info(
                        "[EXTRACT] fallback_normal_count=%d fallback_normal_quality=%.2f",
                        fallback_normal_count,
                        fallback_normal_quality,
                    )
                    logging.info("[EXTRACT] hard_fallback_used=%s hard_fallback_count=%d", hard_fallback_used, hard_count)
                    extraction_mode = "groq_vision+fallback_text" if fallback_events else ("groq_vision" if events_final else "none")
                else:
                    logging.info(
                        "[EXTRACT] fallback_normal_count=%d fallback_normal_quality=%.2f",
                        fallback_normal_count,
                        fallback_normal_quality,
                    )

                    hard_fallback_used = _should_use_hard_fallback(
                        rows_count=rows_count,
                        date_hits=date_hits,
                        block_count=block_count,
                        events_count=merged_count,
                        avg_quality=merged_quality,
                    )
                    hard_events: List[Dict[str, Any]] = []
                    hard_already_merged = False
                    if hard_fallback_used and blocks:
                        selected_blocks = _pick_top_ocr_blocks(blocks, HARD_FALLBACK_MAX_BLOCKS)
                        for b in selected_blocks:
                            block_text = _format_rows_for_llm(b)
                            if not block_text.strip():
                                continue
                            block_rows = b
                            block_events, block_dbg = _groq_extract_events(
                                full_text=block_text,
                                header=header,
                                rows=block_rows,
                                ig_meta=ig_meta,
                                image_bytes=None,
                                model=GROQ_MODEL_FALLBACK_TEXT_HARD,
                                response_format=_events_response_format(True if GROQ_FALLBACK_STRICT_JSON else False),
                                system_prompt=_GROQ_HARD_BLOCK_SYSTEM,
                            )
                            if block_dbg.get("error") and not block_events:
                                # Retry flexible: strict false first, then no response_format.
                                block_events, block_dbg2 = _groq_extract_events(
                                    full_text=block_text,
                                    header=header,
                                    rows=block_rows,
                                    ig_meta=ig_meta,
                                    image_bytes=None,
                                    model=GROQ_MODEL_FALLBACK_TEXT_HARD,
                                    response_format=_events_response_format(False),
                                    system_prompt=_GROQ_HARD_BLOCK_SYSTEM,
                                )
                                if block_dbg2.get("error") and not block_events:
                                    block_events, _ = _groq_extract_events(
                                        full_text=block_text,
                                        header=header,
                                        rows=block_rows,
                                        ig_meta=ig_meta,
                                        image_bytes=None,
                                        model=GROQ_MODEL_FALLBACK_TEXT_HARD,
                                        response_format=None,
                                        system_prompt=_GROQ_HARD_BLOCK_SYSTEM,
                                    )
                            if block_events:
                                hard_events.extend(block_events)
                                # Early cut: si ya quedó bien, no seguir gastando hard fallback.
                                preview_merged = _merge_events_prefer_complete(events_final, hard_events)
                                if _is_good_enough(preview_merged):
                                    events_final = preview_merged
                                    hard_already_merged = True
                                    break

                        if hard_events and not hard_already_merged:
                            events_final = _merge_events_prefer_complete(events_final, hard_events)

                    hard_count = len(hard_events)
                    logging.info(
                        "[EXTRACT] hard_fallback_used=%s hard_fallback_count=%d",
                        hard_fallback_used,
                        hard_count,
                    )

                    if hard_count > 0:
                        extraction_mode = "groq_vision+fallback_text+hard"
                    elif fallback_events:
                        extraction_mode = "groq_vision+fallback_text"
                    else:
                        extraction_mode = "groq_vision" if events_final else "none"

        final_count = len(events_final)
        final_quality = _avg_event_completeness(events_final)
        logging.info("[EXTRACT] final_count=%d final_quality=%.2f extraction_mode=%s", final_count, final_quality, extraction_mode)

        result = {
            "ok": True,
            "source_bucket": bucket_name,
            "source_object": object_name,
            "converted_to_jpeg": converted,
            "text": full_text,
            "ig_metadata": ig_meta or {},
            "events": events_final,
            "events_count": len(events_final),
            "extraction_mode": extraction_mode,
            "vision_primary": vision_debug,
            "groq_fallback": groq_debug,
            "timing_ms": timings,
            "took_ms": int(_ms(t_total_start, time.perf_counter())),
        }

        out_bucket = storage_client.bucket(OUT_BUCKET)

        # Parsed result completo
        parsed_name = _parsed_out_name(object_name)
        out_bucket.blob(parsed_name).upload_from_string(_safe_json(result), content_type="application/json")

        # Sidecar de eventos (lo que lee el bot)
        sidecar = {
            "events": events_final,
            "events_count": len(events_final),
            "source_object": object_name,
            "fecha": header,
            "extraction_mode": extraction_mode,
        }
        events_name = _events_out_name(object_name)
        t_sidecar_start = time.perf_counter()
        out_bucket.blob(events_name).upload_from_string(_safe_json(sidecar), content_type="application/json")
        t_sidecar_end = time.perf_counter()
        timings["7_escritura_sidecar_gcs"] = _ms(t_sidecar_start, t_sidecar_end)

        # --- Actualizar consolidado único (todos los eventos) ---
        t_cons_start = time.perf_counter()
        consolidated_for_supabase: Optional[List[Dict[str, Any]]] = None
        try:
            cons_blob = out_bucket.blob(CONSOLIDADO_NAME)
            if cons_blob.exists():
                existing = json.loads(cons_blob.download_as_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = []
            else:
                existing = []
            # Reemplazar eventos de este source_object
            existing = [e for e in existing if e.get("_source") != object_name]
            for ev in events_final:
                ev_copy = dict(ev)
                ev_copy["_source"] = object_name
                existing.append(ev_copy)
            cons_blob.upload_from_string(_safe_json(existing), content_type="application/json")
            # El upsert a Supabase se hace contra el consolidado para espejar eventos.json.
            consolidated_for_supabase = [
                {k: v for k, v in e.items() if k != "_source"}
                for e in existing
                if isinstance(e, dict)
            ]
            logging.info("Updated %s: %d events total", CONSOLIDADO_NAME, len(existing))
        except Exception as cons_err:
            logging.warning("Failed to update consolidado: %s", cons_err)
        t_cons_end = time.perf_counter()
        timings["8_update_consolidado"] = _ms(t_cons_start, t_cons_end)

        # Persistir en backend de datos usando la misma base del consolidado eventos.json.
        source_for_store = consolidated_for_supabase if consolidated_for_supabase is not None else events_final
        t_upsert_start = time.perf_counter()
        if DATA_BACKEND == "firestore":
            store_upsert = _upsert_events_to_firestore(source_for_store)
        else:
            store_upsert = _upsert_events_to_supabase(source_for_store)
        t_upsert_end = time.perf_counter()
        timings["9_upsert_supabase"] = _ms(t_upsert_start, t_upsert_end)
        logging.info(
            "[STORE] backend=%s enabled=%s target=%s source_count=%s upserted=%s reason=%s",
            DATA_BACKEND,
            store_upsert.get("enabled"),
            store_upsert.get("collection") or store_upsert.get("table"),
            len(source_for_store),
            store_upsert.get("upserted"),
            store_upsert.get("reason"),
        )
        if SUPABASE_UPSERT_STRICT and source_for_store and not store_upsert.get("ok"):
            raise RuntimeError(f"Data store upsert failed: {store_upsert.get('reason')}")
        if SUPABASE_UPSERT_STRICT and source_for_store and int(store_upsert.get("upserted") or 0) <= 0:
            raise RuntimeError("Data store upsert affected 0 rows with non-empty events")

        t_total_end = time.perf_counter()
        timings["10_total_por_imagen"] = _ms(t_total_start, t_total_end)
        _log_timing_summary(object_name, timings)

        logging.info("Wrote parsed=%s sidecar=%s events=%d mode=%s", parsed_name, events_name, len(events_final), extraction_mode)

    except Exception:
        logging.exception("Unhandled error")
        raise
