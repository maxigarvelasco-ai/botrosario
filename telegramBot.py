import ast
import json
import logging
import math
import os
import re
import unicodedata
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    import requests
except Exception:
    requests = None

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None

try:
    from google.cloud import storage
except Exception:
    storage = None

try:
    from google.cloud import firestore
except Exception:
    firestore = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ============================================================
# ENV
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", os.getenv("groq_api_key", "")).strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b").strip()
GROQ_MODEL_FALLBACK = os.getenv("GROQ_MODEL_FALLBACK", GROQ_MODEL).strip()
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

SUPABASE_DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL", os.getenv("DATABASE_URL", "")).strip()
SUPABASE_EVENTS_TABLE = os.getenv("SUPABASE_EVENTS_TABLE", "events").strip()
ALLOW_PAST_EVENTS = os.getenv("ALLOW_PAST_EVENTS", "1").strip().lower() not in ("0", "false", "no")
DATA_BACKEND = os.getenv("DATA_BACKEND", "firestore").strip().lower()
FIRESTORE_EVENTS_COLLECTION = os.getenv("FIRESTORE_EVENTS_COLLECTION", "events").strip()
FIRESTORE_USERS_COLLECTION = os.getenv("FIRESTORE_USERS_COLLECTION", "users").strip()
FIRESTORE_INTERACTIONS_COLLECTION = os.getenv("FIRESTORE_INTERACTIONS_COLLECTION", "user_interactions").strip()
FIRESTORE_CATEGORIZED_COLLECTION = os.getenv("FIRESTORE_CATEGORIZED_COLLECTION", "events_by_category").strip()

EVENTS_BUCKET = os.getenv("EVENTS_BUCKET", "flyers-out").strip()
ENABLE_GCS_STATE = os.getenv("ENABLE_GCS_STATE", "1").strip().lower() not in ("0", "false", "no")
LOCATIONS_PREFIX = os.getenv("LOCATIONS_PREFIX", "user_locations").strip().rstrip("/") + "/"
GEOCACHE_PREFIX = os.getenv("GEOCACHE_PREFIX", "geocache").strip().rstrip("/") + "/"
STATE_PREFIX = os.getenv("STATE_PREFIX", "bot_state").strip().rstrip("/") + "/"

BOT_TIMEZONE = os.getenv("BOT_TIMEZONE", "America/Argentina/Buenos_Aires").strip()
DEFAULT_USER_LAT = float(os.getenv("DEFAULT_USER_LAT", "-32.94682") or -32.94682)
DEFAULT_USER_LNG = float(os.getenv("DEFAULT_USER_LNG", "-60.63932") or -60.63932)

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else ""

storage_client = None
if storage is not None:
    try:
        storage_client = storage.Client()
    except Exception as exc:
        logging.warning("storage client no disponible: %s", exc)

firestore_client = None
if firestore is not None:
    try:
        firestore_client = firestore.Client()
    except Exception as exc:
        logging.warning("firestore client no disponible: %s", exc)


# ============================================================
# UTILS
# ============================================================

def _strip_accents(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _norm(value: str) -> str:
    return _strip_accents((value or "").lower()).strip()


def _now_local() -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    try:
        return datetime.now(ZoneInfo(BOT_TIMEZONE))
    except Exception:
        return datetime.now()


def _default_reference_location() -> Dict[str, float]:
    return {"lat": DEFAULT_USER_LAT, "lng": DEFAULT_USER_LNG}


# ============================================================
# GCS STATE
# ============================================================

def _gcs_read_json(blob_name: str) -> Any:
    if not ENABLE_GCS_STATE or storage_client is None or not EVENTS_BUCKET:
        return None
    try:
        bucket = storage_client.bucket(EVENTS_BUCKET)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        raw = blob.download_as_text(encoding="utf-8")
        return json.loads(raw)
    except Exception:
        return None


def _gcs_write_json(blob_name: str, data: Any) -> None:
    if not ENABLE_GCS_STATE or storage_client is None or not EVENTS_BUCKET:
        return
    try:
        bucket = storage_client.bucket(EVENTS_BUCKET)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")
    except Exception:
        logging.exception("No pude escribir %s", blob_name)


def _state_blob(chat_id: int) -> str:
    return f"{STATE_PREFIX}chat_{chat_id}.json"


def _load_chat_state(chat_id: int) -> Dict[str, Any]:
    data = _gcs_read_json(_state_blob(chat_id))
    return data if isinstance(data, dict) else {}


def _save_chat_state(chat_id: int, state: Dict[str, Any]) -> None:
    _gcs_write_json(_state_blob(chat_id), state)


def _get_user_location(chat_id: int) -> Optional[Dict[str, float]]:
    data = _gcs_read_json(f"{LOCATIONS_PREFIX}{chat_id}.json")
    if not isinstance(data, dict):
        return None
    try:
        return {"lat": float(data["lat"]), "lng": float(data["lng"])}
    except Exception:
        return None


def _set_user_location(chat_id: int, loc: Dict[str, float]) -> None:
    _gcs_write_json(f"{LOCATIONS_PREFIX}{chat_id}.json", {"lat": float(loc["lat"]), "lng": float(loc["lng"])})


# ============================================================
# DATABASE
# ============================================================

def _read_events_from_supabase(max_events: int = 220) -> List[Dict[str, Any]]:
    if not SUPABASE_DATABASE_URL or psycopg2 is None:
        return []
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", SUPABASE_EVENTS_TABLE):
        logging.warning("SUPABASE_EVENTS_TABLE invalida: %s", SUPABASE_EVENTS_TABLE)
        return []

    conn = None
    try:
        conn = psycopg2.connect(SUPABASE_DATABASE_URL)
        with conn.cursor() as cur:
            if ALLOW_PAST_EVENTS:
                cur.execute(
                    f"""
                    SELECT payload, categoria, fecha_text, hora, ciudad, lugar, event_date
                    FROM {SUPABASE_EVENTS_TABLE}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (max_events,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT payload, categoria, fecha_text, hora, ciudad, lugar, event_date
                    FROM {SUPABASE_EVENTS_TABLE}
                    WHERE event_date IS NULL OR event_date >= CURRENT_DATE
                    ORDER BY COALESCE(event_date, CURRENT_DATE + INTERVAL '365 days') ASC, created_at DESC
                    LIMIT %s
                    """,
                    (max_events,),
                )
            rows = cur.fetchall()

        events: List[Dict[str, Any]] = []
        for payload, categoria, fecha_text, hora, ciudad, lugar, event_date in rows:
            ev: Dict[str, Any] = {}

            if isinstance(payload, dict):
                ev = dict(payload)
            elif isinstance(payload, str):
                try:
                    parsed = json.loads(payload)
                    if isinstance(parsed, dict):
                        ev = parsed
                except Exception:
                    ev = {}

            if categoria and not ev.get("categoria"):
                ev["categoria"] = str(categoria)
            if fecha_text and not ev.get("fecha"):
                ev["fecha"] = str(fecha_text)
            if hora and not ev.get("hora"):
                ev["hora"] = str(hora)
            if ciudad and not ev.get("ciudad"):
                ev["ciudad"] = str(ciudad)
            if lugar and not ev.get("lugar"):
                ev["lugar"] = str(lugar)
            if event_date and not ev.get("event_date"):
                ev["event_date"] = str(event_date)

            titulo = (
                ev.get("titulo")
                or ev.get("name")
                or ev.get("artista_o_show")
                or ev.get("artist")
                or ev.get("show")
                or ""
            )
            titulo = str(titulo).strip()

            if not titulo:
                desc = str(ev.get("descripcion") or "").strip()
                if desc:
                    titulo = re.split(r"[\n\.,:;\-–—]", desc)[0].strip()

            if titulo:
                ev["titulo"] = titulo

            if not ev.get("resumen") and ev.get("descripcion"):
                ev["resumen"] = str(ev.get("descripcion"))

            if ev:
                events.append(ev)
        return events
    except Exception:
        logging.exception("No pude leer eventos desde Supabase")
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _read_events_from_firestore(max_events: int = 220) -> List[Dict[str, Any]]:
    if firestore_client is None or not FIRESTORE_EVENTS_COLLECTION:
        return []

    try:
        query = firestore_client.collection(FIRESTORE_EVENTS_COLLECTION).order_by(
            "updated_at",
            direction=firestore.Query.DESCENDING,
        ).limit(max_events)
        docs = query.stream()

        events: List[Dict[str, Any]] = []
        for doc in docs:
            data = doc.to_dict() or {}
            if not isinstance(data, dict):
                continue

            ev: Dict[str, Any] = {}
            payload = data.get("payload")
            if isinstance(payload, dict):
                ev = dict(payload)
            else:
                ev = dict(data)

            if data.get("fecha_text") and not ev.get("fecha"):
                ev["fecha"] = str(data.get("fecha_text"))
            if data.get("event_date") and not ev.get("event_date"):
                ev["event_date"] = str(data.get("event_date"))

            if ev:
                events.append(ev)
        return events
    except Exception:
        logging.exception("No pude leer eventos desde Firestore")
        return []


def _category_doc_id(category: str) -> str:
    normalized = _strip_accents(_norm(category or ""))
    normalized = normalized.replace("+", " plus ")
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = normalized.strip("_")
    return normalized or "sin_categoria"


def _read_events_from_firestore_by_categories(categories: List[str], max_events: int = 220) -> List[Dict[str, Any]]:
    if firestore_client is None or not FIRESTORE_CATEGORIZED_COLLECTION:
        return []

    clean_categories = [
        _norm(str(cat or ""))
        for cat in (categories or [])
        if _norm(str(cat or ""))
    ]
    clean_categories = list(dict.fromkeys(clean_categories))
    if not clean_categories:
        return []

    try:
        limit_per_category = max(12, math.ceil(max_events / max(1, len(clean_categories))))
        seen_hashes = set()
        events: List[Dict[str, Any]] = []

        for category in clean_categories:
            doc_id = _category_doc_id(category)
            query = firestore_client.collection(FIRESTORE_CATEGORIZED_COLLECTION).document(doc_id).collection("items").order_by(
                "updated_at",
                direction=firestore.Query.DESCENDING,
            ).limit(limit_per_category)

            for doc in query.stream():
                data = doc.to_dict() or {}
                if not isinstance(data, dict):
                    continue

                event_hash = str(data.get("event_hash") or doc.id)
                if event_hash in seen_hashes:
                    continue
                seen_hashes.add(event_hash)

                events.append(data)
                if len(events) >= max_events:
                    return events

        return events
    except Exception:
        logging.exception("No pude leer eventos categorizados desde Firestore")
        return []


def _candidate_categories_for_query(constraints: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(constraints, dict):
        return []

    include = [
        str(cat)
        for cat in (constraints.get("include_categories") or [])
        if str(cat) in CATEGORY_TERMS
    ]
    if include:
        return list(dict.fromkeys(include))

    q = _norm(str(constraints.get("raw_text") or ""))
    inferred: List[str] = []
    for cat, terms in CATEGORY_TERMS.items():
        if any(term in q for term in terms):
            inferred.append(cat)

    mood = str(constraints.get("mood") or "").strip().lower()
    if mood == "tranquilo":
        inferred.extend(["teatro", "museo", "cine", "feria"])
    elif mood == "movido":
        inferred.extend(["musica"])
    elif mood == "familiar":
        inferred.extend(["museo", "feria", "cine"])

    return list(dict.fromkeys(inferred))


def _ensure_user_row(chat_id: int, nombre: str = "") -> None:
    if DATA_BACKEND == "firestore" and firestore_client is not None and FIRESTORE_USERS_COLLECTION:
        try:
            user_ref = firestore_client.collection(FIRESTORE_USERS_COLLECTION).document(str(chat_id))
            snap = user_ref.get()
            current = snap.to_dict() if snap.exists and isinstance(snap.to_dict(), dict) else {}
            payload: Dict[str, Any] = {
                "chat_id": chat_id,
                "nombre": (nombre or str(current.get("nombre") or "")).strip(),
                "last_seen": datetime.utcnow(),
                "interaction_count": int(current.get("interaction_count") or 0),
            }
            if not snap.exists:
                payload["created_at"] = datetime.utcnow()
            user_ref.set(payload, merge=True)
        except Exception:
            logging.exception("No pude asegurar users en Firestore")
        return

    if not SUPABASE_DATABASE_URL or psycopg2 is None:
        return
    conn = None
    try:
        conn = psycopg2.connect(SUPABASE_DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (chat_id, nombre, last_seen)
                VALUES (%s, %s, now())
                ON CONFLICT (chat_id)
                DO UPDATE SET last_seen = now(), nombre = COALESCE(NULLIF(EXCLUDED.nombre, ''), users.nombre)
                """,
                (chat_id, nombre or ""),
            )
            conn.commit()
    except Exception:
        logging.exception("No pude asegurar users")
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _save_interaction(chat_id: int, user_text: str, bot_response: str, matched_events: List[str], detected_interests: List[str]) -> None:
    if DATA_BACKEND == "firestore" and firestore_client is not None and FIRESTORE_INTERACTIONS_COLLECTION:
        try:
            firestore_client.collection(FIRESTORE_INTERACTIONS_COLLECTION).add(
                {
                    "chat_id": chat_id,
                    "user_text": user_text or "",
                    "bot_response": bot_response or "",
                    "detected_interests": detected_interests or [],
                    "matched_events": matched_events or [],
                    "created_at": datetime.utcnow(),
                }
            )

            if FIRESTORE_USERS_COLLECTION:
                user_ref = firestore_client.collection(FIRESTORE_USERS_COLLECTION).document(str(chat_id))
                snap = user_ref.get()
                current = snap.to_dict() if snap.exists and isinstance(snap.to_dict(), dict) else {}
                user_ref.set(
                    {
                        "chat_id": chat_id,
                        "interaction_count": int(current.get("interaction_count") or 0) + 1,
                        "last_seen": datetime.utcnow(),
                    },
                    merge=True,
                )
        except Exception:
            logging.exception("No pude guardar user_interactions en Firestore")
        return

    if not SUPABASE_DATABASE_URL or psycopg2 is None:
        return
    conn = None
    try:
        conn = psycopg2.connect(SUPABASE_DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_interactions (chat_id, user_text, bot_response, detected_interests, matched_events)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (chat_id, user_text or "", bot_response or "", detected_interests or [], matched_events or []),
            )
            cur.execute(
                """
                UPDATE users
                SET interaction_count = interaction_count + 1,
                    last_seen = now()
                WHERE chat_id = %s
                """,
                (chat_id,),
            )
            conn.commit()
    except Exception:
        logging.exception("No pude guardar user_interactions")
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


# ============================================================
# EVENT NORMALIZATION
# ============================================================

NOISE_TERMS = [
    "planaxia",
    "una galaxia de planes",
    "agenda semanal",
    "sorteo",
    "promo",
    "promocion",
    "promociones",
    "listado de eventos",
]

CATEGORY_TERMS = {
    "musica": ["dj", "dj set", "live set", "en vivo", "tributo", "recital", "concierto", "banda", "acustico", "show musical"],
    "teatro": ["teatro", "obra", "teatral", "drama", "comedia"],
    "museo": ["museo", "muestra", "instalacion", "exposicion", "galeria"],
    "feria": ["feria", "mercado", "emprendedores", "artesanos"],
    "circo": ["circo", "circense", "acrobacia", "clown", "variete"],
    "cine": ["cine", "pelicula", "film", "proyeccion"],
}

MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


def _event_blob(ev: Dict[str, Any]) -> str:
    return _norm(" ".join([
        str(ev.get("titulo") or ev.get("name") or ev.get("artista_o_show") or ""),
        str(ev.get("descripcion") or ""),
        str(ev.get("resumen") or ""),
        str(ev.get("categoria") or ""),
        str(ev.get("tipo") or ""),
        str(ev.get("lugar") or ""),
        str(ev.get("ciudad") or ""),
    ]))


def _is_noise_event(ev: Dict[str, Any]) -> bool:
    blob = _event_blob(ev)
    return any(term in blob for term in NOISE_TERMS)


def _load_all_events(constraints: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    if DATA_BACKEND == "firestore":
        requested_categories = _candidate_categories_for_query(constraints)
        events = []
        if requested_categories:
            events = _read_events_from_firestore_by_categories(requested_categories, max_events=260)

        if not events:
            events = _read_events_from_firestore(max_events=260)
        if not events:
            events = _read_events_from_supabase(max_events=260)
    else:
        events = _read_events_from_supabase(max_events=260)
    return [ev for ev in events if not _is_noise_event(ev)]


def _title_for_reply(ev: Dict[str, Any]) -> str:
    title = (
        ev.get("titulo")
        or ev.get("name")
        or ev.get("artista_o_show")
        or ev.get("artist")
        or ev.get("show")
        or ""
    )
    title = str(title).strip()
    if title:
        return title

    desc = str(ev.get("descripcion") or "").strip()
    if desc:
        return re.split(r"[\n\.,:;\-–—]", desc)[0].strip()[:80]

    lugar = str(ev.get("lugar") or "").strip()
    if lugar:
        return f"Evento en {lugar}"

    return "Evento sin titulo"


def _event_date_text(ev: Dict[str, Any]) -> str:
    if ev.get("event_date"):
        return str(ev.get("event_date"))
    return str(ev.get("fecha") or "")


def _parse_event_date(ev: Dict[str, Any], now_local: Optional[datetime] = None) -> Optional[datetime]:
    now_local = now_local or _now_local()
    text = _norm(" ".join([
        str(ev.get("event_date") or ""),
        str(ev.get("fecha") or ""),
        str(ev.get("descripcion") or ""),
        str(ev.get("titulo") or ""),
    ]))
    if not text:
        return None

    m = re.search(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", text)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass

    m = re.search(r"\b(\d{1,2})\s+de\s+([a-zñ]+)(?:\s+de\s+(\d{4}))?\b", text)
    if m:
        month = MONTHS.get(m.group(2))
        if month:
            year = int(m.group(3)) if m.group(3) else now_local.year
            try:
                return datetime(year, month, int(m.group(1)))
            except Exception:
                pass

    m = re.search(r"\b(?:lunes|martes|miercoles|jueves|viernes|sabado|domingo)\s+(\d{1,2})\b", text)
    if m:
        day = int(m.group(1))
        for delta_month in range(-1, 4):
            month = ((now_local.month - 1 + delta_month) % 12) + 1
            year = now_local.year + ((now_local.month - 1 + delta_month) // 12)
            try:
                dt = datetime(year, month, day)
                if abs((dt.date() - now_local.date()).days) <= 45:
                    return dt
            except Exception:
                continue
    return None


def _event_hour_bucket(ev: Dict[str, Any]) -> str:
    raw = _norm(" ".join([str(ev.get("hora") or ""), str(ev.get("descripcion") or "")]))
    m = re.search(r"\b(\d{1,2})(?::|\.)(\d{2})\b", raw)
    if m:
        hour = int(m.group(1))
    else:
        m = re.search(r"\b(\d{1,2})\s*(?:hs|h)\b", raw)
        if not m:
            return "unknown"
        hour = int(m.group(1))
    if 13 <= hour < 20:
        return "afternoon"
    if hour >= 20 or hour <= 2:
        return "night"
    return "other"


def _event_is_future(ev: Dict[str, Any], now_local: datetime) -> bool:
    if ALLOW_PAST_EVENTS:
        return True
    dt = _parse_event_date(ev, now_local)
    return dt is None or dt.date() >= now_local.date()


def _derive_event_category(ev: Dict[str, Any]) -> str:
    blob = _event_blob(ev)
    scores: List[Tuple[str, int]] = []
    for cat, terms in CATEGORY_TERMS.items():
        s = sum(1 for t in terms if t in blob)
        if s > 0:
            scores.append((cat, s))
    if not scores:
        return "evento"
    scores.sort(key=lambda x: x[1], reverse=True)
    if len(scores) > 1 and scores[0][1] == scores[1][1]:
        return "evento"
    return scores[0][0]


def _matches_excluded_categories(ev: Dict[str, Any], excluded: List[str]) -> bool:
    blob = _event_blob(ev)
    for cat in excluded:
        terms = CATEGORY_TERMS.get(cat) or []
        if any(term in blob for term in terms):
            return True
    return False


def _energy_signal(ev: Dict[str, Any]) -> str:
    blob = _event_blob(ev)
    if any(x in blob for x in ["dj", "dj set", "live set", "boliche", "after", "dance", "en vivo", "tributo"]):
        return "high"
    if any(x in blob for x in ["museo", "muestra", "instalacion", "exposicion", "feria", "mercado", "charla"]):
        return "low"
    return "mid"


def _dedup_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for ev in events:
        key = (
            _norm(_title_for_reply(ev)),
            _norm(str(ev.get("fecha") or ev.get("event_date") or "")),
            _norm(str(ev.get("hora") or "")),
            _norm(str(ev.get("lugar") or "")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(ev)
    return out


# ============================================================
# GEOCODING
# ============================================================

_geocode_cache_mem: Dict[str, Optional[Tuple[float, float]]] = {}


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def _geocode_place(lugar: str, ciudad: str) -> Optional[Tuple[float, float]]:
    if not GOOGLE_MAPS_API_KEY or requests is None:
        return None
    place = (lugar or "").strip()
    city = (ciudad or "").strip()
    if len(place) < 3 or len(city) < 3:
        return None

    key = _norm(f"{place}|{city}")
    if key in _geocode_cache_mem:
        return _geocode_cache_mem[key]

    cached = _gcs_read_json(f"{GEOCACHE_PREFIX}{key}.json")
    if isinstance(cached, dict) and isinstance(cached.get("lat"), (int, float)) and isinstance(cached.get("lng"), (int, float)):
        out = (float(cached["lat"]), float(cached["lng"]))
        _geocode_cache_mem[key] = out
        return out

    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": f"{place}, {city}", "key": GOOGLE_MAPS_API_KEY},
            timeout=4,
        )
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        if results:
            loc = ((results[0].get("geometry") or {}).get("location") or {})
            lat = loc.get("lat")
            lng = loc.get("lng")
            if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
                out = (float(lat), float(lng))
                _geocode_cache_mem[key] = out
                _gcs_write_json(f"{GEOCACHE_PREFIX}{key}.json", {"lat": out[0], "lng": out[1]})
                return out
    except Exception:
        return None

    _geocode_cache_mem[key] = None
    return None


def _resolve_user_location_from_text(text: str) -> Optional[Dict[str, float]]:
    if not GOOGLE_MAPS_API_KEY or requests is None:
        return None
    query = (text or "").strip()
    if not query:
        return None
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": query, "key": GOOGLE_MAPS_API_KEY},
            timeout=4,
        )
        r.raise_for_status()
        results = (r.json().get("results") or [])
        if not results:
            return None
        loc = ((results[0].get("geometry") or {}).get("location") or {})
        lat = loc.get("lat")
        lng = loc.get("lng")
        if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
            return {"lat": float(lat), "lng": float(lng)}
    except Exception:
        return None
    return None


# ============================================================
# LLM
# ============================================================


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if m:
        try:
            parsed = json.loads(m.group(0))
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            pass
    try:
        parsed = ast.literal_eval(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _llm_json_request(messages: List[Dict[str, str]], max_tokens: int = 220, timeout: int = 10) -> Dict[str, Any]:
    if not GROQ_API_KEY or requests is None:
        raise RuntimeError("groq_unavailable")

    last_error = None
    for model in [GROQ_MODEL, GROQ_MODEL_FALLBACK]:
        if not model:
            continue
        try:
            body = {
                "model": model,
                "messages": messages,
                "temperature": 0.0,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
            }
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json=body,
                timeout=timeout,
            )
            if r.status_code >= 400:
                try:
                    detail = r.json()
                except Exception:
                    detail = r.text
                raise RuntimeError(f"groq_{r.status_code}: {detail}")
            content = ((r.json().get("choices") or [{}])[0].get("message") or {}).get("content", "")
            parsed = _extract_json_object(str(content))
            if not isinstance(parsed, dict):
                raise RuntimeError("invalid_llm_json")
            return parsed
        except Exception as exc:
            last_error = exc
            logging.warning("Groq fallo con %s: %s", model, exc)
    raise RuntimeError(str(last_error or "groq_failed"))


def _empty_constraints(text: str) -> Dict[str, Any]:
    return {
        "raw_text": text,
        "date_scope": "none",
        "time_scope": "none",
        "nearby": False,
        "mood": None,
        "style_hint": "none",
        "include_categories": [],
        "exclude_categories": [],
        "split_plan": False,
        "no_mix_days": False,
    }


def _deterministic_constraints(text: str) -> Dict[str, Any]:
    q = _norm(text)
    out = _empty_constraints(text)

    if "esta noche" in q or "hoy a la noche" in q:
        out["date_scope"] = "tonight"
    elif "manana" in q and "pasado manana" not in q:
        out["date_scope"] = "tomorrow"
    elif "hoy" in q:
        out["date_scope"] = "today"

    if any(x in q for x in ["a la tarde", "por la tarde", "tarde"]):
        out["time_scope"] = "afternoon"
    elif any(x in q for x in ["a la noche", "por la noche", "noche"]):
        out["time_scope"] = "night"

    out["nearby"] = any(x in q for x in ["cerca", "cerca mio", "cerca mío", "a mano", "caminable"])
    out["split_plan"] = any(x in q for x in ["despues", "después", "seguir con", "arrancar con"])
    out["no_mix_days"] = any(x in q for x in ["sin mezclar", "mismo dia", "mismo día", "otros dias", "otros días"])

    if any(x in q for x in ["movido", "bien arriba", "ambiente", "mucha gente"]):
        out["mood"] = "movido"
    elif any(x in q for x in ["tranquilo", "calmado", "calmo", "agradable", "charlar", "conversar"]):
        out["mood"] = "tranquilo"

    if any(x in q for x in ["no careta", "sin careta"]):
        out["style_hint"] = "no_careta"
    elif any(x in q for x in ["elegante", "formal", "cuidado", "premium"]):
        out["style_hint"] = "careta"

    excluded = []
    if any(x in q for x in ["no recital", "sin recital", "sin musica", "no musica", "sin musica en vivo"]):
        excluded.append("musica")
    if any(x in q for x in ["no teatro", "sin teatro", "no obra", "no teatro clasico"]):
        excluded.append("teatro")
    out["exclude_categories"] = list(dict.fromkeys(excluded))
    return out


def parse_query_with_llm(text: str, now_local: datetime) -> Dict[str, Any]:
    fallback = _deterministic_constraints(text)
    if not GROQ_API_KEY or requests is None:
        fallback["_llm_error"] = "groq_unavailable"
        return fallback

    prompt = {
        "task": "Parse user intent for a cultural events recommendation bot in Argentine Spanish.",
        "query": text,
        "now_local": now_local.strftime("%Y-%m-%d %H:%M"),
        "rules": [
            "Return one valid JSON object only.",
            "Prioritize intent over literal keywords.",
            "Treat explicit exclusions as hard constraints.",
            "Use split_plan=true only if the user clearly wants two stages or two plans.",
        ],
        "output_keys": ["date_scope", "time_scope", "nearby", "mood", "style_hint", "include_categories", "exclude_categories", "split_plan", "no_mix_days"],
    }
    try:
        parsed = _llm_json_request([
            {"role": "system", "content": "You are a strict intent parser. Return valid JSON only."},
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=True)},
        ], max_tokens=220)

        out = dict(fallback)
        if parsed.get("date_scope") in {"none", "today", "tomorrow", "tonight"}:
            out["date_scope"] = parsed["date_scope"]
        if parsed.get("time_scope") in {"none", "afternoon", "night"}:
            out["time_scope"] = parsed["time_scope"]
        if isinstance(parsed.get("nearby"), bool):
            out["nearby"] = parsed["nearby"]
        if parsed.get("mood") in {None, "movido", "tranquilo", "amigos", "cita", "adultos", "familiar"}:
            out["mood"] = parsed.get("mood")
        if parsed.get("style_hint") in {"none", "no_careta", "careta"}:
            out["style_hint"] = parsed["style_hint"]
        if isinstance(parsed.get("split_plan"), bool):
            out["split_plan"] = parsed["split_plan"]
        if isinstance(parsed.get("no_mix_days"), bool):
            out["no_mix_days"] = parsed["no_mix_days"]
        if isinstance(parsed.get("include_categories"), list):
            out["include_categories"] = [str(x) for x in parsed["include_categories"] if str(x) in CATEGORY_TERMS]
        if isinstance(parsed.get("exclude_categories"), list):
            out["exclude_categories"] = [str(x) for x in parsed["exclude_categories"] if str(x) in CATEGORY_TERMS]
        out["include_categories"] = [x for x in list(dict.fromkeys(out["include_categories"])) if x not in out["exclude_categories"]]
        out["exclude_categories"] = list(dict.fromkeys(out["exclude_categories"]))
        return out
    except Exception as exc:
        fallback["_llm_error"] = str(exc)
        return fallback


def _is_followup_query(text: str) -> bool:
    q = _norm(text)
    shortish = len(q.split()) <= 8
    cues = ["y ", "pero", "entonces", "bueno", "dale", "y para", "y en", "y manana", "y mañana"]
    return shortish and any(q.startswith(c) or c in q for c in cues)


def _llm_resolve_followup_query(current_query: str, last_query: str, last_constraints: Dict[str, Any]) -> str:
    if not GROQ_API_KEY or requests is None:
        return current_query
    prompt = {
        "task": "Rewrite current query as a standalone query using the previous query if needed.",
        "current_user_query": current_query,
        "previous_user_query": last_query,
        "previous_constraints": last_constraints,
        "output": {"use_previous_context": True, "standalone_query": "string"},
    }
    try:
        parsed = _llm_json_request([
            {"role": "system", "content": "You resolve ambiguous follow-ups for an events assistant. Return valid JSON only."},
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=True)},
        ], max_tokens=180)
        if parsed.get("use_previous_context") and isinstance(parsed.get("standalone_query"), str) and parsed["standalone_query"].strip():
            return parsed["standalone_query"].strip()
    except Exception:
        pass
    return current_query


def _parse_query_with_context(text: str, chat_id: Optional[int], now_local: datetime) -> Dict[str, Any]:
    if not chat_id or not _is_followup_query(text):
        return parse_query_with_llm(text, now_local)
    st = _load_chat_state(chat_id)
    last_user = str(st.get("last_user") or "")
    last_constraints = st.get("last_constraints") or {}
    if not last_user:
        return parse_query_with_llm(text, now_local)
    standalone = _llm_resolve_followup_query(text, last_user, last_constraints if isinstance(last_constraints, dict) else {})
    out = parse_query_with_llm(standalone, now_local)
    out["raw_text"] = standalone
    return out


# ============================================================
# FILTERING + RANKING
# ============================================================


def _filter_candidates(events: List[Dict[str, Any]], constraints: Dict[str, Any], now_local: datetime, user_loc: Optional[Dict[str, float]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ev in events:
        if not _event_is_future(ev, now_local):
            continue

        dt = _parse_event_date(ev, now_local)
        scope = str(constraints.get("date_scope") or "none")
        if scope == "today" and (dt is None or dt.date() != now_local.date()):
            continue
        if scope == "tomorrow" and (dt is None or dt.date() != now_local.date() + timedelta(days=1)):
            continue
        if scope == "tonight":
            if dt is None or dt.date() != now_local.date() or _event_hour_bucket(ev) != "night":
                continue

        excluded = list(constraints.get("exclude_categories") or [])
        if excluded and _matches_excluded_categories(ev, excluded):
            continue

        include_categories = list(constraints.get("include_categories") or [])
        if include_categories and _derive_event_category(ev) not in include_categories:
            continue

        ev2 = dict(ev)
        if user_loc is not None:
            lat = ev.get("lat")
            lng = ev.get("lng")
            ciudad = str(ev.get("ciudad") or "").strip()
            lugar = str(ev.get("lugar") or "").strip()

            native_coords_ok = False
            if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
                raw_distance = _distance_km(
                    float(user_loc["lat"]),
                    float(user_loc["lng"]),
                    float(lat),
                    float(lng),
                )

                local_hint_blob = _norm(f"{ciudad} {lugar}")
                looks_local = any(x in local_hint_blob for x in [
                    "rosario",
                    "city center rosario",
                    "jarana",
                    "beatmemo",
                    "garcia bar",
                    "garcia bar & rock",
                    "hum",
                    "lavarden",
                    "atlas",
                    "sala de las artes",
                    "castagnino",
                    "el aserradero",
                ])

                if looks_local and raw_distance > 120:
                    lat, lng = None, None
                else:
                    ev2["distance_km"] = raw_distance
                    native_coords_ok = True

            if not native_coords_ok:
                coords = _geocode_place(lugar, ciudad) if ciudad else None
                if coords is not None:
                    ev2["lat"], ev2["lng"] = coords
                    lat, lng = coords
                    ev2["distance_km"] = _distance_km(
                        float(user_loc["lat"]),
                        float(user_loc["lng"]),
                        float(lat),
                        float(lng),
                    )
        out.append(ev2)
    return out


def _single_date_fallback(events: List[Dict[str, Any]], constraints: Dict[str, Any], now_local: datetime, user_loc: Optional[Dict[str, float]]) -> Tuple[List[Dict[str, Any]], bool]:
    filtered = _filter_candidates(events, constraints, now_local, user_loc)
    if filtered:
        return filtered, False

    scope = str(constraints.get("date_scope") or "none")
    if scope not in {"today", "tomorrow", "tonight"}:
        return filtered, False

    relaxed = dict(constraints)
    relaxed["date_scope"] = "none"
    if scope != "tonight":
        relaxed["time_scope"] = constraints.get("time_scope") or "none"

    fallback = _filter_candidates(events, relaxed, now_local, user_loc)
    target = now_local.date() if scope in {"today", "tonight"} else now_local.date() + timedelta(days=1)

    narrowed = []
    for ev in fallback:
        dt = _parse_event_date(ev, now_local)
        if dt is None:
            continue
        delta = (dt.date() - target).days
        if 0 <= delta <= 2:
            narrowed.append(ev)
    return narrowed, bool(narrowed)


def _rank_events(events: List[Dict[str, Any]], constraints: Dict[str, Any], now_local: datetime) -> List[Dict[str, Any]]:
    raw_q = _norm(str(constraints.get("raw_text") or ""))
    want_high = any(x in raw_q for x in ["bien arriba", "movido", "mucha gente", "ambiente", "clima", "con gente"])
    want_low = any(x in raw_q for x in ["tranquilo", "calmo", "calmado", "charlar", "conversar", "agradable"])

    scored = []
    for ev in events:
        score = 0.0

        dist = ev.get("distance_km")
        if isinstance(dist, (int, float)):
            score += max(0.0, 8.0 - min(float(dist), 8.0))

        dt = _parse_event_date(ev, now_local)
        if dt is not None:
            score += 1.0

        bucket = _event_hour_bucket(ev)
        ts = str(constraints.get("time_scope") or "none")
        if ts == "night":
            if bucket == "night":
                score += 2.0
            elif bucket == "afternoon":
                score -= 2.0
            elif bucket == "unknown":
                score -= 0.5
        elif ts == "afternoon":
            if bucket == "afternoon":
                score += 2.0
            elif bucket == "night":
                score -= 2.0
            elif bucket == "unknown":
                score -= 0.5

        energy = _energy_signal(ev)
        if want_high and energy == "high":
            score += 2.5
        if want_high and energy == "low":
            score -= 2.5
        if want_low and energy == "low":
            score += 2.5
        if want_low and energy == "high":
            score -= 2.5

        if constraints.get("style_hint") == "careta":
            if any(x in _event_blob(ev) for x in ["city center", "premium", "tapeo", "broadway"]):
                score += 1.2
        elif constraints.get("style_hint") == "no_careta":
            if any(x in _event_blob(ev) for x in ["city center", "premium"]):
                score -= 1.2

        scored.append((score, ev))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [ev for _, ev in scored]


# ============================================================
# FINAL PICK + RENDER
# ============================================================


def _llm_pick_best_index(candidates: List[Dict[str, Any]], constraints: Dict[str, Any]) -> Tuple[int, str]:
    if not candidates:
        return 0, ""
    if not GROQ_API_KEY or requests is None:
        return 0, "Coincide mejor con lo que pediste."

    packed = []
    for idx, ev in enumerate(candidates[:8]):
        packed.append({
            "idx": idx,
            "title": _title_for_reply(ev),
            "category": _derive_event_category(ev),
            "hour_bucket": _event_hour_bucket(ev),
            "distance_km": ev.get("distance_km"),
            "summary": str(ev.get("descripcion") or ev.get("resumen") or "")[:200],
        })

    prompt = {
        "task": "Choose the best event for the user's intent.",
        "query": str(constraints.get("raw_text") or ""),
        "constraints": {
            "date_scope": constraints.get("date_scope"),
            "time_scope": constraints.get("time_scope"),
            "mood": constraints.get("mood"),
            "style_hint": constraints.get("style_hint"),
            "nearby": constraints.get("nearby"),
            "exclude_categories": constraints.get("exclude_categories") or [],
        },
        "candidates": packed,
        "rules": [
            "Prioritize semantic fit over pure distance.",
            "Do not choose a candidate that violates the requested vibe.",
            "Use distance and timing as tie-breakers.",
            "If no candidate is perfect, choose the closest reasonable approximation instead of rejecting all.",
        ],
        "output": {"pick_index": "int", "reason": "string"},
    }
    try:
        parsed = _llm_json_request([
            {"role": "system", "content": "You choose the best event. Return valid JSON only."},
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=True)},
        ], max_tokens=180)
        idx = int(parsed.get("pick_index", 0))
        if idx < 0 or idx >= len(candidates[:8]):
            idx = 0
        reason = str(parsed.get("reason") or "Coincide mejor con lo que pediste.").strip()
        return idx, reason
    except Exception:
        return 0, "Coincide mejor con lo que pediste."


def _event_line(ev: Dict[str, Any]) -> str:
    title = _title_for_reply(ev)
    fecha = str(ev.get("fecha") or ev.get("event_date") or "Sin fecha")
    hora = str(ev.get("hora") or "Sin hora")
    lugar = str(ev.get("lugar") or "Sin lugar")
    dist = ev.get("distance_km")
    dist_txt = f"\n  Distancia: {float(dist):.1f} km" if isinstance(dist, (int, float)) else ""
    return f"- {title}\n  Fecha/Hora/Lugar: {fecha} | {hora} | {lugar}{dist_txt}"


def _render_shortlist_answer(shortlist: List[Dict[str, Any]], reason: str, fallback_note: str = "") -> str:
    if not shortlist:
        return "No encontre una opcion que realmente encaje con lo que pediste.\nSi queres, ajusto por dia, horario, zona o tipo."
    lines = ["Te recomiendo estas opciones:"]
    for ev in shortlist[:5]:
        lines.append(_event_line(ev))
    lines.append("")
    lines.append(f"Me quedo con {_title_for_reply(shortlist[0])}: {reason}")
    if fallback_note:
        lines.append("")
        lines.append(fallback_note)
    return "\n".join(lines)


# ============================================================
# CORE
# ============================================================


def _wants_nearby(text: str) -> bool:
    q = _norm(text)
    return any(x in q for x in ["cerca", "cerca mio", "cerca mío", "a mano", "caminable"])


def _split_plan_queries(text: str) -> Tuple[str, str]:
    normalized = _norm(text)
    parts = re.split(r"\bdespues\b|\bdespués\b|\bluego\b", text, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return text.strip(), text.strip()


def _inherit_split_parent_constraints(parent: Dict[str, Any], child: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(child)
    for key in ["date_scope", "nearby", "no_mix_days"]:
        if out.get(key) in {None, False, "none"} and parent.get(key) not in {None, False, "none"}:
            out[key] = parent.get(key)
    parent_ex = list(parent.get("exclude_categories") or [])
    child_ex = list(out.get("exclude_categories") or [])
    out["exclude_categories"] = list(dict.fromkeys(child_ex + parent_ex))
    return out


def handle_event_query(text: str, user_loc: Optional[Dict[str, float]], now_local: datetime, chat_id: Optional[int]) -> Dict[str, Any]:
    query = text
    if chat_id is not None and _is_followup_query(text):
        st = _load_chat_state(chat_id)
        last_user = str(st.get("last_user") or "")
        last_constraints = st.get("last_constraints") or {}
        if last_user:
            query = _llm_resolve_followup_query(text, last_user, last_constraints if isinstance(last_constraints, dict) else {})

    constraints = _parse_query_with_context(query, chat_id, now_local)
    constraints["raw_text"] = query

    if constraints.get("nearby") and user_loc is None:
        return {"ask_location": True, "answer": "", "shortlist": [], "constraints": constraints}

    events = _dedup_events(_load_all_events(constraints))

    if constraints.get("split_plan"):
        q1, q2 = _split_plan_queries(query)
        c1 = _inherit_split_parent_constraints(constraints, parse_query_with_llm(q1, now_local))
        c2 = _inherit_split_parent_constraints(constraints, parse_query_with_llm(q2, now_local))
        c1["raw_text"] = q1
        c2["raw_text"] = q2

        f1, fb1 = _single_date_fallback(events, c1, now_local, user_loc)
        r1 = _rank_events(f1, c1, now_local)
        shortlist1 = r1[:5]

        common_day = None
        if constraints.get("no_mix_days") and shortlist1:
            dt = _parse_event_date(shortlist1[0], now_local)
            if dt is not None:
                common_day = dt.date()

        f2, fb2 = _single_date_fallback(events, c2, now_local, user_loc)
        if constraints.get("no_mix_days") and common_day is not None:
            f2 = [ev for ev in f2 if (_parse_event_date(ev, now_local) and _parse_event_date(ev, now_local).date() == common_day)]
        r2 = _rank_events(f2, c2, now_local)
        shortlist2 = r2[:5]

        if not shortlist1 or not shortlist2:
            return {
                "ask_location": False,
                "shortlist": [],
                "constraints": constraints,
                "answer": "No encontre dos planes que entren en un mismo dia con esos filtros.",
            }

        idx1, reason1 = _llm_pick_best_index(shortlist1, c1)
        idx2, reason2 = _llm_pick_best_index(shortlist2, c2)
        shortlist1[0], shortlist1[idx1] = shortlist1[idx1], shortlist1[0]
        shortlist2[0], shortlist2[idx2] = shortlist2[idx2], shortlist2[0]

        note1 = "No vi opciones exactas para ese tramo; te dejo la aproximacion mas cercana." if fb1 else ""
        note2 = "No vi opciones exactas para ese tramo; te dejo la aproximacion mas cercana." if fb2 else ""
        answer = f"Plan 1:\n{_render_shortlist_answer(shortlist1, reason1, note1)}\n\nPlan 2:\n{_render_shortlist_answer(shortlist2, reason2, note2)}"
        return {"ask_location": False, "answer": answer, "shortlist": shortlist1 + shortlist2, "constraints": constraints}

    filtered, used_fallback = _single_date_fallback(events, constraints, now_local, user_loc)
    ranked = _rank_events(filtered, constraints, now_local)
    shortlist = ranked[:5]
    if not shortlist:
        return {"ask_location": False, "answer": "No encontre opciones con esos filtros.\nSi queres, ajusto por dia, horario, zona o tipo.", "shortlist": [], "constraints": constraints}

    idx, reason = _llm_pick_best_index(shortlist, constraints)
    shortlist[0], shortlist[idx] = shortlist[idx], shortlist[0]
    note = "No vi opciones exactas para esta fecha; te dejo la mejor aproximacion cercana." if used_fallback else ""
    answer = _render_shortlist_answer(shortlist, reason, note)
    return {"ask_location": False, "answer": answer, "shortlist": shortlist, "constraints": constraints}


# ============================================================
# TELEGRAM
# ============================================================


def tg_send(chat_id: int, text: str) -> Tuple[bool, str]:
    if not TG_API or requests is None:
        return False, "telegram_unavailable"
    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)] or [text]
    try:
        for chunk in chunks:
            r = requests.post(f"{TG_API}/sendMessage", json={"chat_id": chat_id, "text": chunk}, timeout=10)
            r.raise_for_status()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def _extract_message(update: Dict[str, Any]) -> Tuple[Optional[int], str, Optional[Dict[str, float]], str]:
    msg = update.get("message") or update.get("edited_message") or {}
    chat = msg.get("chat") or {}
    user = msg.get("from") or {}
    chat_id = chat.get("id")
    text = msg.get("text") or ""
    nombre = str(user.get("first_name") or "").strip()
    location = msg.get("location") or {}
    user_loc = None
    try:
        if "latitude" in location and "longitude" in location:
            user_loc = {"lat": float(location["latitude"]), "lng": float(location["longitude"])}
    except Exception:
        user_loc = None
    return (chat_id if isinstance(chat_id, int) else None), str(text), user_loc, nombre


def _is_greeting_only(text: str) -> bool:
    return _norm(text) in {"hola", "buenas", "buen dia", "buenas tardes", "buenas noches"}


def telegram_webhook(request: Any):
    method = str(getattr(request, "method", "POST") or "POST").upper()
    if method == "GET":
        return ({"ok": True, "service": "telegram_webhook"}, 200)

    try:
        payload = request.get_json(silent=True) if hasattr(request, "get_json") else None
    except Exception:
        payload = None
    if not isinstance(payload, dict):
        return ({"ok": False, "error": "invalid_payload"}, 400)

    chat_id, text, incoming_loc, nombre = _extract_message(payload)
    if chat_id is None:
        return ({"ok": True, "ignored": True}, 200)

    _ensure_user_row(chat_id, nombre)
    st = _load_chat_state(chat_id)

    if incoming_loc is not None:
        _set_user_location(chat_id, incoming_loc)
        st["pending_location_request"] = False

    user_loc = incoming_loc or _get_user_location(chat_id)
    clean_text = (text or "").strip()
    now_local = _now_local()

    if not clean_text and incoming_loc is not None:
        answer = "Perfecto, guarde tu ubicacion. Ahora pedime por ejemplo: que hay cerca mio hoy?"
    elif not clean_text:
        return ({"ok": True, "ignored": True}, 200)
    elif clean_text.startswith("/ubicacion") and incoming_loc is None:
        parsed_loc = _resolve_user_location_from_text(clean_text)
        if parsed_loc is not None:
            _set_user_location(chat_id, parsed_loc)
            st["pending_location_request"] = False
            pending_q = str(st.pop("pending_nearby_query", "") or "")
            if pending_q:
                result = handle_event_query(pending_q, parsed_loc, now_local, chat_id)
                answer = "Perfecto, ya tengo tu ubicacion. Retomo tu consulta anterior:\n\n" + (result.get("answer") or "No encontre opciones por ahora.")
            else:
                answer = "Perfecto, ya guarde tu ubicacion. Ahora pedime por ejemplo: que hay cerca mio hoy?"
        else:
            st["pending_location_request"] = True
            answer = "Compartime el pin desde Telegram o escribime una direccion completa (ej: Necochea 1564, Rosario)."
    elif _is_greeting_only(clean_text):
        answer = "Hola, que te pinta hacer hoy? Si queres, te tiro algo movido, tranqui, familiar o cerca tuyo."
    elif clean_text.startswith("/todosloseventos"):
        events = [ev for ev in _load_all_events() if _event_is_future(ev, now_local)]
        events = _dedup_events(events)
        lines = [f"Eventos cargados: {len(events)}", ""]
        for idx, ev in enumerate(events[:50], start=1):
            lines.append(f"{idx}. {_title_for_reply(ev)}")
            lines.append(f"   {str(ev.get('fecha') or ev.get('event_date') or 'Sin fecha')} | {str(ev.get('hora') or 'Sin hora')} | {str(ev.get('lugar') or 'Sin lugar')}")
        answer = "\n".join(lines)
    else:
        effective_user_loc = user_loc
        used_reference = False
        if effective_user_loc is None and _wants_nearby(clean_text):
            inferred = _resolve_user_location_from_text(clean_text)
            if inferred is not None:
                effective_user_loc = inferred
                _set_user_location(chat_id, inferred)
            else:
                effective_user_loc = _default_reference_location()
                used_reference = True

        result = handle_event_query(clean_text, effective_user_loc, now_local, chat_id)
        if result.get("ask_location"):
            st["pending_location_request"] = True
            st["pending_nearby_query"] = clean_text
            answer = "Para recomendarte algo cerca, pasame tu ubicacion con /ubicacion."
        else:
            st.pop("pending_nearby_query", None)
            answer = result.get("answer") or "No encontre opciones por ahora."
            if used_reference and _wants_nearby(clean_text):
                answer += "\n\nNota: use una ubicacion de referencia aproximada para calcular cercania."

    st["last_user"] = clean_text
    st["last_assistant"] = answer
    if isinstance(locals().get("result"), dict):
        st["last_constraints"] = result.get("constraints") or {}
        shortlist = result.get("shortlist") or []
        matched_names = [_title_for_reply(ev) for ev in shortlist[:5]]
        detected = list((result.get("constraints") or {}).get("include_categories") or [])
        _save_interaction(chat_id, clean_text, answer, matched_names, detected)
    _save_chat_state(chat_id, st)

    ok, info = tg_send(chat_id, answer)
    return ({"ok": ok, "info": info}, 200)
