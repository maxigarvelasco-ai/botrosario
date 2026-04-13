"""
Cloud Run service: recibe webhook de Apify, baja imágenes y sube a GCS
con metadata normalizada como sidecar .meta.json.

Deploy: gcloud run deploy flyer-ingestor ...
"""
import json
import logging
import os
import time
import uuid
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import storage

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

# -----------------------------
# Config
# -----------------------------
storage_client = None
try:
    storage_client = storage.Client()
except Exception as exc:
    logging.warning("storage client no disponible: %s", exc)

IN_BUCKET = os.environ.get("IN_BUCKET", "flyers-in")
IN_PREFIX = os.environ.get("IN_PREFIX", "incoming").rstrip("/")
APIFY_TOKEN = os.environ.get("APIFY_TOKEN") or os.environ.get("apify_token")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "image/,/*;q=0.8",
    "Referer": "https://www.instagram.com/",
}


# ─────────────────────────────────────────────
# Apify → esquema interno: ÚNICO PUNTO DE MAPEO
# ─────────────────────────────────────────────
def _normalize_ig_meta(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convierte un item crudo de Apify a nuestro esquema interno.
    Si Apify cambia nombres de campos, se actualiza SOLO esta función.

    Esquema interno que usa extract():
      caption, locationName, ownerFullName, ownerUsername,
      taggedUsers [{full_name, username}],
      coauthorProducers [{full_name, username}],
      mentions [str], alt, shortCode, postUrl, timestamp
    """
    if not isinstance(item, dict):
        return {}

    def _str(key: str) -> str:
        v = item.get(key)
        return str(v).strip() if v else ""

    def _list(key: str) -> list:
        v = item.get(key)
        return v if isinstance(v, list) else []

    # --- caption: varios nombres posibles ---
    caption = _str("caption") or _str("text") or _str("description") or ""

    # --- location ---
    location_name = _str("locationName")
    if not location_name:
        loc = item.get("location")
        if isinstance(loc, dict):
            location_name = (loc.get("name") or loc.get("city") or "").strip()
        elif isinstance(loc, str):
            location_name = loc.strip()

    # --- owner ---
    owner_full = _str("ownerFullName")
    owner_user = _str("ownerUsername")
    if not owner_full and not owner_user:
        owner = item.get("owner") or item.get("user")
        if isinstance(owner, dict):
            owner_full = (owner.get("full_name") or owner.get("fullName") or "").strip()
            owner_user = (owner.get("username") or "").strip()

    # --- tagged users ---
    raw_tagged = _list("taggedUsers") or _list("usertags")
    tagged_users = []
    tagged_by_id: Dict[str, Dict[str, str]] = {}  # id → {full_name, username}
    for t in raw_tagged:
        if isinstance(t, dict):
            fn = (t.get("full_name") or t.get("fullName") or "").strip()
            un = (t.get("username") or "").strip()
            tagged_users.append({"full_name": fn, "username": un})
            uid = str(t.get("id") or "").strip()
            if uid:
                tagged_by_id[uid] = {"full_name": fn, "username": un}
        elif isinstance(t, str):
            tagged_users.append({"full_name": "", "username": t.strip()})

    # --- coauthor producers ---
    # NOTA: Apify devuelve coauthorProducers solo con id/is_verified/profile_pic_url,
    # sin username ni full_name. Cross-referenciamos con taggedUsers por id.
    raw_coauthors = _list("coauthorProducers") or _list("collaborators")
    coauthor_producers = []
    for c in raw_coauthors:
        if isinstance(c, dict):
            fn = (c.get("full_name") or c.get("fullName") or "").strip()
            un = (c.get("username") or "").strip()
            # Si faltan datos, buscar en taggedUsers por id
            if not fn and not un:
                uid = str(c.get("id") or "").strip()
                if uid and uid in tagged_by_id:
                    fn = tagged_by_id[uid]["full_name"]
                    un = tagged_by_id[uid]["username"]
            if fn or un:  # Solo agregar si tiene datos útiles
                coauthor_producers.append({"full_name": fn, "username": un})
        elif isinstance(c, str) and c.strip():
            coauthor_producers.append({"full_name": "", "username": c.strip()})

    # --- mentions ---
    raw_mentions = _list("mentions") or _list("mentionedUsers")
    mentions = []
    for m in raw_mentions:
        if isinstance(m, str) and m.strip():
            mentions.append(m.strip().lstrip("@"))
        elif isinstance(m, dict) and m.get("username"):
            mentions.append(m["username"].strip().lstrip("@"))

    # --- otros ---
    alt = _str("alt") or _str("accessibilityCaption") or ""
    short_code = _str("shortCode") or _str("code") or ""
    post_url = _str("url") or _str("postUrl") or ""
    timestamp = _str("timestamp") or _str("taken_at") or ""
    post_id = _str("id") or ""
    likes_count = item.get("likesCount")

    meta: Dict[str, Any] = {
        "caption": caption,
        "locationName": location_name,
        "ownerFullName": owner_full,
        "ownerUsername": owner_user,
        "shortCode": short_code,
    }

    # Solo incluir listas no vacías
    if tagged_users:
        meta["taggedUsers"] = tagged_users
    if coauthor_producers:
        meta["coauthorProducers"] = coauthor_producers
    if mentions:
        meta["mentions"] = mentions
    if alt:
        meta["alt"] = alt
    if post_url:
        meta["postUrl"] = post_url
    if timestamp:
        meta["timestamp"] = timestamp
    if post_id:
        meta["postId"] = post_id
    if likes_count is not None and isinstance(likes_count, (int, float)):
        meta["likesCount"] = int(likes_count)

    return meta


# -----------------------------
# Helpers HTTP responses
# -----------------------------
def _json_response(obj: Dict[str, Any], status: int) -> Tuple[str, int, Dict[str, str]]:
    return (json.dumps(obj, ensure_ascii=False), status, {"Content-Type": "application/json"})


def _apify_error(where: str, resp: requests.Response) -> Dict[str, Any]:
    text = resp.text if resp.text else ""
    return {
        "ok": False,
        "where": where,
        "apify_status": resp.status_code,
        "apify_url": resp.url.split("?token=")[0] if "?token=" in resp.url else resp.url,
        "apify_text": text[:800] if text else None,
    }


def _safe_err(e: Exception) -> str:
    return str(e) or type(e).__name__


# -----------------------------
# Image + parsing helpers
# -----------------------------
def _sanitize_filename(name: str) -> str:
    name = (name or "flyer").strip()
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r"[^a-zA-Z0-9._ -]", "_", name)
    return (name[-120:] if len(name) > 120 else name) or "flyer"


def _download_image(url: str) -> Tuple[bytes, str]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    content_type = r.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
    return r.content, content_type


def _extract_image_urls(item: Dict[str, Any]) -> List[str]:
    if not isinstance(item, dict):
        return []

    out: List[str] = []

    # 1) images
    imgs = item.get("images")
    if isinstance(imgs, list):
        for u in imgs:
            if isinstance(u, str) and u.strip():
                out.append(u.strip())
            elif isinstance(u, dict):
                candidate = (
                    u.get("url")
                    or u.get("displayUrl")
                    or u.get("src")
                    or ""
                )
                if isinstance(candidate, str) and candidate.strip():
                    out.append(candidate.strip())

    # 2) carouselMedia
    carousel = item.get("carouselMedia")
    if isinstance(carousel, list):
        for x in carousel:
            if isinstance(x, dict):
                candidate = (
                    x.get("displayUrl")
                    or x.get("url")
                    or x.get("src")
                    or ""
                )
                if isinstance(candidate, str) and candidate.strip():
                    out.append(candidate.strip())

    # 3) childPosts
    child_posts = item.get("childPosts")
    if isinstance(child_posts, list):
        for child in child_posts:
            if not isinstance(child, dict):
                continue

            child_display = child.get("displayUrl")
            if isinstance(child_display, str) and child_display.strip():
                out.append(child_display.strip())

            child_imgs = child.get("images")
            if isinstance(child_imgs, list):
                for u in child_imgs:
                    if isinstance(u, str) and u.strip():
                        out.append(u.strip())
                    elif isinstance(u, dict):
                        candidate = (
                            u.get("url")
                            or u.get("displayUrl")
                            or u.get("src")
                            or ""
                        )
                        if isinstance(candidate, str) and candidate.strip():
                            out.append(candidate.strip())

    # 4) displayUrl fallback
    display_url = item.get("displayUrl")
    if isinstance(display_url, str) and display_url.strip():
        out.append(display_url.strip())

    # 5) dedupe
    seen = set()
    deduped: List[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    return deduped


# -----------------------------
# Payload picking helpers
# -----------------------------
def _pick_dataset_id(payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    if payload.get("datasetId"):
        return payload["datasetId"]

    resource = payload.get("resource") or {}
    if isinstance(resource, dict) and resource.get("defaultDatasetId"):
        return resource["defaultDatasetId"]

    data = payload.get("data") or {}
    if isinstance(data, dict):
        if data.get("defaultDatasetId"):
            return data["defaultDatasetId"]
        if data.get("datasetId"):
            return data["datasetId"]

    if payload.get("defaultDatasetId"):
        return payload["defaultDatasetId"]

    return None


def _pick_run_id(payload: Dict[str, Any]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    resource = payload.get("resource") or {}
    if isinstance(resource, dict) and resource.get("id"):
        return resource["id"]

    if payload.get("runId"):
        return payload["runId"]

    return None


def _dataset_id_from_run(run_id: str) -> Optional[str]:
    if not APIFY_TOKEN:
        return None

    url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    r = requests.get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    obj = r.json() or {}
    data = obj.get("data") or {}
    return data.get("defaultDatasetId")


def _get_apify_dataset_items(dataset_id: str) -> Tuple[Optional[List[Any]], Optional[requests.Response]]:
    if not APIFY_TOKEN:
        return None, None

    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}"
    resp = requests.get(url, timeout=60)
    if not resp.ok:
        return None, resp
    return resp.json(), None


# -----------------------------
# Background dataset processing
# -----------------------------
def process_dataset(dataset_id: str) -> Dict[str, Any]:
    """Descarga items del dataset de Apify, baja imágenes y las sube a GCS."""
    if storage_client is None:
        return {"ok": False, "error": "storage_unavailable"}

    try:
        items, apify_err_resp = _get_apify_dataset_items(dataset_id)
        if apify_err_resp is not None:
            logging.error("Apify error for dataset %s: %s %s", dataset_id, apify_err_resp.status_code, (apify_err_resp.text or "")[:400])
            return {"ok": False, "error": f"apify_error_{apify_err_resp.status_code}"}
        if not isinstance(items, list):
            logging.error("Apify items not a list for dataset %s", dataset_id)
            return {"ok": False, "error": "apify_items_not_list"}

        bucket = storage_client.bucket(IN_BUCKET)
        processed = 0
        skipped = 0
        failures = 0
        date_prefix = time.strftime("%Y/%m/%d")

        for item in items:
            urls = _extract_image_urls(item)
            if not urls:
                skipped += 1
                continue

            # Normalizar metadata UNA vez por item (todas las imágenes comparten la misma)
            ig_meta = _normalize_ig_meta(item)

            for u in urls:
                try:
                    raw, content_type = _download_image(u)
                    filename = _sanitize_filename(u.split("?")[0].split("/")[-1])

                    if "." not in filename:
                        if content_type == "image/webp":
                            filename += ".webp"
                        elif content_type == "image/png":
                            filename += ".png"
                        else:
                            filename += ".jpg"

                    obj_name = f"{IN_PREFIX}/{date_prefix}/{uuid.uuid4().hex}_{filename}"

                    # 1) Subir sidecar .meta.json PRIMERO (antes de la imagen)
                    #    para que cuando el trigger de GCS dispare extract(),
                    #    la metadata ya esté disponible.
                    if ig_meta:
                        meta_name = re.sub(r"\.[^.]+$", ".meta.json", obj_name)
                        try:
                            bucket.blob(meta_name).upload_from_string(
                                json.dumps(ig_meta, ensure_ascii=False, indent=2),
                                content_type="application/json",
                            )
                            logging.info("IG sidecar saved: gs://%s/%s (%d keys)", IN_BUCKET, meta_name, len(ig_meta))
                        except Exception as meta_err:
                            logging.warning("Failed to save IG sidecar %s: %s", meta_name, _safe_err(meta_err))

                    # 2) Subir imagen (esto dispara extract() vía Cloud Function trigger)
                    bucket.blob(obj_name).upload_from_string(raw, content_type=content_type)
                    processed += 1
                except Exception as e:
                    failures += 1
                    logging.warning("Failed to process image %s: %s", u[:120], _safe_err(e))

        logging.info("process_dataset done: dataset=%s processed=%d skipped=%d failures=%d", dataset_id, processed, skipped, failures)
        return {
            "ok": True,
            "datasetId": dataset_id,
            "processed": processed,
            "skipped": skipped,
            "failures_count": failures,
        }
    except Exception as e:
        logging.exception("process_dataset error for %s: %s", dataset_id, _safe_err(e))
        return {"ok": False, "error": _safe_err(e)}


# -----------------------------
# Main HTTP handler
# -----------------------------
def ingest(request):
    payload = request.get_json(silent=True) or {}

    if storage_client is None:
        return _json_response({"ok": False, "error": "storage_unavailable"}, 500)

    if not APIFY_TOKEN:
        return _json_response({"ok": False, "error": "APIFY_TOKEN_not_set"}, 500)

    dataset_id = _pick_dataset_id(payload)

    if isinstance(dataset_id, str) and "{{" in dataset_id:
        return _json_response({"ok": True, "msg": "Test detectado, ignorando llaves"}, 200)

    if not dataset_id:
        run_id = _pick_run_id(payload)
        if run_id:
            if isinstance(run_id, str) and "{{" in run_id:
                return _json_response({"ok": True, "msg": "Test detectado en runId"}, 200)

            try:
                dataset_id = _dataset_id_from_run(run_id)
            except Exception as e:
                return _json_response({"ok": False, "error": _safe_err(e)}, 500)

    if not dataset_id:
        return _json_response({"ok": False, "error": "No datasetId found in payload"}, 400)

    # Procesar sincrónicamente (Cloud Run timeout debe ser >= 300s)
    result = process_dataset(dataset_id)

    return _json_response(result, 200)


def ping(request):
    return _json_response({"ok": True, "service": "flyer-ingestor"}, 200)
