#!/usr/bin/env python3
"""
watcher_noon_90.py — Noon Marketplace (regular noon.com) Watcher
------------------------------------------------------------------
Strict, quiet, no-external-AI watcher. Alerts ONLY when a product is
at least 90% off, via either of two independent signals:

  A) Noon's own officially declared discount is >= 90%, OR
  B) The current price has collapsed >= 90% vs this SKU's own
     historical median price (needs >= MIN_OBSERVATIONS prior
     readings before this signal is trusted).

No external AI, no peer/category comparison — just the two rules
above, exactly as requested. This keeps it fast, free, and immune to
any third-party API outage or rate limit.

Endpoint note: this uses the REAL endpoint captured from a live
session:
    https://www.noon.com/_vs/nc/mp-customer-catalog-api/api/v3/u/search
NOT the one that was guessed in the original spec
(.../_svc/catalog/api/v3/mp/sa-ar/search), because only the former
was actually confirmed working from a captured request. Override via
the API_ENDPOINT env var if you have a confirmed reason to use a
different path.

Honesty note: the exact response JSON schema for this endpoint was
not confirmed (no sample response was available while writing this),
so product/price extraction below uses a flexible recursive scan for
common field-name variants rather than one fixed schema. If it finds
nothing on first run, send a sample response and this can be tuned
precisely in minutes.
"""

import os
import re
import sys
import json
import time
import random
import logging
import statistics
from urllib.parse import quote

import requests

# ----------------------------- CONFIG ----------------------------- #

API_ENDPOINT = os.environ.get(
    "API_ENDPOINT",
    "https://www.noon.com/_vs/nc/mp-customer-catalog-api/api/v3/u/search",
)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Real headers captured from a live session on noon.com (regular marketplace,
# not Minutes). Override any via env vars if they rotate/expire.
X_AB_TEST_DEFAULT = (
    "3001,3491,3502,3811,2001,2741,3311,3441,4372,1802,4190,4401,1531,2222,"
    "3142,3701,3852,1832,2101,2161,4032,4081,4203,2733,2832,3391,3480,2501,"
    "3272,4093,4151,4292,4532,2462,2631,2881,4182,4502,2451,2771,3691,3431,"
    "2341,4250,3191,1750,2821,2900,3781,3900,4560,1841,2401,1771,2541,3351,"
    "4341,2201,2531,3162,3721,2212,2311,3451,4441,1891,2261,2921,3561,4581,"
    "3573,4620,1471,3952,4481,3321,3471,3771,1581,4470,1915,3732,3861,4632,"
    "1981,2304,1931,3031,3592,3792,4511,1272,2841,2941,3630,1162,2071,3621,"
    "4131,2271,2802,4230,1881,2690,4361,1960,2762,3150,3920,4591,4600,2351,2561"
)

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Cache-Control": "no-cache, max-age=0, must-revalidate, no-store",
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1"
    ),
    "x-platform": os.environ.get("X_PLATFORM", "web"),
    "x-cms": os.environ.get("X_CMS", "v2"),
    "x-content": os.environ.get("X_CONTENT", "mobile"),
    "x-visitor-id": os.environ.get("X_VISITOR_ID", "ae53dba7-0002-4b38-8c9e-b7fc88fabc35"),
    "x-locale": os.environ.get("X_LOCALE", "en-sa"),  # captured value; set to ar-sa if you prefer Arabic results
    "x-lat": os.environ.get("X_LAT", "247311382"),
    "x-lng": os.environ.get("X_LNG", "466700814"),
    "x-ab-test": os.environ.get("X_AB_TEST", X_AB_TEST_DEFAULT),
    "x-border-enabled": "true",
    "x-ecom-zonecode": os.environ.get("X_ECOM_ZONECODE", "SA-RUH-S17"),
    "x-mp-country": "sa",
    "x-rocket-enabled": "true",
    "x-rocket-zonecode": os.environ.get("X_ROCKET_ZONECODE", "W00083496A"),
}

NOON_COOKIES = os.environ.get("NOON_COOKIES", "")
LOCATION_LABEL = os.environ.get("LOCATION_LABEL", "حي طويق، الرياض")

DEFAULT_KEYWORDS = (
    "شاحن,سماعة,باوربانك,ساعة ذكية,ايفون,سامسونج,لابتوب,"
    "بروتين,مكمل غذائي,فيتامين,عطر,ساعة,كاميرا,تابلت,"
    "قهوة,شوكولاتة,العاب,مستلزمات اطفال,مكواة,مكنسة,خلاط"
)
KEYWORDS = [
    k.strip() for k in os.environ.get("KEYWORDS", DEFAULT_KEYWORDS).split(",") if k.strip()
]

# --- The ONE rule, in two flavors ---
OFFICIAL_DISCOUNT_THRESHOLD = float(os.environ.get("OFFICIAL_DISCOUNT_THRESHOLD", "90"))
BASELINE_COLLAPSE_THRESHOLD = float(os.environ.get("BASELINE_COLLAPSE_THRESHOLD", "90"))
MIN_OBSERVATIONS = int(os.environ.get("MIN_OBSERVATIONS", "3"))

MIN_CYCLE_SLEEP = int(os.environ.get("MIN_CYCLE_SLEEP", "600"))
MAX_CYCLE_SLEEP = int(os.environ.get("MAX_CYCLE_SLEEP", "900"))
MIN_REQUEST_DELAY = float(os.environ.get("MIN_REQUEST_DELAY", "2.5"))
MAX_REQUEST_DELAY = float(os.environ.get("MAX_REQUEST_DELAY", "6.0"))

SEEN_FILE = os.path.join(os.path.dirname(__file__), "seen_noon_90.json")
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "price_history_noon_90.json")

RESEND_AFTER_SECONDS = 60 * 60 * 24
HISTORY_MAX_POINTS = int(os.environ.get("HISTORY_MAX_POINTS", "40"))
HISTORY_MAX_DAYS = int(os.environ.get("HISTORY_MAX_DAYS", "45"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("noon90-watcher")

# ----------------------------- STATE HELPERS ----------------------------- #


def load_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_json_if_changed(path: str, data: dict, previous_serialized: str) -> str:
    """Writes the file ONLY if content actually changed, to keep GitHub
    commits meaningful (this is on top of git-auto-commit-action already
    skipping no-op commits — belt and suspenders)."""
    new_serialized = json.dumps(data, ensure_ascii=False, sort_keys=True)
    if new_serialized == previous_serialized:
        log.info(f"No change in {os.path.basename(path)} — skipping write.")
        return previous_serialized
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"Updated {os.path.basename(path)}.")
    except Exception as e:
        log.warning(f"Could not save {path}: {e}")
    return new_serialized


def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("No Telegram token/chat_id set — printing alert instead:")
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": False,
            },
            timeout=15,
        )
        if r.status_code != 200:
            log.warning(f"Telegram send failed: {r.status_code} {r.text}")
    except Exception as e:
        log.warning(f"Telegram send error: {e}")


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    if NOON_COOKIES:
        for pair in NOON_COOKIES.split(";"):
            if "=" in pair:
                k, v = pair.strip().split("=", 1)
                s.cookies.set(k, v)
    return s


# ----------------------------- PRICE HISTORY ----------------------------- #


def get_baseline(history: dict, sku: str):
    points = history.get(sku, [])
    if len(points) < MIN_OBSERVATIONS:
        return None, len(points)
    prices = [p for _, p in points]
    return statistics.median(prices), len(points)


def record_observation(history: dict, sku: str, price: float, now: float):
    points = history.setdefault(sku, [])
    points.append([now, price])
    cutoff = now - HISTORY_MAX_DAYS * 86400
    points[:] = [pt for pt in points if pt[0] >= cutoff]
    if len(points) > HISTORY_MAX_POINTS:
        del points[: len(points) - HISTORY_MAX_POINTS]


# ------------------------ FLEXIBLE PRODUCT EXTRACTION ------------------------ #
# Schema for this endpoint wasn't confirmed from a sample response, so this
# scans recursively for common field-name variants instead of one fixed shape.

CURRENT_PRICE_KEYS = {"saleprice", "offerprice", "sellingprice", "price", "finalprice", "specialprice"}
ORIGINAL_PRICE_KEYS = {
    "originalprice", "oldprice", "listprice", "pricebeforediscount",
    "mrp", "was", "strikedprice", "strikeoffprice", "compareatprice", "regularprice",
}
NAME_KEYS = {"name", "title", "productname", "displayname"}
URL_KEYS = {"url", "producturl", "link", "path", "seourl", "slug"}
ID_KEYS = {"sku", "id", "productid", "code"}


def _norm(k: str) -> str:
    return re.sub(r"[^a-z]", "", k.lower())


def extract_products(data):
    found = []

    def walk(node):
        if isinstance(node, dict):
            norm_map = {_norm(k): k for k in node.keys()}
            cur_key = next((norm_map[k] for k in CURRENT_PRICE_KEYS if k in norm_map), None)
            orig_key = next((norm_map[k] for k in ORIGINAL_PRICE_KEYS if k in norm_map), None)

            if cur_key and orig_key:
                try:
                    cur = float(node[cur_key])
                    orig = float(node[orig_key])
                    if orig > cur > 0:
                        name_key = next((norm_map[k] for k in NAME_KEYS if k in norm_map), None)
                        url_key = next((norm_map[k] for k in URL_KEYS if k in norm_map), None)
                        id_key = next((norm_map[k] for k in ID_KEYS if k in norm_map), None)
                        sku = str(node.get(id_key) or node.get(name_key) or id(node))
                        found.append(
                            {
                                "id": sku,
                                "title": node.get(name_key, "منتج بدون اسم") if name_key else "منتج بدون اسم",
                                "current_price": cur,
                                "official_original_price": orig,
                                "url": node.get(url_key, "") if url_key else "",
                            }
                        )
                except (TypeError, ValueError):
                    pass

            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return found


def search_keyword(session: requests.Session, keyword: str):
    params = {"q": keyword}
    try:
        resp = session.get(API_ENDPOINT, params=params, timeout=20)
    except Exception as e:
        log.warning(f"Connection error for '{keyword}': {e}")
        return []

    if resp.status_code != 200:
        log.warning(f"Unexpected status ({resp.status_code}) for '{keyword}': {resp.text[:200]}")
        return []

    try:
        data = resp.json()
    except Exception as e:
        log.warning(f"Could not parse JSON for '{keyword}': {e}")
        return []

    items = extract_products(data)
    for item in items:
        item["keyword"] = keyword
    return items


# ----------------------------- CLASSIFICATION ----------------------------- #


def classify_item(item: dict, history: dict):
    """Returns (label, extra_dict) or (None, None). Only the two 90%-rules apply."""
    sku = item["id"]
    current = item["current_price"]
    official_original = item.get("official_original_price")

    official_discount_pct = None
    if official_original and official_original > current:
        official_discount_pct = (official_original - current) / official_original * 100

    baseline, n_obs = get_baseline(history, sku)
    drop_vs_baseline_pct = None
    if baseline and baseline > 0:
        drop_vs_baseline_pct = (baseline - current) / baseline * 100

    reasons = []
    if official_discount_pct is not None and official_discount_pct >= OFFICIAL_DISCOUNT_THRESHOLD:
        reasons.append(f"خصم رسمي معلن {official_discount_pct:.0f}%")
    if drop_vs_baseline_pct is not None and drop_vs_baseline_pct >= BASELINE_COLLAPSE_THRESHOLD:
        reasons.append(f"انهيار {drop_vs_baseline_pct:.0f}% عن سعره المعتاد التاريخي")

    if not reasons:
        return None, None

    return " + ".join(reasons), {
        "official_discount": official_discount_pct,
        "baseline": baseline,
        "drop_vs_baseline": drop_vs_baseline_pct,
    }


# ----------------------------- MESSAGE FORMATTING ----------------------------- #


def format_message(label, item, extra):
    if item.get("url"):
        link = item["url"]
        if link.startswith("/"):
            link = "https://www.noon.com" + link
    else:
        link = f"https://www.noon.com/saudi-ar/search/?q={quote(item['title'])}"

    lines = [
        "🚨 *صيدة نادرة — خصم ٩٠٪ فأكثر*",
        f"📦 *{item['title']}*",
        f"💰 السعر الحالي: `{item['current_price']:.2f}` ر.س",
    ]
    if item.get("official_original_price"):
        lines.append(f"عليه: `{item['official_original_price']:.2f}` ر.س")
    if extra.get("baseline"):
        lines.append(f"📊 سعره المعتاد: `{extra['baseline']:.2f}` ر.س")
    lines.append(f"📉 السبب: *{label}*")
    lines += [
        f"🔎 كلمة البحث: {item['keyword']}",
        f"🆔 SKU: `{item['id']}`",
        f"🔗 [افتح المنتج]({link})",
    ]
    return "\n".join(lines)


# ----------------------------- MAIN CYCLE ----------------------------- #


def run_one_cycle(session: requests.Session, seen: dict, history: dict) -> int:
    now = time.time()
    alerts_sent = 0

    log.info(f"Scanning {len(KEYWORDS)} keywords this cycle (>=90% deals only)")

    for kw in KEYWORDS:
        log.info(f"🔍 Searching: {kw}")
        try:
            items = search_keyword(session, kw)
        except Exception as e:
            log.warning(f"Unexpected error scanning '{kw}': {e}")
            items = []

        for item in items:
            sku = item["id"]
            if not sku:
                continue

            label, extra = classify_item(item, history)

            if label:
                last_sent = seen.get(sku)
                if not (last_sent and (now - last_sent) < RESEND_AFTER_SECONDS):
                    send_telegram(format_message(label, item, extra))
                    seen[sku] = now
                    alerts_sent += 1

            record_observation(history, sku, item["current_price"], now)

        time.sleep(random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY))

    return alerts_sent


SINGLE_CYCLE = os.environ.get("SINGLE_CYCLE", "false").lower() == "true"


def main():
    log.info("🚀 Starting watcher_noon_90 (Noon Marketplace, >=90% deals only)")
    session = build_session()
    seen = load_json(SEEN_FILE)
    history = load_json(HISTORY_FILE)

    seen_serialized = json.dumps(seen, ensure_ascii=False, sort_keys=True)
    history_serialized = json.dumps(history, ensure_ascii=False, sort_keys=True)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ Telegram token/chat_id not set — alerts will only be logged.")

    def persist():
        nonlocal seen_serialized, history_serialized
        seen_serialized = save_json_if_changed(SEEN_FILE, seen, seen_serialized)
        history_serialized = save_json_if_changed(HISTORY_FILE, history, history_serialized)

    if SINGLE_CYCLE:
        try:
            sent = run_one_cycle(session, seen, history)
            log.info(f"✅ Single cycle finished. Alerts sent: {sent}")
        except Exception as e:
            log.error(f"❌ Unexpected error in cycle: {e}")
        finally:
            persist()
        return

    while True:
        try:
            sent = run_one_cycle(session, seen, history)
            log.info(f"✅ Cycle finished. Alerts sent: {sent}")
        except Exception as e:
            log.error(f"❌ Unexpected error in cycle: {e}")
        finally:
            persist()

        sleep_for = random.uniform(MIN_CYCLE_SLEEP, MAX_CYCLE_SLEEP)
        log.info(f"😴 Sleeping {sleep_for/60:.1f} minutes before next cycle...")
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("🛑 Stopped manually.")
        sys.exit(0)
