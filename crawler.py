import time
import yaml
import hashlib
import requests
import re
import sys
import threading
import traceback
import logging
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, urldefrag, urljoin, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient, ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError
from bs4 import BeautifulSoup


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("crawler")


def now_ts() -> int:
    return int(time.time())


def normalize_url(url: str) -> str:
    url, _ = urldefrag(url)
    p = urlsplit(url)
    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()
    path = p.path or "/"
    query = p.query
    return urlunsplit((scheme, netloc, path, query, ""))


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def is_good_wiki_title(title: str) -> bool:
    if not title:
        return False
    if ":" in title:
        return False
    return True


def html_to_clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_wiki_links(html: str, base_url: str, allowed_netlocs: set[str]) -> list[str]:
    out = set()
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith(("#", "javascript:", "mailto:")):
            continue

        full = urljoin(base_url, href)
        p = urlsplit(full)
        netloc = (p.netloc or "").lower()
        if netloc not in allowed_netlocs:
            continue

        if not p.path.startswith("/wiki/"):
            continue

        title = unquote(p.path[len("/wiki/"):])
        if not is_good_wiki_title(title):
            continue

        out.add(normalize_url(full))

    return list(out)



def ensure_indexes(docs, queue):
    docs.create_index([("url_norm", ASCENDING)], unique=True)
    docs.create_index([("source", ASCENDING), ("fetched_at", ASCENDING)])

    queue.create_index([("url_norm", ASCENDING)], unique=True)
    queue.create_index([("status", ASCENDING), ("next_fetch_at", ASCENDING)])
    queue.create_index([("locked_until", ASCENDING)])
    queue.create_index([("source", ASCENDING), ("status", ASCENDING), ("priority", ASCENDING)])


def queue_put(queue, docs, source: str, url: str, priority: int = 2):
    url_norm = normalize_url(url)

    if docs.find_one({"url_norm": url_norm}, {"_id": 1}) is not None:
        return False

    doc = {
        "url_norm": url_norm,
        "url": url,
        "source": source,
        "priority": int(priority),
        "status": "pending",
        "attempts": 0,
        "last_error": None,
        "created_at": now_ts(),
        "updated_at": now_ts(),
        "next_fetch_at": float(now_ts()),
        "locked_until": 0,
        "locked_by": None,
    }
    try:
        queue.insert_one(doc)
        return True
    except DuplicateKeyError:
        return False


def release_stale_locks(queue, lease_seconds: int):
    cutoff = now_ts()
    queue.update_many(
        {"status": "in_progress", "locked_until": {"$lt": cutoff}},
        {"$set": {"status": "pending", "locked_by": None, "updated_at": now_ts()}}
    )


def acquire_job(queue, docs, source_limits: dict, worker_id: int, lease_seconds: int):
    now = float(now_ts())

    allowed_sources = []
    for src, lim in source_limits.items():
        if docs.count_documents({"source": src}) < lim:
            allowed_sources.append(src)

    if not allowed_sources:
        return None

    job = queue.find_one_and_update(
        {
            "status": "pending",
            "source": {"$in": allowed_sources},
            "next_fetch_at": {"$lte": now},
        },
        {
            "$set": {
                "status": "in_progress",
                "locked_by": worker_id,
                "locked_until": now_ts() + lease_seconds,
                "updated_at": now_ts(),
            }
        },
        sort=[("priority", ASCENDING), ("next_fetch_at", ASCENDING), ("created_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    return job


def mark_done(queue, url_norm: str):
    queue.update_one(
        {"url_norm": url_norm},
        {"$set": {
            "status": "done",
            "updated_at": now_ts(),
            "locked_by": None,
            "locked_until": 0
        }}
    )


def mark_retry(queue, url_norm: str, attempts: int, err: str, retry_in_sec: int):
    queue.update_one(
        {"url_norm": url_norm},
        {"$set": {
            "status": "pending",
            "attempts": attempts,
            "last_error": err,
            "updated_at": now_ts(),
            "next_fetch_at": float(now_ts() + retry_in_sec),
            "locked_by": None,
            "locked_until": 0,
        }}
    )


def mark_error(queue, url_norm: str, attempts: int, err: str):
    queue.update_one(
        {"url_norm": url_norm},
        {"$set": {
            "status": "error",
            "attempts": attempts,
            "last_error": err,
            "updated_at": now_ts(),
            "locked_by": None,
            "locked_until": 0,
        }}
    )

def seed_mediawiki_category(queue, docs, source_cfg, ua: str, limit: int):
    src = source_cfg["name"]
    api_url = source_cfg["seed"]["api_url"]
    base = source_cfg["url_builder"]["base"]
    priority = source_cfg.get("priority", 2)

    categories = source_cfg["seed"].get("categories", [])
    if not categories:
        return 0

    params_base = {
        "action": "query",
        "format": "json",
        "list": "categorymembers"
    }
    params_base.update(source_cfg["seed"].get("params", {}))

    inserted = 0
    sess = requests.Session()
    sess.headers.update({"User-Agent": ua})

    for cat in categories:
        cont = {}
        params_base["cmtitle"] = cat

        while inserted < limit:
            params = dict(params_base)
            if cont:
                params.update(cont)

            r = sess.get(api_url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            members = data.get("query", {}).get("categorymembers", [])
            for m in members:
                title = m.get("title", "")
                if not is_good_wiki_title(title):
                    continue

                url = base + title.replace(" ", "_")
                if queue_put(queue, docs, src, url, priority):
                    inserted += 1
                    if inserted >= limit:
                        break

            if inserted >= limit:
                break

            if "continue" in data:
                cont = data["continue"]
            else:
                break

    log.info(f"[seed] {src}: inserted={inserted}")
    return inserted



def worker_loop(worker_id: int, cfg, docs, queue, stop_event: threading.Event, source_limits: dict, allowed_netlocs: set[str]):
    ua = cfg["logic"]["user_agent"]
    timeout = int(cfg["logic"].get("timeout_seconds", 20))
    delay = float(cfg["logic"].get("delay_seconds", 0.3))
    max_retries = int(cfg["logic"].get("max_retries", 3))
    lease_seconds = int(cfg["logic"].get("lease_seconds", 120))

    sess = requests.Session()
    sess.headers.update({"User-Agent": ua})

    empty = 0

    while not stop_event.is_set():
        try:
            if worker_id == 0:
                release_stale_locks(queue, lease_seconds)

            job = acquire_job(queue, docs, source_limits, worker_id, lease_seconds)
            if not job:
                empty += 1
                time.sleep(1.0 if empty < 5 else 3.0)
                continue
            empty = 0

            url = job["url"]
            url_norm = job["url_norm"]
            source = job["source"]
            attempts = int(job.get("attempts", 0))

            if docs.find_one({"url_norm": url_norm}, {"_id": 1}) is not None:
                mark_done(queue, url_norm)
                continue

            try:
                r = sess.get(url, timeout=timeout)

                if r.status_code == 200:
                    html = r.text
                    content_hash = sha256_text(html)
                    clean_text = html_to_clean_text(html)

                    docs.update_one(
                        {"url_norm": url_norm},
                        {"$set": {
                            "url": url,
                            "url_norm": url_norm,
                            "source": source,
                            "fetched_at": now_ts(),
                            "raw_html": html,
                            "clean_text": clean_text,
                            "content_hash": content_hash,
                            "http_status": 200,
                        }},
                        upsert=True
                    )

                    try:
                        new_links = extract_wiki_links(html, url, allowed_netlocs)
                        for link in new_links:
                            queue_put(queue, docs, source, link, priority=2)
                    except Exception:
                        pass

                    mark_done(queue, url_norm)

                elif r.status_code in (301, 302, 303, 307, 308):
                    loc = r.headers.get("Location")
                    if loc:
                        new_url = normalize_url(urljoin(url, loc))
                        queue_put(queue, docs, source, new_url, priority=int(job.get("priority", 2)))
                    mark_done(queue, url_norm)

                elif r.status_code in (429, 500, 502, 503, 504):
                    attempts += 1
                    if attempts >= max_retries:
                        mark_error(queue, url_norm, attempts, f"HTTP {r.status_code}")
                    else:
                        backoff = 30 * attempts
                        mark_retry(queue, url_norm, attempts, f"HTTP {r.status_code}", retry_in_sec=backoff)

                else:
                    attempts += 1
                    if attempts >= max_retries:
                        mark_error(queue, url_norm, attempts, f"HTTP {r.status_code}")
                    else:
                        mark_retry(queue, url_norm, attempts, f"HTTP {r.status_code}", retry_in_sec=60)

            except Exception as e:
                attempts += 1
                if attempts >= max_retries:
                    mark_error(queue, url_norm, attempts, f"EXC {type(e).__name__}: {e}")
                else:
                    mark_retry(queue, url_norm, attempts, f"EXC {type(e).__name__}: {e}", retry_in_sec=60)

            time.sleep(delay)

        except Exception as e:
            log.error(f"worker {worker_id} loop error: {e}")
            time.sleep(2.0)


def main(cfg_path: str):
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    client = MongoClient(cfg["db"]["uri"])
    db = client[cfg["db"]["name"]]

    docs_name = cfg["db"].get("docs_collection", "labs_doc")
    queue_name = cfg["db"].get("queue_collection", "labs_queue")

    docs = db[docs_name]
    queue = db[queue_name]

    ensure_indexes(docs, queue)

    threads = int(cfg["logic"].get("threads", 7))
    if threads != 7:
        threads = 7

    ua = cfg["logic"]["user_agent"]

    source_limits = {}
    max_docs_total = 0
    for k in ["wikipedia_science_target", "wikibooks_science_target"]:
        if k in cfg["logic"]:
            max_docs_total += int(cfg["logic"][k])

    if "wikipedia_science_target" in cfg["logic"]:
        source_limits["wikipedia_ru_science"] = int(cfg["logic"]["wikipedia_science_target"])
    if "wikibooks_science_target" in cfg["logic"]:
        source_limits["wikibooks_ru_science"] = int(cfg["logic"]["wikibooks_science_target"])

    allowed_netlocs = {"ru.wikipedia.org", "ru.wikibooks.org"}

    if queue.count_documents({"status": {"$in": ["pending", "in_progress"]}}) < 200:
        for s_cfg in cfg["sources"]:
            st = s_cfg.get("seed", {}).get("type")
            if st == "mediawiki_api_category":
                lim = source_limits.get(s_cfg["name"], 5000) * 2
                seed_mediawiki_category(queue, docs, s_cfg, ua, limit=lim)

    stop_event = threading.Event()

    start = time.time()
    last_report = 0.0

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker_loop, i, cfg, docs, queue, stop_event, source_limits, allowed_netlocs) for i in range(threads)]

        try:
            while True:
                time.sleep(5)

                wiki = docs.count_documents({"source": "wikipedia_ru_science"})
                books = docs.count_documents({"source": "wikibooks_ru_science"})
                total = wiki + books

                if time.time() - last_report >= 15:
                    pending = queue.count_documents({"status": "pending"})
                    inprog = queue.count_documents({"status": "in_progress"})
                    done = queue.count_documents({"status": "done"})
                    err = queue.count_documents({"status": "error"})

                    elapsed = max(1.0, time.time() - start)
                    speed = (total / elapsed) * 3600.0

                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] progress")
                    print(f"  docs: wiki={wiki} books={books} total={total}/{max_docs_total}")
                    print(f"  queue: pending={pending} in_progress={inprog} done={done} error={err}")
                    print(f"  speed: {speed:.1f} docs/hour")
                    last_report = time.time()

                if max_docs_total > 0 and total >= max_docs_total:
                    print("\nTARGET REACHED. stopping...")
                    stop_event.set()
                    break

        except KeyboardInterrupt:
            print("\nSTOP requested. saving state and exiting...")
            stop_event.set()

        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    client.close()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    try:
        main(sys.argv[1])
    except Exception as e:
        print(f"ошибка: {e}")
        traceback.print_exc()
        sys.exit(1)
