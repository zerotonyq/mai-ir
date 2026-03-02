import argparse
import gzip
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import quote
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup


WIKI_SITEMAP_INDEX = "https://ru.wikipedia.org/sitemap-index.xml"
HABR_SITEMAP_INDEX = "https://habr.com/sitemap.xml"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _safe_filename(value: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("_")
    if not base:
        base = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return base[:150]


def _fetch_text(
    session: requests.Session,
    url: str,
    timeout: int,
    retries: int = 2,
    headers: dict | None = None,
) -> str:
    last_exc: Exception | None = None
    for _ in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True, headers=headers)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(0.5)
    raise last_exc  # type: ignore[misc]


def _fetch_binary(
    session: requests.Session,
    url: str,
    timeout: int,
    retries: int = 2,
    headers: dict | None = None,
) -> bytes:
    last_exc: Exception | None = None
    for _ in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True, headers=headers)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(0.5)
    raise last_exc  # type: ignore[misc]


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _parse_sitemap(xml_text: str) -> tuple[str, list[str]]:
    root = ET.fromstring(xml_text)
    tag = _strip_ns(root.tag)
    locs = []
    for elem in root.iter():
        if _strip_ns(elem.tag) == "loc" and elem.text:
            locs.append(elem.text.strip())
    return tag, locs


def _iter_rss_items(xml_text: str):
    root = ET.fromstring(xml_text)

    def _findall(elem, name):
        return [child for child in elem if child.tag.endswith(name)]

    channel = None
    if root.tag.endswith("rss"):
        channels = _findall(root, "channel")
        channel = channels[0] if channels else None
    if channel is None:
        channel = root

    for item in _findall(channel, "item"):
        yield item


def _find_text(item, name: str) -> str | None:
    for child in item:
        if child.tag.endswith(name):
            return (child.text or "").strip()
    return None


def _fetch_sitemap_xml(
    session: requests.Session,
    url: str,
    timeout: int,
    headers: dict | None,
) -> str:
    def _fetch(u: str) -> str:
        if u.endswith(".gz"):
            data = _fetch_binary(session, u, timeout, headers=headers)
            return gzip.decompress(data).decode("utf-8", errors="ignore")
        return _fetch_text(session, u, timeout, headers=headers)

    try:
        return _fetch(url)
    except requests.HTTPError as exc:
        # Wikipedia sometimes exposes sitemap at sitemap.xml
        if url.endswith("sitemap-index.xml"):
            alt = url.replace("sitemap-index.xml", "sitemap.xml")
            return _fetch(alt)
        if url.endswith("sitemap.xml"):
            alt = url.replace("sitemap.xml", "sitemap-index.xml")
            return _fetch(alt)
        raise exc


def collect_urls_from_sitemaps(
    session: requests.Session,
    index_url: str,
    max_sitemaps: int,
    max_items: int,
    url_patterns: list[re.Pattern],
    headers: dict | None,
    timeout: int,
) -> list[str]:
    xml_text = _fetch_sitemap_xml(session, index_url, timeout, headers)
    tag, locs = _parse_sitemap(xml_text)
    sitemaps = []
    if tag == "sitemapindex":
        sitemaps = locs
    else:
        sitemaps = [index_url]

    urls: list[str] = []
    for idx, sm in enumerate(sitemaps, start=1):
        if idx > max_sitemaps:
            break
        print(f"[SITEMAP] {idx}/{min(len(sitemaps), max_sitemaps)} {sm}")
        try:
            sm_xml = _fetch_sitemap_xml(session, sm, timeout, headers)
        except requests.RequestException as exc:
            print(f"[SITEMAP] skip {sm} ({exc})")
            continue
        _tag, sm_locs = _parse_sitemap(sm_xml)
        for loc in sm_locs:
            if max_items and len(urls) >= max_items:
                return urls
            if any(p.search(loc) for p in url_patterns):
                urls.append(loc)

    return urls


def collect_wiki_urls_via_api(
    session: requests.Session,
    max_items: int,
    timeout: int,
    headers: dict | None,
) -> list[str]:
    urls: list[str] = []
    cont = {}
    base = "https://ru.wikipedia.org/w/api.php"
    while max_items == 0 or len(urls) < max_items:
        params = {
            "action": "query",
            "list": "allpages",
            "aplimit": 500,
            "format": "json",
        }
        params.update(cont)
        resp = session.get(base, params=params, timeout=timeout, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("query", {}).get("allpages", []):
            title = item.get("title")
            if not title:
                continue
            url = "https://ru.wikipedia.org/wiki/" + quote(title.replace(" ", "_"))
            urls.append(url)
            if max_items and len(urls) >= max_items:
                break
        cont = data.get("continue") or {}
        if not cont:
            break
    return urls


def collect_urls_from_rss(
    session: requests.Session,
    feed_url: str,
    max_items: int,
    timeout: int,
    headers: dict | None,
) -> list[str]:
    xml_text = _fetch_text(session, feed_url, timeout, headers=headers)
    urls = []
    for item in _iter_rss_items(xml_text):
        if max_items and len(urls) >= max_items:
            break
        link = _find_text(item, "link")
        if link:
            urls.append(link)
    return urls


def _extract_article(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def meta(*names):
        for name in names:
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                return tag["content"].strip()
            tag = soup.find("meta", attrs={"property": name})
            if tag and tag.get("content"):
                return tag["content"].strip()
        return None

    title = meta("og:title", "twitter:title")
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else None

    author = meta("author", "article:author")
    published = meta("article:published_time", "date", "article:published")

    # Wikipedia: main content in #mw-content-text
    container = soup.find(id="mw-content-text") or soup.find("article") or soup.find("main") or soup
    text_parts = []
    for el in container.find_all(["p", "h2", "h3"]):
        t = el.get_text(" ", strip=True)
        if t:
            text_parts.append(t)
    text = "\n".join(text_parts).strip()

    return {
        "title": title,
        "author": author,
        "published_at": published,
        "text": text,
    }


def build_from_raw(raw_source_dir: str, source_name: str) -> list[dict]:
    results = []
    if not os.path.isdir(raw_source_dir):
        print(f"[RAW] directory not found: {raw_source_dir}")
        return results

    files = sorted(
        f for f in os.listdir(raw_source_dir) if f.lower().endswith(".html")
    )
    total = len(files)
    for idx, fname in enumerate(files, start=1):
        path = os.path.join(raw_source_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                html = f.read()
        except Exception as exc:
            print(f"[RAW] skip {path} ({exc})")
            continue

        extracted = _extract_article(html)
        if not extracted.get("text"):
            continue

        results.append(
            {
                "source": source_name,
                "url": None,
                "title": extracted.get("title"),
                "author": extracted.get("author"),
                "published_at": extracted.get("published_at"),
                "text": extracted.get("text"),
                "raw_path": path,
                "fetched_at": _now_iso(),
            }
        )
        if idx % 100 == 0 or idx == total:
            print(f"[RAW] {idx}/{total} {path}")

    return results


def scrape_source(
    session: requests.Session,
    source_name: str,
    urls: list[str],
    timeout: int,
    sleep_s: float,
    raw_dir: str,
) -> list[dict]:
    results = []
    total = len(urls)
    for idx, url in enumerate(urls, start=1):
        print(f"[{source_name}] {idx}/{total} {url}")
        try:
            html = _fetch_text(session, url, timeout)
        except requests.RequestException as exc:
            print(f"[{source_name}] skip {url} ({exc})")
            continue

        filename = _safe_filename(url) + ".html"
        raw_path = os.path.join(raw_dir, source_name.lower(), filename)
        _ensure_dir(os.path.dirname(raw_path))
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(html)

        extracted = _extract_article(html)
        if not extracted.get("text"):
            continue

        results.append(
            {
                "source": source_name,
                "url": url,
                "title": extracted.get("title"),
                "author": extracted.get("author"),
                "published_at": extracted.get("published_at"),
                "text": extracted.get("text"),
                "raw_path": raw_path,
                "fetched_at": _now_iso(),
            }
        )

        if sleep_s > 0:
            time.sleep(sleep_s)

    return results


def write_jsonl(path: str, records: list[dict]) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def compute_stats(records: list[dict]) -> dict:
    raw_total = 0
    text_total = 0
    for r in records:
        raw_path = r.get("raw_path")
        if raw_path and os.path.exists(raw_path):
            raw_total += os.path.getsize(raw_path)
        text_total += len((r.get("text") or "").encode("utf-8"))

    count = len(records)
    avg_raw = raw_total / count if count else 0
    avg_text = text_total / count if count else 0
    return {
        "documents": count,
        "raw_total_bytes": raw_total,
        "text_total_bytes": text_total,
        "avg_raw_bytes": avg_raw,
        "avg_text_bytes": avg_text,
    }


def update_report(report_path: str, stats: dict) -> None:
    block = (
        "### Автоматически посчитанные статистики\n"
        f"- Дата расчёта: {stats.get('calculated_at')}\n"
        f"- Количество документов: {stats.get('documents')}\n"
        f"- Размер \"сырых\" документов (байт): {stats.get('raw_total_bytes')}\n"
        f"- Размер выделенного текста (байт): {stats.get('text_total_bytes')}\n"
        f"- Средний размер \"сырого\" документа (байт): {stats.get('avg_raw_bytes'):.2f}\n"
        f"- Средний объём текста в документе (байт): {stats.get('avg_text_bytes'):.2f}\n"
    )
    begin = "<!-- STATS:BEGIN -->"
    end = "<!-- STATS:END -->"

    if not os.path.exists(report_path):
        content = (
            "# Отчёт по корпусу документов\n\n"
            "## Статистическая информация\n\n"
            f"{begin}\n{block}{end}\n"
        )
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(content)
        return

    with open(report_path, "r", encoding="utf-8") as f:
        content = f.read()

    if begin in content and end in content:
        pre, _rest = content.split(begin, 1)
        _mid, post = _rest.split(end, 1)
        new_content = pre + begin + "\n" + block + end + post
    else:
        new_content = content.rstrip() + "\n\n" + begin + "\n" + block + end + "\n"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(new_content)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape full-text articles from Kommersant and RBC via sitemaps."
    )
    parser.add_argument("--out-dir", default="task_1/data")
    parser.add_argument("--sleep", type=float, default=0.4)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--report", default="task_1/REPORT.md")
    parser.add_argument("--from-raw", action="store_true", help="Build JSONL from raw HTML")
    parser.add_argument("--raw-source", default="kommersant", help="raw source folder name")

    parser.add_argument("--wiki-max-items", type=int, default=5000)
    parser.add_argument("--wiki-max-sitemaps", type=int, default=50)
    parser.add_argument("--wiki-sitemap", default=WIKI_SITEMAP_INDEX)

    parser.add_argument("--habr-max-items", type=int, default=5000)
    parser.add_argument("--habr-max-sitemaps", type=int, default=50)
    parser.add_argument("--habr-sitemap", default=HABR_SITEMAP_INDEX)

    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; InfoSearchBot/1.0)"})

    headers = {"User-Agent": session.headers.get("User-Agent", "")}

    wiki_patterns = [
        re.compile(r"^https?://ru\.wikipedia\.org/wiki/[^:#?]+$"),
    ]
    habr_patterns = [
        re.compile(r"^https?://habr\.com/ru/articles/\d+/"),
    ]

    raw_dir = os.path.join(args.out_dir, "raw")
    parsed_dir = os.path.join(args.out_dir, "parsed")

    try:
        wiki_urls = collect_urls_from_sitemaps(
            session,
            args.wiki_sitemap,
            args.wiki_max_sitemaps,
            args.wiki_max_items,
            wiki_patterns,
            headers,
            args.timeout,
        )
    except requests.HTTPError as exc:
        print(f"[WIKI] sitemap failed ({exc}), fallback to API")
        wiki_urls = collect_wiki_urls_via_api(
            session,
            args.wiki_max_items,
            args.timeout,
            headers,
        )
    habr_urls = collect_urls_from_sitemaps(
        session,
        args.habr_sitemap,
        args.habr_max_sitemaps,
        args.habr_max_items,
        habr_patterns,
        headers,
        args.timeout,
    )

    wiki_records = scrape_source(
        session,
        "Wikipedia",
        wiki_urls,
        args.timeout,
        args.sleep,
        raw_dir,
    )
    habr_records = scrape_source(
        session,
        "Habr",
        habr_urls,
        args.timeout,
        args.sleep,
        raw_dir,
    )

    all_records = wiki_records + habr_records

    write_jsonl(os.path.join(parsed_dir, "wikipedia.jsonl"), wiki_records)
    write_jsonl(os.path.join(parsed_dir, "habr.jsonl"), habr_records)
    write_jsonl(os.path.join(parsed_dir, "all.jsonl"), all_records)

    stats = compute_stats(all_records)
    stats["calculated_at"] = _now_iso()
    if args.report:
        update_report(args.report, stats)

    print(f"Saved {len(all_records)} articles to {parsed_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
