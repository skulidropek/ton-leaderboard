#!/usr/bin/env python3
"""
fetch_commits.py — автономный сборщик статистики с PAT (5 000 req/ч) и накоплением истории в leaderboard.json:

• Читает ton_repos.json (организации или owner/repo)
• Хранит единый кэш cache.json для incremental fetch (commits/issues)
• При первом запуске делает полный дамп всех репо
• Дальше инкрементально добавляет только новые коммиты, issue и PR
• Сохраняет список изменённых файлов в каждом коммите
• Логирует процесс по репозиториям/страницам
• Объединяет старый leaderboard.json с новыми записями и сохраняет его
• Пишет итог в leaderboard.json

Требует в секретах GitHub Actions задать PAT_TOKEN с правами public_repo.
"""

import os
import sys
import time
import json
import requests
import pathlib

from collections import defaultdict
from urllib.parse import urlparse

# === config ===
REPOS_FILE  = "ton_repos.json"
CACHE_FILE  = "cache.json"
OUTPUT_FILE = "leaderboard.json"
PER_PAGE    = 100
ORG_TTL     = 7 * 24 * 3600  # 7 дней


def safe_get(url, **kw):
    backoff = 1
    while True:
        r = requests.get(url, **kw)
        # Обработка secondary rate limit и forbidden
        if r.status_code in (429, 403):
            msg = ""
            if r.headers.get("Content-Type", "").startswith("application/json"):
                try:
                    msg = r.json().get("message", "").lower()
                except Exception:
                    pass
            if r.status_code == 403 and "secondary rate limit" in msg:
                retry = int(r.headers.get("Retry-After", backoff))
                log("warn", f"Secondary rate limit on {url}, sleeping {retry}s")
                time.sleep(retry)
                continue
            if r.status_code == 403:
                raise RuntimeError(f"403 Forbidden {url} → {msg or 'token lacks permission'}")
            # 429 too many requests
            retry = int(r.headers.get("Retry-After", backoff))
            log("warn", f"429 from {url}, sleeping {retry}s")
            time.sleep(retry)
            backoff = min(backoff * 2, 60)
            continue
        return r


def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def gh_headers() -> dict[str, str]:
    """
    Формируем заголовки для GitHub REST v3.
    Требуем наличия PAT, иначе падаем — так скрипт никогда не пойдёт анонимно.
    """
    token = os.getenv("PAT_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("PAT_TOKEN или GITHUB_TOKEN не заданы в env")
    return {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ton-leaderboard-bot/1.0",
        "Authorization": f"Bearer {token}"
    }

# === cache ===
EMPTY_CACHE = {
    "commits": [],
    "issues": [],
    "orgs": {},   # org → { "repos": [...], "ts": timestamp }
    "repos": {}   # owner/repo → { "c_since","c_page","i_since","i_page" }
}


def load_cache() -> dict:
    p = pathlib.Path(CACHE_FILE)
    if p.exists():
        try:
            data = json.load(open(p, encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception as e:
            log("warn", f"Broken {CACHE_FILE} ({e}), resetting")
    return EMPTY_CACHE.copy()


def save_cache(cache: dict):
    json.dump(cache, open(CACHE_FILE, "w", encoding="utf-8"),
              indent=2, ensure_ascii=False)

# === list organization repos ===
def org_repos_from_api(org: str) -> list[str]:
    repos = []
    page = 1
    while True:
        log("info", f"[ORG] listing {org}, page {page}")
        resp = safe_get(
            f"https://api.github.com/orgs/{org}/repos",
            headers=gh_headers(),
            params={"per_page": PER_PAGE, "page": page},
            timeout=30
        )
        if resp.status_code == 404:
            log("warn", f"[ORG] {org} not found (404)")
            break
        if not resp.ok:
            log("warn", f"[ORG] {org} list error: {resp.status_code}")
            break
        block = resp.json()
        if not block:
            break
        repos += [f"{org}/{r['name']}" for r in block]
        page += 1
        time.sleep(0.1)
    return repos


def get_repos_list(cache: dict) -> dict[str, bool]:
    if not pathlib.Path(REPOS_FILE).exists():
        log("error", f"{REPOS_FILE} not found"); sys.exit(1)
    cfg = json.load(open(REPOS_FILE, encoding="utf-8"))

    def normalize(entry: str) -> str | None:
        e = entry.strip()
        if e.startswith(("http://", "https://")):
            p = urlparse(e)
            path = p.path.lstrip("/").rstrip("/")
            return path[:-4] if path.endswith(".git") else path
        return e or None

    official   = {normalize(x) for x in cfg.get("official", [])   if normalize(x)}
    unofficial = {normalize(x) for x in cfg.get("unofficial", []) if normalize(x)}

    def expand(src: set[str]) -> set[str]:
        out = set()
        now = time.time()
        for x in src:
            if not x:
                continue
            parts = x.split("/")
            if len(parts) == 1:
                meta  = cache["orgs"].get(x, {})
                repos = meta.get("repos", [])
                ts    = meta.get("ts", 0)
                if not repos or now - ts > ORG_TTL:
                    fetched = org_repos_from_api(x)
                    cache["orgs"][x] = {"repos": fetched, "ts": now}
                    repos = fetched
                out.update(repos)
            elif len(parts) == 2:
                out.add(x)
            else:
                log("warn", f"Bad entry in {REPOS_FILE}: {x}")
        return out

    result = {r: True  for r in expand(official)}
    result.update({r: False for r in expand(unofficial)})
    return result

# === fetch commits and issues ===
def fetch_commits(repo: str, is_off: bool, st: dict, seen: set):
    owner, name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page  = st.get("c_page", 1)
    since = st.get("c_since")
    log("info", f"[{repo}] commits since={since} page={page}")

    while True:
        params = {"per_page": PER_PAGE, "page": page}
        if since:
            params["since"] = since
        resp = safe_get(
            f"https://api.github.com/repos/{repo}/commits",
            headers=gh_headers(),
            params=params,
            timeout=30
        )
        if not resp.ok:
            log("warn", f"[{repo}] commits error: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break

        for c in data:
            sha = c.get("sha")
            if not sha or sha in seen:
                continue
            det = safe_get(
                f"https://api.github.com/repos/{repo}/commits/{sha}",
                headers=gh_headers(), timeout=30
            ).json()
            files = [f["filename"] for f in det.get("files", []) if f.get("filename")]

            author = (c.get("author") and c["author"].get("login")) \
                     or c["commit"]["author"].get("name", "unknown")

            rec = {
                "sha":         sha,
                "author":      author,
                "url":         f"{base}/commit/{sha}",
                "repo":        base,
                "date":        c["commit"]["author"].get("date"),
                "files":       files,
                "is_official": is_off
            }
            seen.add(sha)
            yield author, rec

        page += 1
        time.sleep(0.1)

    st["c_page"]  = 1
    st["c_since"] = utc_now()


def fetch_items(repo: str, is_off: bool, st: dict, seen: set):
    owner, name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page  = st.get("i_page", 1)
    since = st.get("i_since")
    log("info", f"[{repo}] issues since={since} page={page}")

    while True:
        params = {"state": "all", "per_page": PER_PAGE, "page": page}
        if since:
            params["since"] = since
        resp = safe_get(
            f"https://api.github.com/repos/{repo}/issues",
            headers=gh_headers(),
            params=params,
            timeout=30
        )
        if not resp.ok:
            log("warn", f"[{repo}] issues error: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break

        for it in data:
            author = it.get("user", {}).get("login")
            if not author:
                continue
            key = f"{repo}#{
