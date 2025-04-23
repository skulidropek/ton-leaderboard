#!/usr/bin/env python3
"""
fetch_commits.py — автономный сборщик статистики с PAT (5 000 req/ч) и накоплением истории:

• Читает ton_repos.json (организации или owner/repo)
• Хранит единый кэш cache.json для incremental fetch (commits/issues/orgs/repos)
• При первом запуске делает полный дамп всех репо
• Далее инкрементально добавляет только новые коммиты, issue и PR
• Возвращает только имена файлов (с расширениями), без их содержимого
• Логирует прогресс по страницам и по репозиториям
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
        if r.status_code in (429, 403):
            msg = ""
            if r.headers.get("Content-Type", "").startswith("application/json"):
                try:
                    msg = r.json().get("message", "").lower()
                except Exception:
                    pass
            # handle secondary rate limit
            if r.status_code == 403 and "secondary rate limit" in msg:
                retry = int(r.headers.get("retry-after", backoff))
                log("warn", f"Secondary rate limit on {url}, sleeping {retry}s")
                time.sleep(retry)
                backoff = min(backoff * 2, 60)
                continue
            # handle general rate limit exceeded
            if r.status_code == 403 and "rate limit exceeded" in msg:
                retry = int(r.headers.get("retry-after", backoff))
                log("warn", f"API rate limit exceeded on {url}, sleeping {retry}s")
                time.sleep(retry)
                backoff = min(backoff * 2, 60)
                continue
            # other forbidden
            if r.status_code == 403:
                raise RuntimeError(f"403 Forbidden {url} → {msg or 'token lacks permission'}")
            # too many requests
            retry = int(r.headers.get("retry-after", backoff))
            log("warn", f"429 Too Many Requests on {url}, sleeping {retry}s")
            time.sleep(retry)
            backoff = min(backoff * 2, 60)
            continue
        return r


def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def gh_headers() -> dict:
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
simple_cache_template = {
    "commits": [],
    "issues": [],
    "orgs": {},
    "repos": {}
}

def load_cache() -> dict:
    p = pathlib.Path(CACHE_FILE)
    if p.exists():
        try:
            data = json.load(open(CACHE_FILE, encoding="utf-8"))
            if isinstance(data, dict):
                # Guarantee keys
                for k, v in simple_cache_template.items():
                    data.setdefault(k, v.copy() if isinstance(v, list) else v)
                return data
        except Exception:
            log("warn", f"Broken {CACHE_FILE}, resetting cache")
    return {**simple_cache_template}


def save_cache(cache: dict):
    json.dump(cache, open(CACHE_FILE, "w", encoding="utf-8"), indent=2, ensure_ascii=False)

# === list organization repos ===

def org_repos_from_api(org: str) -> list:
    repos = []
    page = 1
    while True:
        log("info", f"[ORG] listing {org}, page {page}")
        resp = safe_get(
            f"https://api.github.com/orgs/{org}/repos",
            headers=gh_headers(), params={"per_page": PER_PAGE, "page": page}, timeout=30
        )
        if resp.status_code == 404:
            log("warn", f"[ORG] {org} not found (404)")
            break
        if not resp.ok:
            log("warn", f"[ORG] {org} list error: {resp.status_code}")
            break
        page_data = resp.json()
        if not page_data:
            break
        repos += [f"{org}/{r['name']}" for r in page_data]
        page += 1
        time.sleep(0.1)
    return repos


def get_repos_list(cache: dict) -> dict:
    cache.setdefault("orgs", {})
    cache.setdefault("repos", {})
    if not pathlib.Path(REPOS_FILE).exists():
        log("error", f"{REPOS_FILE} not found"); sys.exit(1)
    cfg = json.load(open(REPOS_FILE, encoding="utf-8"))

    def normalize(u: str):
        u = u.strip()
        if u.startswith(("http://", "https://")):
            p = urlparse(u)
            path = p.path.lstrip("/").rstrip("/")
            return path[:-4] if path.endswith(".git") else path
        return u or None

    official = {normalize(x) for x in cfg.get("official", []) if normalize(x)}
    unofficial = {normalize(x) for x in cfg.get("unofficial", []) if normalize(x)}

    def expand(set_src):
        out = set()
        now = time.time()
        for entry in set_src:
            parts = entry.split("/")
            if len(parts) == 1:
                org = entry
                meta = cache["orgs"].get(org, {})
                last_list = meta.get("repos", [])
                ts = meta.get("ts", 0)
                if not last_list or now - ts > ORG_TTL:
                    repos = org_repos_from_api(org)
                    cache["orgs"][org] = {"repos": repos, "ts": now}
                else:
                    repos = last_list
                out.update(repos)
            elif len(parts) == 2:
                out.add(entry)
            else:
                log("warn", f"Bad entry in {REPOS_FILE}: {entry}")
        return out

    mapping = {r: True for r in expand(official)}
    mapping.update({r: False for r in expand(unofficial)})
    return mapping

# === fetch commits ===
def fetch_commits(repo, is_off, state, seen):
    owner, name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page = state.get("c_page", 1)
    since = state.get("c_since")
    log("info", f"[{repo}] commits since={since} page={page}")
    while True:
        params = {"per_page": PER_PAGE, "page": page}
        if since:
            params["since"] = since
        r = safe_get(f"https://api.github.com/repos/{repo}/commits",
                     headers=gh_headers(), params=params, timeout=30)
        data = r.json()
        cnt = len(data) if isinstance(data, list) else 0
        log("info", f"[{repo}] page {page}: got {cnt} commits")
        if not data:
            break
        for c in data:
            sha = c.get("sha")
            if not sha or sha in seen:
                continue
            det = safe_get(f"https://api.github.com/repos/{repo}/commits/{sha}",
                           headers=gh_headers(), timeout=30).json()
            files = [os.path.basename(f.get("filename", "")) for f in det.get("files", [])]
            author = (c.get("author") or {}).get("login") or c["commit"]["author"].get("name")
            rec = {"sha": sha, "author": author, "url": f"{base}/commit/{sha}",
                   "repo": base, "date": c["commit"]["author"].get("date"),
                   "file_names": files, "is_official": is_off}
            seen.add(sha)
            yield author, rec
        page += 1
        time.sleep(0.1)
    state["c_page"] = 1
    state["c_since"] = utc_now()

# === fetch issues ===
def fetch_items(repo, is_off, state, seen):
    owner, name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page = state.get("i_page", 1)
    since = state.get("i_since")
    log("info", f"[{repo}] issues since={since} page={page}")
    while True:
        params = {"state": "all", "per_page": PER_PAGE, "page": page}
        if since:
            params["since"] = since
        r = safe_get(f"https://api.github.com/repos/{repo}/issues}",
                     headers=gh_headers(), params=params, timeout=30)
        data = r.json()
        cnt = len(data) if isinstance(data, list) else 0
        log("info", f"[{repo}] page {page}: got {cnt} issues/PR")
