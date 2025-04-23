#!/usr/bin/env python3
"""
fetch_commits.py — автономный сборщик статистики с PAT (5 000 req/ч):

• Читает ton_repos.json (организации или owner/repo)
• Хранит единый кэш cache.json
• При первом запуске делает полный дамп всех репо
• Дальше инкрементально добавляет только новые коммиты, issue и PR
• Сохраняет список изменённых файлов в каждом коммите
• Логирует процесс по репозиториям/страницам
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
        if r.status_code == 429:
            retry = r.headers.get("Retry-After")
            wait = int(retry) if retry else backoff
            log("warn", f"429 from {url}, sleeping {wait}s")
            time.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue
        return r

def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def gh_headers():
    """
    Собираем заголовки с токеном:
    Сначала смотрим PAT_TOKEN, затем GITHUB_TOKEN.
    """
    h = {"Accept": "application/vnd.github+json"}
    tok = os.getenv("PAT_TOKEN") or os.getenv("GITHUB_TOKEN")
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h

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
            key = f"{repo}#{it.get('number')}"
            if key in seen:
                continue
            seen.add(key)
            rec = {
                "number":     it.get("number"),
                "title":      it.get("title"),
                "url":        it.get("html_url"),
                "repo":       base,
                "state":      it.get("state"),
                "created_at": it.get("created_at"),
                "is_official": is_off,
                "type":       "pull_request" if "pull_request" in it else "issue"
            }
            yield author, rec

        page += 1
        time.sleep(0.1)

    st["i_page"]  = 1
    st["i_since"] = utc_now()

# === main ===
def main():
    log("info", "Loading cache...")
    cache = load_cache()

    log("info", "Building repository list...")
    repos_map = get_repos_list(cache)
    log("info", f"Total repos to process: {len(repos_map)}")

    seen_shas   = set(cache.get("commits", []))
    seen_issues = set(cache.get("issues", []))
    repo_state  = cache.setdefault("repos", {})

    users = defaultdict(lambda: {
        "login":         None,
        "profile_url":   None,
        "commits":       [],
        "issues":        [],
        "pull_requests": []
    })

    for repo, is_off in repos_map.items():
        log("info", f"--- Processing {repo} (official={is_off}) ---")
        st = repo_state.setdefault(repo, {})

        for author, cm in fetch_commits(repo, is_off, st, seen_shas):
            u = users[author]
            u["login"]       = author
            u["profile_url"] = f"https://github.com/{author}"
            u["commits"].append(cm)

        for author, it in fetch_items(repo, is_off, st, seen_issues):
            u = users[author]
            u["login"]       = author
            u["profile_url"] = f"https://github.com/{author}"
            col = "pull_requests" if it["type"] == "pull_request" else "issues"
            u[col].append(it)

    cache["commits"] = list(seen_shas)
    cache["issues"]  = list(seen_issues)
    save_cache(cache)

    out = {"users": list(users.values())}
    json.dump(out, open(OUTPUT_FILE, "w", encoding="utf-8"),
              indent=2, ensure_ascii=False)

    log("info", f"Done: users={len(out['users'])}, commits={len(seen_shas)}, issues+PR={len(seen_issues)}")

if __name__ == "__main__":
    main()
