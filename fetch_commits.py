#!/usr/bin/env python3
"""
fetch_commits.py — автономный сборщик статистики с PAT (5 000 req/ч) и накоплением истории:

• Читает ton_repos.json (организации или owner/repo)
• Хранит единый кэш cache.json для incremental fetch (commits/issues/orgs/repos)
• При первом запуске делает полный дамп всех репо
• Далее инкрементально добавляет только новые коммиты, issue и PR
• Возвращает только имена изменённых файлов (с расширениями)
• Логирует прогресс по страницам и репозиториям
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
            # secondary rate limit
            if "secondary rate limit" in msg or "rate limit exceeded" in msg:
                retry = int(r.headers.get("Retry-After", backoff))
                log("warn", f"Rate limit on {url}, sleeping {retry}s")
                time.sleep(retry)
                backoff = min(backoff * 2, 60)
                continue
            # other forbidden
            if r.status_code == 403:
                raise RuntimeError(f"403 Forbidden {url} → {msg or 'permission denied'}")
            # too many requests
            retry = int(r.headers.get("Retry-After", backoff))
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
        raise RuntimeError("PAT_TOKEN or GITHUB_TOKEN not set")
    return {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ton-leaderboard-bot/1.0",
        "Authorization": f"Bearer {token}"
    }

# === cache ===
simple_cache = {"commits": [], "issues": [], "orgs": {}, "repos": {}}

def load_cache() -> dict:
    p = pathlib.Path(CACHE_FILE)
    if p.exists():
        try:
            data = json.load(open(CACHE_FILE, encoding="utf-8"))
            if isinstance(data, dict):
                for k, v in simple_cache.items():
                    data.setdefault(k, v.copy() if isinstance(v, list) else v)
                return data
        except Exception:
            log("warn", f"Broken {CACHE_FILE}, resetting")
    return {**simple_cache}


def save_cache(cache: dict):
    json.dump(cache, open(CACHE_FILE, "w", encoding="utf-8"), indent=2, ensure_ascii=False)

# === list org repos ===
def org_repos_from_api(org: str) -> list:
    repos, page = [], 1
    while True:
        log("info", f"[ORG] listing {org}, page {page}")
        resp = safe_get(
            f"https://api.github.com/orgs/{org}/repos",
            headers=gh_headers(), params={"per_page": PER_PAGE, "page": page}, timeout=30
        )
        if resp.status_code == 404 or not resp.ok:
            break
        data = resp.json()
        if not data:
            break
        repos += [f"{org}/{r['name']}" for r in data]
        page += 1
        time.sleep(0.1)
    return repos


def get_repos_list(cache: dict) -> dict:
    cache.setdefault("orgs", {})
    cache.setdefault("repos", {})
    if not pathlib.Path(REPOS_FILE).exists():
        log("error", f"{REPOS_FILE} not found"); sys.exit(1)
    cfg = json.load(open(REPOS_FILE, encoding="utf-8"))
    def norm(u: str):
        u = u.strip()
        if u.startswith(("http://", "https://")):
            p = urlparse(u); path = p.path.lstrip("/").rstrip("/")
            return path[:-4] if path.endswith(".git") else path
        return u or None
    official = {x for x in (norm(x) for x in cfg.get("official", [])) if x}
    unofficial = {x for x in (norm(x) for x in cfg.get("unofficial", [])) if x}
    def expand(src: set) -> set:
        out, now = set(), time.time()
        for e in src:
            parts = e.split("/")
            if len(parts) == 1:
                org, meta = e, cache["orgs"].get(e, {})
                last, ts = meta.get("repos", []), meta.get("ts", 0)
                if not last or now - ts > ORG_TTL:
                    lst = org_repos_from_api(org)
                    cache["orgs"][org] = {"repos": lst, "ts": now}
                else:
                    lst = last
                out.update(lst)
            elif len(parts) == 2:
                out.add(e)
        return out
    result = {r: True for r in expand(official)}
    result.update({r: False for r in expand(unofficial)})
    return result

# === fetch commits ===
def fetch_commits(repo: str, is_off: bool, st: dict, seen: set):
    owner, name = repo.split("/"); base = f"https://github.com/{owner}/{name}"
    page, since = st.get("c_page", 1), st.get("c_since")
    log("info", f"[{repo}] commits since={since} page={page}")
    while True:
        params = {"per_page": PER_PAGE, "page": page}
        if since: params["since"] = since
        r = safe_get(f"https://api.github.com/repos/{repo}/commits", headers=gh_headers(), params=params, timeout=30)
        data = r.json(); cnt = len(data) if isinstance(data, list) else 0
        log("info", f"[{repo}] page {page}: got {cnt} commits")
        if not data: break
        for c in data:
            sha = c.get("sha");
            if not sha or sha in seen: continue
            det = safe_get(f"https://api.github.com/repos/{repo}/commits/{sha}", headers=gh_headers(), timeout=30).json()
            files = [os.path.basename(f.get("filename", "")) for f in det.get("files", [])]
            author = (c.get("author") or {}).get("login") or c["commit"]["author"].get("name")
            rec = {"sha": sha, "author": author, "url": f"{base}/commit/{sha}",
                   "repo": base, "date": c["commit"]["author"].get("date"),
                   "file_names": files, "is_official": is_off}
            seen.add(sha); yield author, rec
        page += 1; time.sleep(0.1)
    st["c_page"], st["c_since"] = 1, utc_now()

# === fetch issues & PR ===
def fetch_items(repo: str, is_off: bool, st: dict, seen: set):
    owner, name = repo.split("/"); base = f"https://github.com/{owner}/{name}"
    page, since = st.get("i_page", 1), st.get("i_since")
    log("info", f"[{repo}] issues since={since} page={page}")
    while True:
        params = {"state": "all", "per_page": PER_PAGE, "page": page}
        if since: params["since"] = since
        r = safe_get(f"https://api.github.com/repos/{repo}/issues", headers=gh_headers(), params=params, timeout=30)
        data = r.json(); cnt = len(data) if isinstance(data, list) else 0
        log("info", f"[{repo}] page {page}: got {cnt} issues/PR")
        if not data: break
        for it in data:
            author = it.get("user", {}).get("login");
            if not author: continue
            key = f"{repo}#{it.get('number')}"
            if key in seen: continue
            rec = {"number": it.get("number"),
                   "title": it.get("title"),
                   "url": it.get("html_url"),
                   "repo": base,
                   "state": it.get("state"),
                   "created_at": it.get("created_at"),
                   "is_official": is_off,
                   "type": "pull_request" if "pull_request" in it else "issue"}
            seen.add(key); yield author, rec
        page += 1; time.sleep(0.1)
    st["i_page"], st["i_since"] = 1, utc_now()

# === main ===
def main():
    log("info", "Loading cache and existing leaderboard…")
    cache = load_cache()
    # merge previous leaderboard
    prev = {}
    if pathlib.Path(OUTPUT_FILE).exists():
        try: prev = json.load(open(OUTPUT_FILE, encoding="utf-8"))
        except: prev = {"users": []}
    users_map = {u["login"]: u for u in prev.get("users", [])}
    users = defaultdict(lambda: {"login": None, "profile_url": None, "commits": [], "issues": [], "pull_requests": []}, users_map)
    log("info", "Building repository list…")
    repos_map = get_repos_list(cache)
    log("info", f"Total repos to process: {len(repos_map)}")
    seen_shas, seen_issues = set(cache.get("commits", [])), set(cache.get("issues", []))
    repo_state = cache.setdefault("repos", {})
    for repo, is_off in repos_map.items():
        log("info", f"--- Processing {repo} (official={is_off}) ---")
        st = repo_state.setdefault(repo, {})
        for author, cm in fetch_commits(repo, is_off, st, seen_shas):
            u = users[author]; u["login"], u["profile_url"] = author, f"https://github.com/{author}"; u["commits"].append(cm)
        for author, it in fetch_items(repo, is_off, st, seen_issues):
            u = users[author]; u["login"], u["profile_url"] = author, f"https://github.com/{author}";
            col = "pull_requests" if it["type"] == "pull_request" else "issues"; u[col].append(it)
        log("info", f"[{repo}] done: commits={len(seen_shas)}, issues={len(seen_issues)}")
    cache["commits"], cache["issues"] = list(seen_shas), list(seen_issues)
    save_cache(cache)
    out = {"users": list(users.values())}
    json.dump(out, open(OUTPUT_FILE, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
    log("info", f"Done: total users={len(out['users'])}, commits={len(seen_shas)}, issues+PR={len(seen_issues)}")

if __name__ == "__main__":
    main()
