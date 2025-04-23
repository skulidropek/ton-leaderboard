#!/usr/bin/env python3
"""
fetch_commits.py — автономный сборщик статистики:

• Читает ton_repos.json (организации или owner/repo)
• Хранит единый кэш cache.json
• При первом запуске делает полный дамп всех репо
• Дальше инкрементально добавляет только новые данные
• В каждом коммите сохраняет список изменённых файлов
• Логирует процесс по репозиториям/страницам
• Выдаёт итог в leaderboard.json
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

def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")

def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def gh_headers():
    h = {"Accept": "application/vnd.github+json"}
    if (token := os.getenv("GITHUB_TOKEN")):
        h["Authorization"] = f"Bearer {token}"
    return h

# === cache helpers ===
EMPTY_CACHE = {
    "commits": [],
    "issues": [],
    "orgs": {},   # org → {"repos": [...], "ts": timestamp}
    "repos": {}   # "owner/repo" → {"c_since","c_page","i_since","i_page"}
}

def load_cache() -> dict:
    if pathlib.Path(CACHE_FILE).exists():
        try:
            data = json.load(open(CACHE_FILE, encoding="utf-8"))
            if isinstance(data, dict):
                return data
            else:
                raise ValueError("cache not a dict")
        except Exception as e:
            log("warn", f"Broken {CACHE_FILE} ({e}), resetting")
    return EMPTY_CACHE.copy()

def save_cache(cache: dict):
    json.dump(cache, open(CACHE_FILE, "w", encoding="utf-8"),
              indent=2, ensure_ascii=False)

# === repos list ===
def org_repos_from_api(org: str) -> list[str]:
    repos = []
    page = 1
    while True:
        log("info", f"[ORG] listing {org}, page {page}")
        resp = requests.get(
            f"https://api.github.com/orgs/{org}/repos",
            headers=gh_headers(),
            params={"per_page": PER_PAGE, "page": page},
            timeout=30
        )
        if not resp.ok:
            log("warn", f"[ORG] {org} list error {resp.status_code}")
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

    official   = {normalize(x) for x in cfg.get("official", []) if normalize(x)}
    unofficial = {normalize(x) for x in cfg.get("unofficial", []) if normalize(x)}

    def expand(set_in: set[str]) -> set[str]:
        out = set()
        now = time.time()
        for x in set_in:
            parts = x.split("/")
            if len(parts) == 1:
                # организация
                meta = cache["orgs"].get(x, {})
                ts   = meta.get("ts", 0)
                if now - ts > ORG_TTL:
                    # обновляем
                    repos = org_repos_from_api(x)
                    cache["orgs"][x] = {"repos": repos, "ts": now}
                out.update(cache["orgs"][x]["repos"])
            elif len(parts) == 2:
                # репозиторий
                out.add(x)
            else:
                log("warn", f"Bad entry in {REPOS_FILE}: {x}")
        return out

    return (
        {r: True  for r in expand(official)} |
        {r: False for r in expand(unofficial)}
    )

# === fetch commits & issues ===
def fetch_commits(repo: str, is_off: bool, state: dict, seen: set) -> tuple[str, dict]:
    owner, name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page  = state.get("c_page", 1)
    since = state.get("c_since")
    log("info", f"[{repo}] commits since={since} page={page}")

    while True:
        params = {"per_page": PER_PAGE, "page": page}
        if since: params["since"] = since
        resp = requests.get(
            f"https://api.github.com/repos/{repo}/commits",
            headers=gh_headers(),
            params=params,
            timeout=30
        )
        if not resp.ok:
            log("warn", f"[{repo}] commits error {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break

        for c in data:
            sha = c.get("sha")
            if not sha or sha in seen:
                continue
            # детали для списка файлов
            det = requests.get(
                f"https://api.github.com/repos/{repo}/commits/{sha}",
                headers=gh_headers(), timeout=30
            ).json()
            files = [fitem.get("filename") for fitem in det.get("files", []) if fitem.get("filename")]

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

    # после полного прохода
    state["c_page"]  = 1
    state["c_since"] = utc_now()

def fetch_items(repo: str, is_off: bool, state: dict, seen: set) -> tuple[str, dict]:
    owner, name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page  = state.get("i_page", 1)
    since = state.get("i_since")
    log("info", f"[{repo}] issues since={since} page={page}")

    while True:
        params = {"state": "all", "per_page": PER_PAGE, "page": page}
        if since: params["since"] = since
        resp = requests.get(
            f"https://api.github.com/repos/{repo}/issues",
            headers=gh_headers(),
            params=params,
            timeout=30
        )
        if not resp.ok:
            log("warn", f"[{repo}] issues error {resp.status_code}")
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

    state["i_page"]  = 1
    state["i_since"] = utc_now()

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
        "login":       None,
        "profile_url": None,
        "commits":     [],
        "issues":      [],
        "pull_requests": []
    })

    for repo, is_off in repos_map.items():
        log("info", f"=== Processing {repo} (official={is_off}) ===")
        state = repo_state.setdefault(repo, {})

        # commits
        for author, cm in fetch_commits(repo, is_off, state, seen_shas):
            u = users[author]
            u["login"]       = author
            u["profile_url"] = f"https://github.com/{author}"
            u["commits"].append(cm)

        # issues & PR
        for author, it in fetch_items(repo, is_off, state, seen_issues):
            u = users[author]
            u["login"]       = author
            u["profile_url"] = f"https://github.com/{author}"
            col = "pull_requests" if it["type"] == "pull_request" else "issues"
            u[col].append(it)

    # save cache
    cache["commits"] = list(seen_shas)
    cache["issues"]  = list(seen_issues)
    save_cache(cache)

    # write leaderboard
    out = {"users": list(users.values())}
    json.dump(out, open(OUTPUT_FILE, "w", encoding="utf-8"),
              indent=2, ensure_ascii=False)

    log("info", f"Done: users={len(out['users'])}, commits={len(seen_shas)}, issues+PR={len(seen_issues)}")

if __name__ == "__main__":
    main()
