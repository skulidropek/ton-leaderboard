#!/usr/bin/env python3
"""
fetch_commits.py — собирает статистику по всем коммитам, issue и PR,
метит их флагом is_official на основе одного JSON-файла ton_repos.json
(в котором можно указывать и организации, и конкретные репозитории)
и сохраняет результат в leaderboard.json.
"""

import os
import sys
import time
import json
import requests

from collections import defaultdict
from urllib.parse import urlparse

# === config ===
REPOS_FILE   = "ton_repos.json"    # JSON с ключами "official" и "unofficial"
PER_PAGE     = 100
CACHE_FILE   = "commit_cache.json"
OUTPUT_JSON  = "leaderboard.json"


def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")


def list_org_repos(org: str) -> list[str]:
    """
    Возвращает список публичных репозиториев организации "org" в формате owner/repo.
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    repos = []
    page = 1
    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        resp = requests.get(url, headers=headers,
                            params={"per_page": PER_PAGE, "page": page}, timeout=30)
        if not resp.ok:
            log("warn", f"Ошибка списка реп {org}: HTTP {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break
        for r in data:
            repos.append(f"{r['owner']['login']}/{r['name']}")
        page += 1
        time.sleep(0.1)
    return repos


def load_repos_json() -> dict[str, set[str]]:
    """
    Читает ton_repos.json вида:
    {
      "official":   ["owner/repo", "https://github.com/org2", ...],
      "unofficial": [...]
    }
    Нормализует каждую запись:
      - Если указана URL организации или owner без репы → разворачивает все её репы
      - Если указан конкретный owner/repo → добавляет его
    Возвращает dict с двумя множествами: {"official": {...}, "unofficial": {...}}
    """
    if not os.path.exists(REPOS_FILE):
        log("error", f"{REPOS_FILE} не найден")
        sys.exit(1)

    with open(REPOS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    def normalize(entry: str) -> str | None:
        e = entry.strip()
        if not e or e.startswith("#"):
            return None
        # URL?
        if e.startswith(("http://", "https://")):
            p = urlparse(e)
            path = p.path.lstrip("/").rstrip("/")
            if path.endswith(".git"):
                path = path[:-4]
            return path or None
        # иначе owner/repo или просто owner
        return e

    result = {"official": set(), "unofficial": set()}
    for key in ("official", "unofficial"):
        for raw in data.get(key, []):
            norm = normalize(raw)
            if not norm:
                continue
            parts = norm.split("/")
            if len(parts) == 1:
                # это организация
                for repo in list_org_repos(norm):
                    result[key].add(repo)
            elif len(parts) == 2:
                # конкретный репозиторий
                result[key].add(norm)
            else:
                log("warn", f"Неподдерживаемый формат в {REPOS_FILE}: {raw}")
    return result


def load_cache() -> set[str]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            log("warn", f"Не удалось прочитать {CACHE_FILE}, сбрасываем")
    return set()


def save_cache(shas: set[str]):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(shas), f, indent=2, ensure_ascii=False)


def fetch_commits(repo_full: str, seen: set[str], is_official: bool):
    """
    Генератор новых коммитов из repo_full:
    {
      sha, author, url, repo, date, is_official
    }
    Пропускает SHA из seen.
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        log("warn", "No GITHUB_TOKEN set — rate-limit = 60 req/hour.")

    owner, repo = repo_full.split("/")
    base_url = f"https://github.com/{owner}/{repo}"
    page = 1

    while True:
        log("info", f"Fetch commits for {repo_full}, page {page}")
        resp = requests.get(
            f"https://api.github.com/repos/{repo_full}/commits",
            headers=headers,
            params={"per_page": PER_PAGE, "page": page},
            timeout=30
        )
        if not resp.ok:
            log("warn", f"{repo_full} commits error: HTTP {resp.status_code}")
            break

        data = resp.json()
        if not data:
            break

        for c in data:
            sha = c.get("sha")
            if not sha or sha in seen:
                continue
            author = (c.get("author") and c["author"].get("login")) \
                     or c["commit"]["author"].get("name", "unknown")
            yield {
                "sha":         sha,
                "author":      author,
                "url":         f"{base_url}/commit/{sha}",
                "repo":        base_url,
                "date":        c["commit"]["author"].get("date"),
                "is_official": is_official
            }
            seen.add(sha)

        page += 1
        time.sleep(0.1)


def fetch_issues_prs(repo_full: str, is_official: bool):
    """
    Генератор всех issues и pull requests:
      author, {
        number, title, url, repo, state, created_at, is_official, type
      }
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo = repo_full.split("/")
    base_url = f"https://github.com/{owner}/{repo}"
    page = 1

    while True:
        log("info", f"Fetch issues/PRs for {repo_full}, page {page}")
        resp = requests.get(
            f"https://api.github.com/repos/{repo_full}/issues",
            headers=headers,
            params={"per_page": PER_PAGE, "page": page, "state": "all"},
            timeout=30
        )
        if not resp.ok:
            log("warn", f"{repo_full} issues error: HTTP {resp.status_code}")
            break

        data = resp.json()
        if not data:
            break

        for it in data:
            author = it.get("user", {}).get("login")
            if not author:
                continue
            rec = {
                "number":     it.get("number"),
                "title":      it.get("title"),
                "url":        it.get("html_url"),
                "repo":       base_url,
                "state":      it.get("state"),
                "created_at": it.get("created_at"),
                "is_official": is_official,
                "type":       "pull_request" if "pull_request" in it else "issue"
            }
            yield author, rec

        page += 1
        time.sleep(0.1)


def main():
    cfg = load_repos_json()
    official   = cfg["official"]
    unofficial = cfg["unofficial"]
    all_repos  = {**{r: True for r in official},
                  **{r: False for r in unofficial}}

    seen = load_cache()
    users = defaultdict(lambda: {
        "login":         None,
        "profile_url":   None,
        "commits":       [],
        "issues":        [],
        "pull_requests": []
    })

    # Коммиты
    for repo_full, is_off in all_repos.items():
        for c in fetch_commits(repo_full, seen, is_off):
            u = users[c["author"]]
            u["login"]       = c["author"]
            u["profile_url"] = f"https://github.com/{c['author']}"
            u["commits"].append(c)

    # Issues & PRs
    for repo_full, is_off in all_repos.items():
        for author, rec in fetch_issues_prs(repo_full, is_off):
            u = users.setdefault(author, {
                "login":         author,
                "profile_url":   f"https://github.com/{author}",
                "commits":       [],
                "issues":        [],
                "pull_requests": []
            })
            key = "pull_requests" if rec["type"] == "pull_request" else "issues"
            u[key].append(rec)

    save_cache(seen)

    output = {"users": list(users.values())}
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written stats for {len(output['users'])} users → {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
