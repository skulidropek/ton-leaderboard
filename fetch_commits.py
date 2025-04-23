#!/usr/bin/env python3
"""
fetch_commits.py — собирает полную статистику по всем действиям пользователей
в репозиториях организаций и сохраняет в JSON:
  - commits
  - issues
  - pull_requests
Сохраняет кеш всех SHA в commit_cache.json, чтобы не дублировать запросы.
"""

import os
import sys
import time
import json
import requests
from collections import defaultdict

# === config ===
ORGS         = ["tact-lang"]      # организации для сканирования
EXTRA_REPOS  = []                 # дополнительные репы "owner/repo"
PER_PAGE     = 100
CACHE_FILE   = "commit_cache.json"
OUTPUT_JSON  = "leaderboard.json"


def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")


def load_cache() -> set[str]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            log("warn", f"Не удалось прочитать {CACHE_FILE}, начнём с чистого кеша")
    return set()


def save_cache(shas: set[str]):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(shas), f, indent=2, ensure_ascii=False)


def list_org_repos(org: str) -> list[str]:
    """Собирает все публичные репозитории организации."""
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
            log("warn", f"Ошибка списка реп {org}: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break
        repos += [f"{r['owner']['login']}/{r['name']}" for r in data]
        page += 1
        time.sleep(0.1)
    return repos


def fetch_commits(full_repo: str, seen: set[str]):
    """
    Генератор новых коммитов (sha, author, url, repo, date).
    Пропускает SHA, которые уже в seen.
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        log("warn", "No GITHUB_TOKEN set, rate-limit = 60 req/hour.")

    owner, repo = full_repo.split("/")
    repo_url = f"https://github.com/{owner}/{repo}"
    page = 1

    while True:
        log("info", f"Fetch commits for {full_repo}, page {page}")
        url = f"https://api.github.com/repos/{full_repo}/commits"
        resp = requests.get(url, headers=headers,
                            params={"per_page": PER_PAGE, "page": page}, timeout=30)
        if not resp.ok:
            log("warn", f"Error fetching commits {full_repo}: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break

        for c in data:
            sha = c.get("sha")
            if not sha or sha in seen:
                continue
            # автор
            if c.get("author"):
                login = c["author"].get("login")
                name  = login or c["commit"]["author"].get("name", "unknown")
            else:
                login = None
                name  = c["commit"]["author"].get("name", "unknown")
            yield {
                "sha": sha,
                "author_login": login,
                "author_name": name,
                "url": f"{repo_url}/commit/{sha}",
                "repo": repo_url,
                "date": c["commit"]["author"].get("date"),
            }
            seen.add(sha)

        page += 1
        time.sleep(0.1)


def fetch_issues_prs(full_repo: str):
    """
    Генератор всех issue и pull requests:
      { type, number, title, url, repo, author_login, state, created_at }
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo = full_repo.split("/")
    repo_url = f"https://github.com/{owner}/{repo}"
    page = 1

    while True:
        log("info", f"Fetch issues/PRs for {full_repo}, page {page}")
        url = f"https://api.github.com/repos/{full_repo}/issues"
        resp = requests.get(url, headers=headers,
                            params={"per_page": PER_PAGE, "page": page, "state": "all"}, timeout=30)
        if not resp.ok:
            log("warn", f"Error fetching issues/PRs {full_repo}: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break

        for item in data:
            author = item.get("user", {}).get("login")
            if not author:
                continue
            rec = {
                "number": item.get("number"),
                "title":  item.get("title"),
                "url":    item.get("html_url"),
                "repo":   repo_url,
                "author": author,
                "state":  item.get("state"),
                "created_at": item.get("created_at"),
            }
            if "pull_request" in item:
                rec["type"] = "pull_request"
            else:
                rec["type"] = "issue"
            yield rec

        page += 1
        time.sleep(0.1)


def main():
    # 1) Собираем список реп
    repos = set(EXTRA_REPOS)
    for org in ORGS:
        repos.update(list_org_repos(org))

    # 2) Загружаем кеш SHA
    seen_shas = load_cache()

    # 3) Собираем данные
    users = defaultdict(lambda: {
        "login": None,
        "name": None,
        "profile_url": None,
        "commits": [],
        "issues": [],
        "pull_requests": []
    })

    for repo in sorted(repos):
        # коммиты
        for c in fetch_commits(repo, seen_shas):
            key = c["author_login"] or c["author_name"]
            u = users[key]
            u["login"]       = c["author_login"]
            u["name"]        = c["author_name"]
            u["profile_url"] = f"https://github.com/{c['author_login']}" if c["author_login"] else None
            u["commits"].append({
                "sha": c["sha"],
                "url": c["url"],
                "repo": c["repo"],
                "date": c["date"]
            })
        # issues и PRs
        for rec in fetch_issues_prs(repo):
            u = users[rec["author"]]
            u["login"]       = rec["author"]
            u["profile_url"] = f"https://github.com/{rec['author']}"
            if rec["type"] == "issue":
                u["issues"].append(rec)
            else:
                u["pull_requests"].append(rec)

    # 4) Сохраняем кеш и итоговый JSON
    save_cache(seen_shas)
    result = {"users": list(users.values())}
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Written stats for {len(result['users'])} users → {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
