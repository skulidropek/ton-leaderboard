#!/usr/bin/env python3
"""
fetch_commits.py — собирает полную статистику по всем действиям пользователей
в репозиториях организаций и выводит в JSON:
- commits
- issues
- pull_requests
"""

import os
import sys
import time
import json
import requests
from collections import defaultdict

# === config ===
ORGS        = ["tact-lang"]       # организации, которые надо скрапить
EXTRA_REPOS = []                  # дополнительные репы вида "owner/repo"
PER_PAGE    = 100
OUTPUT_JSON = "leaderboard.json"


def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")


def list_org_repos(org: str) -> list[str]:
    """Получает все публичные репозитории организации."""
    repos = []
    page = 1
    headers = {"Accept": "application/vnd.github+json"}
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        resp = requests.get(url, headers=headers, params={"per_page": PER_PAGE, "page": page}, timeout=30)
        if not resp.ok:
            log("warn", f"Failed to list repos for {org}: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break
        for r in data:
            repos.append(f"{r['owner']['login']}/{r['name']}")
        page += 1
        time.sleep(0.1)
    return repos


def get_commits(full_repo: str):
    """
    Генератор коммитов:
    возвращает dict с sha, message, url, repo, author_login, author_name, date
    """
    headers = {"Accept": "application/vnd.github+json"}
    token = os.getenv("GITHUB_TOKEN")
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
        resp = requests.get(url, headers=headers, params={"per_page": PER_PAGE, "page": page}, timeout=30)
        if not resp.ok:
            log("warn", f"Error fetching commits for {full_repo}: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break
        for c in data:
            sha = c.get("sha")
            msg = c["commit"]["message"].split("\n", 1)[0]
            commit_url = f"{repo_url}/commit/{sha}"
            date = c["commit"]["author"].get("date")
            if c.get("author"):
                login = c["author"].get("login")
                name = c["author"].get("login")
            else:
                login = None
                name = c["commit"]["author"].get("name")
            yield {
                "sha": sha,
                "message": msg,
                "url": commit_url,
                "repo": repo_url,
                "author_login": login,
                "author_name": name,
                "date": date,
            }
        page += 1
        time.sleep(0.1)


def get_issues_and_prs(full_repo: str):
    """
    Генератор: выдаёт и issues, и pull requests из репозитория.
    В type — "issue" или "pull_request".
    """
    headers = {"Accept": "application/vnd.github+json"}
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, repo = full_repo.split("/")
    repo_url = f"https://github.com/{owner}/{repo}"
    page = 1
    while True:
        log("info", f"Fetch issues/PRs for {full_repo}, page {page}")
        url = f"https://api.github.com/repos/{full_repo}/issues"
        resp = requests.get(
            url,
            headers=headers,
            params={"per_page": PER_PAGE, "page": page, "state": "all"},
            timeout=30,
        )
        if not resp.ok:
            log("warn", f"Error fetching issues for {full_repo}: {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break
        for item in data:
            author = item.get("user", {}).get("login")
            number = item.get("number")
            title = item.get("title")
            html_url = item.get("html_url")
            state = item.get("state")
            created = item.get("created_at")
            if "pull_request" in item:
                typ = "pull_request"
            else:
                typ = "issue"
            yield {
                "type": typ,
                "number": number,
                "title": title,
                "url": html_url,
                "repo": repo_url,
                "author_login": author,
                "state": state,
                "created_at": created,
            }
        page += 1
        time.sleep(0.1)


def main():
    # собираем список реп
    all_repos = set(EXTRA_REPOS)
    for org in ORGS:
        all_repos.update(list_org_repos(org))

    users = defaultdict(lambda: {
        "login": None,
        "name": None,
        "profile_url": None,
        "commits": [],
        "issues": [],
        "pull_requests": [],
    })

    # коммиты
    for repo in sorted(all_repos):
        for c in get_commits(repo):
            key = c["author_login"] or c["author_name"]
            u = users[key]
            u["login"] = c["author_login"]
            u["name"] = c["author_name"]
            u["profile_url"] = f"https://github.com/{c['author_login']}" if c["author_login"] else None
            u["commits"].append({
                "sha": c["sha"],
                "message": c["message"],
                "url": c["url"],
                "repo": c["repo"],
                "date": c["date"],
            })

    # issues и PRs
    for repo in sorted(all_repos):
        for act in get_issues_and_prs(repo):
            key = act["author_login"]
            if not key:
                continue
            u = users[key]
            u["login"] = key
            u["profile_url"] = f"https://github.com/{key}"
            record = {
                "number": act["number"],
                "title": act["title"],
                "url": act["url"],
                "repo": act["repo"],
                "state": act["state"],
                "created_at": act["created_at"],
            }
            if act["type"] == "issue":
                u["issues"].append(record)
            else:
                u["pull_requests"].append(record)

    # финальный JSON
    out = {"users": list(users.values())}
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Written stats for {len(out['users'])} users → {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
