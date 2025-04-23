#!/usr/bin/env python3
"""
fetch_commits.py — выкачивает все коммиты из указанного репозитория
и выводит топ-авторов по количеству коммитов (без базы данных).
Исправлена обработка случая, когда GitHub API возвращает "author": null.
"""

import os
import sys
import time
import collections
import requests

# === config ===
REPO     = "tact-lang/tact"
PER_PAGE = 100


def log(level: str, msg: str):
    """Логирование в stderr с указанием уровня."""
    sys.stderr.write(f"[{level}] {msg}\n")


def get_commits(repo: str) -> list[str]:
    """
    Выкачивает все коммиты из репозитория.
    Если commit['author'] is None, берём имя из commit['commit']['author']['name'].
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        log("warn", "No GITHUB_TOKEN set, rate-limit = 60 req/hour.")

    all_authors: list[str] = []
    page = 1

    while True:
        log("info", f"Fetching commits for {repo} (page {page})…")
        url = f"https://api.github.com/repos/{repo}/commits"
        resp = requests.get(url, headers=headers, params={"per_page": PER_PAGE, "page": page}, timeout=30)

        if not resp.ok:
            log("warn", f"Error fetching {repo}: {resp.status_code} {resp.text}")
            break

        data = resp.json()
        if not data:
            break

        for commit in data:
            # GitHub API иногда даёт "author": null
            if commit.get("author") is not None:
                author = (
                    commit["author"].get("login")
                    or commit["commit"]["author"].get("name", "unknown")
                )
            else:
                author = commit["commit"]["author"].get("name", "unknown")

            all_authors.append(author)

        page += 1
        time.sleep(0.1)

    return all_authors


def main():
    commits = get_commits(REPO)
    counter = collections.Counter(commits)
    total = len(commits)

    print(f"Top contributors in {REPO} (total {total} commits):\n")
    for author, count in counter.most_common(20):
        print(f"{count:>4}  {author}")


if __name__ == "__main__":
    main()
