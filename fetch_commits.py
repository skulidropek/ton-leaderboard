#!/usr/bin/env python3
"""
fetch_commits.py — выкачивает все коммиты из указанного репозитория,
обновляет кеш SHA в commit_cache.json и выводит топ-авторов по количеству коммитов.
"""

import os
import sys
import time
import json
import collections
import requests

# === config ===
REPO       = "tact-lang/tact"
PER_PAGE   = 100
CACHE_FILE = "commit_cache.json"


def log(level: str, msg: str):
    sys.stderr.write(f"[{level}] {msg}\n")


def load_cache() -> set[str]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
            return set(data)
        except Exception:
            log("warn", f"Не удалось прочитать {CACHE_FILE}, сбрасываем кеш")
    return set()


def save_cache(shas: set[str]):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(sorted(shas), f, indent=2)
    except Exception as e:
        log("error", f"Не удалось записать {CACHE_FILE}: {e}")


def get_commits(repo: str):
    """
    Генератор: возвращает (sha, author) для каждого коммита в репо.
    """
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        log("warn", "No GITHUB_TOKEN set, rate-limit = 60 req/hour.")

    page = 1
    while True:
        log("info", f"Fetching commits for {repo} (page {page})…")
        url = f"https://api.github.com/repos/{repo}/commits"
        resp = requests.get(
            url,
            headers=headers,
            params={"per_page": PER_PAGE, "page": page},
            timeout=30,
        )

        if not resp.ok:
            log("warn", f"Error fetching {repo}: {resp.status_code}")
            break

        data = resp.json()
        if not data:
            break

        for c in data:
            sha = c.get("sha") or c.get("commit", {}).get("tree", {}).get("sha", "")
            # автор может быть None
            if c.get("author"):
                author = c["author"].get("login") or c["commit"]["author"].get("name", "unknown")
            else:
                author = c["commit"]["author"].get("name", "unknown")
            yield sha, author

        page += 1
        time.sleep(0.1)


def main():
    # Загрузка уже известных SHA
    seen = load_cache()

    all_authors: list[str] = []
    all_shas: set[str] = set(seen)  # начинаем с того, что уже было

    # Пробегаем все коммиты и собираем список авторов + всех SHA
    for sha, author in get_commits(REPO):
        all_shas.add(sha)
        all_authors.append(author)

    # Сохраняем обновлённый кеш SHA
    save_cache(all_shas)

    # Выводим топ-авторов по всем скачанным коммитам
    counter = collections.Counter(all_authors)
    total = len(all_authors)
    print(f"Top contributors in {REPO} (total {total} commits):\n")
    for author, count in counter.most_common(20):
        print(f"{count:>5}  {author}")


if __name__ == "__main__":
    main()
