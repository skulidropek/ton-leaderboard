#!/usr/bin/env python3
"""
fetch_commits.py — выкачивает коммиты из хардкодного списка организаций и
дополнительных репозиториев, выводит топ-авторов по количеству коммитов.
Добавлена поддержка GITHUB_TOKEN для обхода rate-limit и корректная работа stderr.
"""

import requests
import time
import collections
import sys
import os

# Проверка версии Python
if sys.version_info < (3, 1):
    sys.stderr.write("Error: Python 3.1 or later is required.\n")
    sys.exit(1)

# GitHub API settings
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
HEADERS = {'Accept': 'application/vnd.github+json'}
if GITHUB_TOKEN:
    HEADERS['Authorization'] = f"Bearer {GITHUB_TOKEN}"
else:
    sys.stderr.write("[warn]: No GITHUB_TOKEN set, rate-limit = 60 req/hour.\n")

PER_PAGE = 100  # число коммитов на страницу
SLEEP = 0.1     # задержка между запросами (сек)

# === Настройка ===
ORGS = [
    "tact-lang",
    # Другие организации при необходимости
]
REPOS = [
    # "user/custom-repo",
]


def get_commits(repo: str):
    authors = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{repo}/commits"
        try:
            resp = requests.get(url, headers=HEADERS, params={"per_page": PER_PAGE, "page": page}, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            if resp.status_code == 403:
                sys.stderr.write(f"[warn]: 403 Forbidden for {repo}.\n")
                return authors
            else:
                raise
        data = resp.json()
        if not data:
            break
        for c in data:
            author = (
                c.get("author", {}).get("login") or
                c["commit"]["author"].get("name", "unknown")
            )
            authors.append(author)
        page += 1
        time.sleep(SLEEP)
    return authors


def get_org_repos(org: str):
    repos = []
    page = 1
    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        try:
            resp = requests.get(url, headers=HEADERS, params={"per_page": PER_PAGE, "page": page}, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            if resp.status_code == 403:
                sys.stderr.write(f"[warn]: 403 Forbidden for org {org}.\n")
                return repos
            else:
                raise
        data = resp.json()
        if not data:
            break
        repos.extend([f"{org}/{r['name']}" for r in data])
        page += 1
        time.sleep(SLEEP)
    return repos


def main():
    # Собираем репозитории
    repos = []
    for org in ORGS:
        sys.stderr.write(f"[info]: Loading repos for org '{org}'...\n")
        repos.extend(get_org_repos(org))
    repos.extend(REPOS)
    if not repos:
        sys.stderr.write("[error]: No repos to fetch.\n")
        sys.exit(1)

    # Сбор авторов
    all_authors = []
    for repo in repos:
        sys.stderr.write(f"[info]: Fetching commits for {repo}...\n")
        all_authors.extend(get_commits(repo))

    counter = collections.Counter(all_authors)
    total = sum(counter.values())
    print(f"Top contributors (total commits: {total}):\n")
    for author, count in counter.most_common(20):
        print(f"{count:>6}  {author}")


if __name__ == '__main__':
    main()