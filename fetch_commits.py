#!/usr/bin/env python3
"""
fetch_commits.py — собирает статистику по всем коммитам, issue и PR,
метит их флагом is_official на основе ton_repos.json,
поддерживает первый полный дамп при пустом кеше,
и сохраняет всё в leaderboard.json.
"""

import os, sys, time, json, requests
from collections import defaultdict, Counter
from urllib.parse import urlparse

# ==== config ====
REPOS_FILE   = "ton_repos.json"
CACHE_FILE   = "cache.json"
OUTPUT_JSON  = "leaderboard.json"
PER_PAGE     = 100

# расширение → язык
FILE_LANG = {
    ".rs": "Rust", ".go": "Go",
    ".ts": "TypeScript", ".tsx": "TypeScript",
    ".js": "JavaScript", ".py": "Python",
    ".sol": "Solidity", ".c": "C", ".cpp": "C++",
    ".java": "Java", ".kt": "Kotlin", ".swift":"Swift"
}

def log(lvl, msg): sys.stderr.write(f"[{lvl}] {msg}\n")
def utc_now():   return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
def gh_headers():
    h = {"Accept":"application/vnd.github+json"}
    if (t:=os.getenv("GITHUB_TOKEN")):
        h["Authorization"] = f"Bearer {t}"
    return h

# ==== cache helpers ====
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            data = json.load(open(CACHE_FILE, encoding="utf-8"))
            if isinstance(data, dict):
                return data
            else:
                raise ValueError("Bad cache format")
        except Exception:
            log("warn", "Broken cache.json, resetting")
    return {"commits":[], "issues":[], "users":[], "orgs":{}, "repos":{}}

def save_cache(cache):
    json.dump(cache, open(CACHE_FILE,"w",encoding="utf-8"),
              indent=2, ensure_ascii=False)

# ==== repos config ====
def list_org_repos(org, cache):
    # аналогично, но пропускаем TTL — для простоты делаем всегда полный
    repos, page = [], 1
    while True:
        r = requests.get(f"https://api.github.com/orgs/{org}/repos",
                         headers=gh_headers(),
                         params={"per_page":PER_PAGE,"page":page},
                         timeout=30)
        if not r.ok: break
        block = r.json()
        if not block: break
        repos += [f"{org}/{repo['name']}" for repo in block]
        page += 1; time.sleep(0.1)
    return repos

def load_repos(cache):
    if not os.path.exists(REPOS_FILE):
        log("error", f"{REPOS_FILE} not found"); sys.exit(1)
    raw = json.load(open(REPOS_FILE, encoding="utf-8"))
    def norm(e):
        e=e.strip()
        if e.startswith(("http://","https://")):
            p=urlparse(e); path=p.path.lstrip("/").rstrip("/")
            return path[:-4] if path.endswith(".git") else path
        return e
    off, unoff = set(), set()
    for kind, dest in (("official",off),("unofficial",unoff)):
        for ent in raw.get(kind, []):
            n = norm(ent)
            if n: dest.add(n)
    def expand(src):
        out=set()
        for x in src:
            parts=x.split("/")
            if len(parts)==1:
                out.update(list_org_repos(x, cache))
            elif len(parts)==2:
                out.add(x)
        return out
    return {r:True for r in expand(off)} | {r:False for r in expand(unoff)}

# ==== language detection ====
def detect_langs(files_list):
    langs=set()
    for f in files_list:
        ext = os.path.splitext(f["filename"])[1].lower()
        if ext in FILE_LANG:
            langs.add(FILE_LANG[ext])
    return list(langs)

# ==== fetch commits/issues ====
def fetch_commits(repo, state, seen_shas, is_off, first_run):
    owner, name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page = 1 if first_run else state.get("commits_page",1)
    since = None if first_run else state.get("commits_since")
    while True:
        params={"per_page":PER_PAGE,"page":page}
        if since: params["since"]=since
        r = requests.get(f"https://api.github.com/repos/{repo}/commits",
                         headers=gh_headers(), params=params, timeout=30)
        if not r.ok: break
        data = r.json()
        if not data: break
        for c in data:
            sha = c["sha"]
            if sha in seen_shas: continue
            # fetch detail for files
            det = requests.get(
                f"https://api.github.com/repos/{repo}/commits/{sha}",
                headers=gh_headers(), timeout=30).json()
            langs = detect_langs(det.get("files",[]))
            author = (c.get("author") and c["author"].get("login")) \
                     or c["commit"]["author"].get("name","unknown")
            yield author, {
                "sha":sha, "author":author,
                "url":f"{base}/commit/{sha}",
                "repo":base, "date":c["commit"]["author"]["date"],
                "languages":langs, "is_official":is_off
            }
            seen_shas.add(sha)
        page += 1; time.sleep(0.1)
    # после полного прохода
    state["commits_page"]  = 1
    state["commits_since"] = utc_now()

def fetch_items(repo, state, is_off, seen_items, first_run):
    owner,name = repo.split("/")
    base = f"https://github.com/{owner}/{name}"
    page = 1 if first_run else state.get("issues_page",1)
    since = None if first_run else state.get("issues_since")
    while True:
        params={"state":"all","per_page":PER_PAGE,"page":page}
        if since: params["since"]=since
        r = requests.get(f"https://api.github.com/repos/{repo}/issues",
                         headers=gh_headers(), params=params, timeout=30)
        if not r.ok: break
        data = r.json()
        if not data: break
        for it in data:
            author = it.get("user",{}).get("login")
            if not author: continue
            rec = {
                "number": it["number"], "title": it["title"],
                "url": it["html_url"], "repo":base,
                "state": it["state"], "created_at": it["created_at"],
                "is_official":is_off,
                "type":"pull_request" if "pull_request" in it else "issue"
            }
            key = f"{repo}#{it['number']}"
            if key in seen_items: continue
            seen_items.add(key)
            yield author, rec
        page += 1; time.sleep(0.1)
    state["issues_page"]  = 1
    state["issues_since"] = utc_now()

# ==== main ====
def main():
    cache = load_cache()
    first_run = (not cache["commits"] and not cache["issues"])
    repos_map = load_repos(cache)

    seen_shas  = set(cache["commits"])
    seen_items = set(cache["issues"])
    repo_state = cache.setdefault("repos",{})

    users = defaultdict(lambda:{
        "login":None,"profile_url":None,
        "languages":Counter(),"profile_langs":[],
        "commits":[], "issues":[], "pull_requests":[]
    })

    # сбор данных
    for repo, is_off in repos_map.items():
        st = repo_state.setdefault(repo,{})
        # коммиты
        for author, cm in fetch_commits(repo, st, seen_shas, is_off, first_run):
            u = users[author]
            u["login"]       = author
            u["profile_url"] = f"https://github.com/{author}"
            u["commits"].append(cm)
            for lang in cm["languages"]:
                u["languages"][lang] += 1
        # issues/PR
        for author, rec in fetch_items(repo, st, is_off, seen_items, first_run):
            u = users[author]
            u["login"]       = author
            u["profile_url"] = f"https://github.com/{author}"
            key = "pull_requests" if rec["type"]=="pull_request" else "issues"
            u[key].append(rec)

    # fetch_profile_langs можно вставить по аналогии…

    # сохранить кеш
    cache["commits"] = list(seen_shas)
    cache["issues"]  = list(seen_items)
    save_cache(cache)

    # prepare output
    out = {"users": []}
    for u in users.values():
        u["languages"] = dict(u["languages"])
        out["users"].append(u)
    json.dump(out, open(OUTPUT_JSON,"w",encoding="utf-8"),
              indent=2, ensure_ascii=False)

    print(f"Full run: {first_run}; users={len(out['users'])}, commits={len(seen_shas)}, items={len(seen_items)}")

if __name__=="__main__":
    main()