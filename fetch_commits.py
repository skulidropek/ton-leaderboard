#!/usr/bin/env python3
"""
fetch_commits.py — минимальные запросы + резюмирование после rate-limit.
"""

import os, sys, time, json, requests, re, pathlib
from collections import defaultdict, Counter
from urllib.parse import urlparse

# ---------- config ----------
REPOS_FILE  = "ton_repos.json"
CACHE_FILE  = "cache.json"
OUTPUT_JSON = "leaderboard.json"
PER_PAGE    = 100
ORG_TTL_DAYS = 7                 # >>> NEW: как редко обновлять org->repos

FILE_LANG = {".rs":"Rust",".go":"Go",".ts":"TypeScript",".tsx":"TypeScript",
             ".py":"Python",".sol":"Solidity",".c":"C",".cpp":"C++"}

README_RE  = re.compile(r"\b(rust|go|typescript|python|solidity|c\+\+|c\b|java|kotlin)\b", re.I)

def log(l,m): sys.stderr.write(f"[{l}] {m}\n")
def utc():   return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def gh_headers():
    h={"Accept":"application/vnd.github+json"}
    if (t:=os.getenv("GITHUB_TOKEN")): h["Authorization"]=f"Bearer {t}"
    return h

# ---------- cache helpers ----------
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("cache.json is not a JSON object")
            return data
        except Exception:
            log("warn", f"Broken {CACHE_FILE}, resetting to empty")
    # default empty cache
    return {
        "commits": [],
        "issues": [],
        "users": [],
        "orgs": {},
        "repos": {}
    }

def save_cache(c):
    json.dump(c,open(CACHE_FILE,"w",encoding="utf-8"),
              indent=2,ensure_ascii=False)

# ---------- repo lists ----------
def org_needs_refresh(info:str)->bool:
    if not info: return True
    t=time.strptime(info,"%Y-%m-%dT%H:%M:%SZ")
    return (time.time()-time.mktime(t)) > ORG_TTL_DAYS*86400

def list_org_repos(org:str, cache):
    meta = cache["orgs"].get(org)
    if meta and not org_needs_refresh(meta["fetched_at"]):
        return meta["repos"]

    repos,page=[],1
    while True:
        r=requests.get(f"https://api.github.com/orgs/{org}/repos",
                       headers=gh_headers(),
                       params={"per_page":PER_PAGE,"page":page},
                       timeout=30)
        if not r.ok: break
        blk=r.json()
        if not blk: break
        repos += [f"{org}/{rep['name']}" for rep in blk]
        page+=1; time.sleep(0.1)

    cache["orgs"][org]={"repos":repos,"fetched_at":utc()}
    return repos

def load_repos(cache):
    if not pathlib.Path(REPOS_FILE).exists():
        log("error",f"{REPOS_FILE} not found"); sys.exit(1)
    raw=json.load(open(REPOS_FILE,encoding="utf-8"))

    def norm(e):
        e=e.strip()
        if e.startswith(("http://","https://")):
            p=urlparse(e); path=p.path.lstrip("/").rstrip("/"); 
            return path[:-4] if path.endswith(".git") else path
        return e

    off,unoff=set(),set()
    for k,dst in (("official",off),("unofficial",unoff)):
        for rec in raw.get(k,[]): 
            n=norm(rec); dst.add(n) if n else None

    def expand(s:set[str])->set[str]:
        out=set()
        for x in s:
            parts=x.split("/")
            if len(parts)==1: out.update(list_org_repos(x,cache))
            elif len(parts)==2: out.add(x)
        return out

    return {r:True for r in expand(off)} | {r:False for r in expand(unoff)}

# ---------- language helpers ----------
def detect_langs(files):
    langs=set()
    for f in files:
        ext=os.path.splitext(f["filename"])[1].lower()
        if ext in FILE_LANG: langs.add(FILE_LANG[ext])
    return list(langs)

def profile_langs(login, cache):
    if login in cache["users"]: return []
    url=f"https://raw.githubusercontent.com/{login}/{login}/HEAD/README.md"
    r=requests.get(url,timeout=10)
    if not r.ok: return []
    langs=set(m.group(1).capitalize() for m in README_RE.finditer(r.text))
    cache["users"].append(login)
    return list(langs)

# ---------- fetch commits / items with resume ----------
def fetch_commits(repo, state, seen_shas, is_off):
    since   = state.get("commits_since")
    page    = state.get("commits_page",1)
    owner,_ = repo.split("/")
    base    = f"https://github.com/{repo}"

    while True:
        params={"per_page":PER_PAGE,"page":page}
        if since: params["since"]=since
        r=requests.get(f"https://api.github.com/repos/{repo}/commits",
                       headers=gh_headers(),params=params,timeout=30)
        # ---- rate limit handling ----
        remain=int(r.headers.get("X-RateLimit-Remaining","1"))
        if remain==0 or r.status_code==403:
            log("warn",f"Rate limit hit at page {page} of {repo}")
            state["commits_page"]=page   # >>> NEW: remember where stopped
            return

        if not r.ok: break
        data=r.json()
        if not data: break
        for c in data:
            sha=c["sha"]
            if sha in seen_shas: continue
            detail=requests.get(f"https://api.github.com/repos/{repo}/commits/{sha}",
                                headers=gh_headers(),timeout=30).json()
            langs=detect_langs(detail.get("files",[]))
            author=(c.get("author") and c["author"]["login"]) or \
                    c["commit"]["author"]["name"]
            yield author,{
                "sha":sha,"author":author,"url":f"{base}/commit/{sha}",
                "repo":base,"date":c["commit"]["author"]["date"],
                "languages":langs,"is_official":is_off
            }
            seen_shas.add(sha)

        page+=1; time.sleep(0.1)
    # успешно дошли до конца
    state["commits_page"]=1
    state["commits_since"]=utc()

def fetch_items(repo,state,is_off,seen_items):
    since=state.get("issues_since")
    page =state.get("issues_page",1)
    base=f"https://github.com/{repo}"
    while True:
        params={"state":"all","per_page":PER_PAGE,"page":page}
        if since: params["since"]=since
        r=requests.get(f"https://api.github.com/repos/{repo}/issues",
                       headers=gh_headers(),params=params,timeout=30)
        remain=int(r.headers.get("X-RateLimit-Remaining","1"))
        if remain==0 or r.status_code==403:
            log("warn",f"Rate limit hit (issues) page {page} {repo}")
            state["issues_page"]=page
            return
        if not r.ok: break
        data=r.json()
        if not data: break
        for it in data:
            author=it.get("user",{}).get("login")
            if not author: continue
            rec={
              "number":it["number"],"title":it["title"],"url":it["html_url"],
              "repo":base,"state":it["state"],"created_at":it["created_at"],
              "is_official":is_off,
              "type":"pull_request" if "pull_request" in it else "issue"
            }
            key=f"{repo}#{it['number']}"
            if key in seen_items: continue
            seen_items.add(key)
            yield author,rec
        page+=1; time.sleep(0.1)
    state["issues_page"]=1
    state["issues_since"]=utc()

# ---------- main ----------
def main():
    cache          = load_cache()
    repos_map      = load_repos(cache)            # may update cache["orgs"]
    seen_shas      = set(cache["commits"])
    seen_items     = set(cache["issues"])
    repo_state     = cache.setdefault("repos",{})
    users=defaultdict(lambda:{
        "login":None,"profile_url":None,
        "languages":Counter(),"profile_langs":[],
        "commits":[], "issues":[], "pull_requests":[]
    })

    # ---- per repo ----
    for repo,is_off in repos_map.items():
        st=repo_state.setdefault(repo,{})
        # commits
        for author, cm in fetch_commits(repo, st, seen_shas, is_off):
            u=users[author]; u["login"]=author; u["profile_url"]=f"https://github.com/{author}"
            u["commits"].append(cm)
            for l in cm["languages"]: u["languages"][l]+=1
        # issues / PR
        for author, rec in fetch_items(repo, st, is_off, seen_items):
            u=users[author]; u["login"]=author; u["profile_url"]=f"https://github.com/{author}"
            col = "pull_requests" if rec["type"]=="pull_request" else "issues"
            u[col].append(rec)

    # profile README langs
    for author,u in users.items():
        u["profile_langs"]=profile_langs(author, cache)
        u["languages"]=dict(u["languages"])

    # ---- save everything ----
    cache["commits"]=list(seen_shas)
    cache["issues"] =list(seen_items)
    save_cache(cache)
    json.dump({"users":list(users.values())},
              open(OUTPUT_JSON,"w",encoding="utf-8"),indent=2,ensure_ascii=False)
    print(f"users={len(users)}  commits={len(seen_shas)}  items={len(seen_items)}")

if __name__=="__main__":
    main()
