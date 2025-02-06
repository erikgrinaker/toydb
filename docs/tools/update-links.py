#!/usr/bin/env python3
#
# Updates GitHub code links to the latest commit SHA.

import os, re, sys, argparse
import requests

GITHUB_API = "https://api.github.com"

def get_latest_sha(owner, repo, path, token):
    url = f"{GITHUB_API}/repos/{owner}/{repo}/commits"
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    params = {"path": path, "sha": "main", "per_page": 1}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data[0]["sha"] if data else None

def process_markdown(text, token):
    pattern = re.compile(
        r"https://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+)/blob/"
        r"(?P<oldsha>[0-9a-f]{7,40})/(?P<path>[^#)\s]+)"
    )
    cache = {}
    def replacer(m):
        print(f"Checking {m.group(0)}")
        owner, repo, oldsha, path = m.group("owner","repo","oldsha","path")
        key = (owner, repo, path)
        print(f"Key: {key}")
        if key not in cache:
            cache[key] = get_latest_sha(owner, repo, path, token)
        newsha = cache[key]
        if newsha and newsha != oldsha:
            print(f"Updating {m.group(0)} to {newsha}")
            return m.group(0).replace(oldsha, newsha)
        return m.group(0)
    return pattern.sub(replacer, text)

def main():
    p = argparse.ArgumentParser(description="Update GitHub blob links to latest SHAs")
    p.add_argument("file", nargs="?", help="Markdown file to update (defaults to stdin/stdout)")
    args = p.parse_args()
    token = os.getenv("GITHUB_TOKEN")
    if args.file:
        text = open(args.file, encoding="utf-8").read()
        updated = process_markdown(text, token)
        with open(args.file, "w", encoding="utf-8") as f:
            f.write(updated)
    else:
        text = sys.stdin.read()
        sys.stdout.write(process_markdown(text, token))

if __name__ == "__main__":
    main()
