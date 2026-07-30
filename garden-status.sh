#!/usr/bin/env bash
# garden-status — did my last publish build and deploy?
# Usage: ./garden-status.sh   (no auth needed; repo is public)
set -euo pipefail

REPO="sergelerner/digitalgarden"
SITE="https://notes.halfgoat.dev"

python3 - "$REPO" "$SITE" <<'EOF'
import json, sys, urllib.request, datetime

repo, site = sys.argv[1], sys.argv[2]
def get(url, accept="application/vnd.github+json"):
    req = urllib.request.Request(url, headers={"Accept": accept, "User-Agent": "garden-status"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)

commit = get(f"https://api.github.com/repos/{repo}/commits/main")
sha, msg = commit["sha"], commit["commit"]["message"].splitlines()[0]
when = datetime.datetime.fromisoformat(commit["commit"]["committer"]["date"].replace("Z", "+00:00"))
age = datetime.datetime.now(datetime.timezone.utc) - when
mins = max(0, int(age.total_seconds() // 60))
ago = f"{mins} min ago" if mins < 120 else f"{mins // 60} h ago"
print(f"latest commit : {sha[:7]} \"{msg}\" ({ago})")

checks = get(f"https://api.github.com/repos/{repo}/commits/{sha}/check-runs")["check_runs"]
if not checks:
    print("checks        : none reported yet (build may still be queuing)")
for c in checks:
    status = c["conclusion"] or c["status"]          # success/failure or in_progress/queued
    icon = {"success": "✓", "failure": "✗"}.get(status, "…")
    print(f"  {icon} {c['name']:<40} {status}")
    if status == "failure" and c.get("html_url"):
        print(f"      logs: {c['html_url']}")

try:
    req = urllib.request.Request(site, method="HEAD", headers={"User-Agent": "garden-status"})
    code = urllib.request.urlopen(req, timeout=15).status
    print(f"live site     : {site} → HTTP {code}")
except Exception as e:
    print(f"live site     : {site} → UNREACHABLE ({e})")

ok = all((c["conclusion"] == "success") for c in checks) and bool(checks)
print("verdict       :", "deployed ✓" if ok else "NOT deployed — see above")
EOF
