import os
import urllib.request

request = urllib.request.Request(
    "http://127.0.0.1:8000/health/ready",
    headers={"X-API-Key": os.environ["API_KEY"]},
)
with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
    request, timeout=3
) as response:
    if response.status != 200:
        raise SystemExit(1)
