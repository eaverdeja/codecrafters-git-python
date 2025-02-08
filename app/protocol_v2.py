from typing import Any
import urllib.request


def v2_protocol_request(url: str, method: str, data: Any | None = None) -> bytes:
    headers = {"git-protocol": "version=2"}
    request = urllib.request.Request(method=method, url=url, headers=headers, data=data)
    with urllib.request.urlopen(request) as response:
        return response.read()
