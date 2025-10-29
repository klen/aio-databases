from urllib.parse import SplitResult


def redact_url(parsed_url: SplitResult) -> SplitResult:
    """Redact password from URL for representation."""
    if parsed_url.password:
        hostname_port = parsed_url.hostname or ""
        if parsed_url.port:
            hostname_port += f":{parsed_url.port}"
        return parsed_url._replace(netloc=(parsed_url.username or "") + f":***@{hostname_port}")
    return parsed_url
