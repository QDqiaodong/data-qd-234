"""Small mountable WSGI scrape endpoint for the local histogram schema."""
import json


def scrape_app(registry):
    def app(environ, start_response):
        headers = [("Cache-Control", "no-store")]
        if environ["PATH_INFO"] != "/metrics":
            status, media, body = "404 Not Found", "text/plain", b"not found"
        elif environ["REQUEST_METHOD"] != "GET":
            status, media, body = "405 Method Not Allowed", "text/plain", b"method not allowed"
            headers.append(("Allow", "GET"))
        else:
            status, media = "200 OK", "application/json"
            body = json.dumps(registry.snapshot(), separators=(",", ":")).encode("ascii")
        headers.extend([("Content-Type", media), ("Content-Length", str(len(body)))])
        start_response(status, headers)
        return [body]
    return app
