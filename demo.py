"""Synthetic producer with a live, localhost-only HTTP scrape."""
from contextlib import contextmanager
from threading import Thread
from urllib.request import HTTPHandler, ProxyHandler, build_opener
from wsgiref.simple_server import WSGIRequestHandler, make_server
import json

from endpoint import scrape_app
from metrics import Histograms


class Quiet(WSGIRequestHandler):
    def log_message(self, *_):
        pass


@contextmanager
def local_http(app):
    with make_server("127.0.0.1", 0, app, handler_class=Quiet) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": .02})
        thread.start()
        try:
            yield "http://127.0.0.1:%d" % server.server_port
        finally:
            server.shutdown()
            thread.join()


def local_scrape(registry):
    return local_http(scrape_app(registry))


if __name__ == "__main__":
    metrics = Histograms(["parse"], [10, 50])
    for value in (1, 10, 11, 50, 51):
        metrics.observe("parse", value)
    with local_scrape(metrics) as addr:
        with build_opener(ProxyHandler({}), HTTPHandler()).open(addr + "/metrics", timeout=3) as res:
            report = json.load(res)
            assert res.status == 200 and res.headers["Cache-Control"] == "no-store"
    assert report["bounds_us"] == [10, 50]
    assert report["series"] == [{"operation": "parse", "le_counts": [2, 4], "count": 5, "sum_us": 123}]
    print(json.dumps(report, separators=(",", ":")))
