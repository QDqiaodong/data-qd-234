"""Local HTTP demo with synthetic operation times and real admission outcomes."""
from http.client import HTTPConnection
import json
from threading import local
from urllib.parse import urlsplit

from admission import Admission, Busy, Policy
from demo import local_http
from endpoint import scrape_app
from metrics import Histograms


class DemoClock:
    """Per-thread monotone clock, advanced by the demo workload only."""
    def __init__(self):
        self._ticks = local()

    def __call__(self):
        return getattr(self._ticks, "ns", 0)

    def advance(self, us):
        self._ticks.ns = self() + us * 1000


def sample_app(registry, gate, clock):
    scrape = scrape_app(registry)
    workload = {"/parse": ("parse", 50), "/slow-parse": ("parse", 51),
                "/lookup": ("lookup", 10)}

    def app(environ, start_response):
        path = environ["PATH_INFO"]
        if path not in workload:
            return scrape(environ, start_response)
        if environ["REQUEST_METHOD"] != "GET":
            start_response("405 Method Not Allowed", [("Allow", "GET"), ("Content-Length", "0")])
            return [b""]
        operation, usecs = workload[path]
        try:
            with gate.work(operation):
                document = json.loads('{"answer": 42}')
                body = str(document["answer"]).encode("ascii")
                clock.advance(usecs)
            status = "200 OK"
        except Busy:
            status, body = "429 Too Many Requests", b"busy"
        start_response(status, [("Content-Type", "text/plain"), ("Cache-Control", "no-store"),
                                ("Content-Length", str(len(body)))])
        return [body]

    return app


def request(address, path):
    conn = HTTPConnection("127.0.0.1", urlsplit(address).port, timeout=3)
    try:
        conn.request("GET", path)
        with conn.getresponse() as response:
            return response.status, response.read()
    finally:
        conn.close()


def run():
    registry = Histograms(["parse", "lookup"], [10, 50])
    clock = DemoClock()
    policy = Policy(normal_limit=2, protected_limit=1, budget_us=50,
                    min_samples=2, max_slow_percent=50)
    gate = Admission(registry, {"parse": policy, "lookup": policy}, clock_ns=clock)
    with local_http(sample_app(registry, gate, clock)) as address:
        # Hold one parse slot in the calling worker while making HTTP requests.
        with gate.work("parse"):
            assert request(address, "/parse") == (200, b"42")
            for _ in range(2):
                assert request(address, "/slow-parse") == (200, b"42")
            limited = gate.refresh()["parse"]
            rejected = request(address, "/parse")
            unaffected = request(address, "/lookup")
            empty = gate.refresh()["parse"]
            clock.advance(50)
        assert request(address, "/parse") == (200, b"42")
        recovered = gate.refresh()["parse"]
        with gate.work("parse"):
            allowed = request(address, "/parse")
            status, raw = request(address, "/metrics")
            assert status == 200
            scrape = json.loads(raw)
            clock.advance(50)
    return limited, rejected, unaffected, empty, recovered, allowed, scrape


if __name__ == "__main__":
    limited, rejected, unaffected, empty, recovered, allowed, scrape = run()
    assert (limited.samples, limited.slow, limited.limit) == (3, 2, 1)
    assert rejected == (429, b"busy") and unaffected == (200, b"42")
    assert not empty.applied and empty.limit == 1
    assert (recovered.samples, recovered.slow, recovered.limit) == (2, 0, 2)
    assert allowed == (200, b"42")
    assert [item["count"] for item in scrape["series"]] == [1, 6]
    print(f"slow window: {limited.slow}/{limited.samples}; parse limit={limited.limit}; "
          f"parse HTTP {rejected[0]}; lookup HTTP {unaffected[0]}")
    print(f"empty window: limit={empty.limit}; fresh decision={empty.applied}")
    print(f"healthy window: {recovered.slow}/{recovered.samples}; parse limit={recovered.limit}; "
          f"parse HTTP {allowed[0]}; scrape parse count={scrape['series'][1]['count']}")
