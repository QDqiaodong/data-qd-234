from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
import json
from threading import Barrier
from urllib.parse import urlsplit
from unittest import TestCase

from demo import local_scrape
from metrics import Histograms


class MetricsTest(TestCase):
    def test_inclusive_bounds_labels_and_non_mutation_on_rejection(self):
        h = Histograms(["查找", "decode"], [0, 10])
        for us in (0, 1, 10, 11):
            h.observe("查找", us)
        with self.assertRaises(KeyError):
            h.observe("unknown", 1)
        for bad in (True, -1, 1.5):
            with self.assertRaises(ValueError):
                h.observe("查找", bad)
        self.assertEqual(h.snapshot()["series"], [
            {"operation": "decode", "le_counts": [0, 0], "count": 0, "sum_us": 0},
            {"operation": "查找", "le_counts": [1, 3], "count": 4, "sum_us": 22}])

    def test_concurrent_updates_and_scrapes_consistent(self):
        h = Histograms(["cpu"], [4, 5])
        barrier = Barrier(5, timeout=4)

        def producer():
            barrier.wait()
            for _ in range(450):
                h.observe("cpu", 5)

        with ThreadPoolExecutor(max_workers=4) as pool:
            tasks = [pool.submit(producer) for _ in range(4)]
            barrier.wait()
            for _ in range(500):
                item, = h.snapshot()["series"]
                self.assertEqual(item["sum_us"], item["count"] * 5)
                self.assertEqual(item["le_counts"], [0, item["count"]])
            for task in tasks:
                task.result(5)
        self.assertEqual(h.snapshot()["series"][0]["count"], 1800)

    def test_real_scrape_and_http_contract(self):
        h = Histograms(["parse"], [3])
        h.observe("parse", 3)
        with local_scrape(h) as addr:
            port = urlsplit(addr).port
            for method, path, expected in (("GET", "/metrics", 200), ("POST", "/metrics", 405),
                                           ("GET", "/wrong", 404)):
                conn = HTTPConnection("127.0.0.1", port, timeout=3)
                try:
                    conn.request(method, path)
                    with conn.getresponse() as res:
                        data = res.read()
                        self.assertEqual(res.status, expected)
                        self.assertEqual(int(res.headers["Content-Length"]), len(data))
                        if expected == 200:
                            self.assertEqual(json.loads(data), h.snapshot())
                            self.assertEqual(res.headers["Cache-Control"], "no-store")
                        if expected == 405:
                            self.assertEqual(res.headers["Allow"], "GET")
                finally:
                    conn.close()

    def test_invalid_schema(self):
        for operations, bounds in [([], [1]), (["a", "a"], [1]), (["a"], [1, 1]),
                                   (["a"], [True]), (["a"], [])]:
            with self.assertRaises(ValueError):
                Histograms(operations, bounds)
