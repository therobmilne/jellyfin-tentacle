"""GET /api/activity end to end with a Sonarr series in the Searching list.

Exercises the whole wanted-list build (Sonarr wanted/missing -> per-card
episode ids -> the cache -> the route), whatever helper it lives in, so a
refactor that moves those lines into another function cannot leave a name
behind that only existed in the old one (a NameError is a 500 on Activity).

Run from the tentacle/ directory:  python -m unittest discover -s tests
"""
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

NOW = datetime.utcnow()


def _iso(d):
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


SERIES = {"id": 7, "tmdbId": 70, "tvdbId": 71, "title": "Long Runner", "monitored": True, "year": 2020,
          "added": _iso(NOW - timedelta(days=400)), "images": [], "statistics": {"episodeFileCount": 2}}
MISSING = [{"id": 700 + e, "seriesId": 7, "seasonNumber": 1, "episodeNumber": e, "monitored": True,
            "hasFile": False, "airDateUtc": _iso(NOW - timedelta(days=e)), "series": SERIES} for e in (1, 2, 3)]


class _R:
    def __init__(self, body, status=200):
        self.body, self.status_code, self.text = body, status, "x"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def json(self):
        return self.body


def fake_get(url, headers=None, params=None, timeout=None, **kw):
    path = url.split("/api/v3/", 1)[-1].split("?")[0]
    if path == "wanted/missing":
        return _R({"page": 1, "pageSize": 250, "totalRecords": len(MISSING), "records": MISSING})
    if path == "series":
        return _R([SERIES])
    if path in ("queue",):
        return _R({"page": 1, "pageSize": 100, "totalRecords": 0, "records": []})
    if path in ("command", "calendar", "health", "rootfolder", "diskspace", "episode"):
        return _R([])
    return _R([], 404)


class TestActivityRouteWithAWantedSeries(unittest.TestCase):
    def setUp(self):
        import models.database as mdb
        from models.database import get_db, TentacleUser
        from routers import activity
        from routers.auth import get_user_from_request
        self.activity = activity
        engine = create_engine(f"sqlite:///{tempfile.mkdtemp()}/t.db", connect_args={"check_same_thread": False})
        mdb.Base.metadata.create_all(engine)
        self.db = sessionmaker(bind=engine)()
        self.addCleanup(self.db.close)
        self.db.add(TentacleUser(id=1, jellyfin_user_id="a" * 32, display_name="Admin", is_admin=True))
        self.db.add(TentacleUser(id=2, jellyfin_user_id="b" * 32, display_name="Kid", is_admin=False))
        self.db.commit()
        mdb.set_setting(self.db, "sonarr_url", "http://sonarr:8989")
        mdb.set_setting(self.db, "sonarr_api_key", "k")
        mdb.set_setting(self.db, "data_dir", tempfile.mkdtemp())
        self.db.add(mdb.DownloadRequest(tmdb_id=70, media_type="series", user_id=2))
        self.db.commit()
        activity.invalidate_wanted_cache()
        self.addCleanup(activity.invalidate_wanted_cache)
        self.app = FastAPI()
        self.app.include_router(activity.router)
        self.app.dependency_overrides[get_db] = lambda: self.db
        self.user = self.db.query(TentacleUser).get(1)
        self.app.dependency_overrides[get_user_from_request] = lambda: self.user
        for p in (mock.patch("requests.get", side_effect=fake_get),
                  mock.patch("requests.post", return_value=_R({}))):
            p.start()
        self.addCleanup(mock.patch.stopall)

    def get(self):
        return TestClient(self.app, raise_server_exceptions=False).get("/api/activity")

    def test_admin_gets_200_with_the_series_searching(self):
        r = self.get()
        self.assertEqual(200, r.status_code, r.text[:500])
        titles = [x["title"] for x in r.json()["searching"]]
        self.assertIn("Long Runner", titles)
        self.assertNotIn("_missing_ids", r.json()["searching"][titles.index("Long Runner")])

    def test_the_card_is_remembered_for_stop_looking(self):
        self.assertEqual(200, self.get().status_code)
        self.assertEqual({701, 702, 703}, self.activity._missing_ids_for(self.db, {"id": 7}))

    def test_a_requester_sees_their_own_series(self):
        from models.database import TentacleUser
        self.user = self.db.query(TentacleUser).get(2)
        r = self.get()
        self.assertEqual(200, r.status_code, r.text[:500])
        self.assertIn("Long Runner", [x["title"] for x in r.json()["searching"]])

    def test_a_second_request_uses_the_cache_and_still_answers(self):
        self.assertEqual(200, self.get().status_code)
        self.assertEqual(200, self.get().status_code)


if __name__ == "__main__":
    unittest.main()
