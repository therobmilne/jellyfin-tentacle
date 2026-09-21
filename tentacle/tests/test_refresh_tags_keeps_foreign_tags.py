"""Refresh Tags must replace Tentacle's tags, not everybody's.

Run from the tentacle/ directory:  python -m unittest discover -s tests

POST /api/sync/refresh-tags wrote `movie.tags` as the item's WHOLE tag list.
Every other tag path merges; this one replaces, because it is the only way a
stale Tentacle tag (a deleted list, "Recently Added" expiring) ever comes off.
But it could not tell its own tags from anyone else's: a tag added by hand in
Jellyfin, or imported as a TMDB keyword, was wiped from every item Tentacle
tags (seen live, #107). Tentacle now remembers every tag it has managed and
replaces only those.
"""
import tempfile
import unittest
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PureMerge(unittest.TestCase):
    def merge(self, existing, desired, managed):
        from services.jellyfin import merge_managed_tags
        return merge_managed_tags(existing, desired, managed)

    def test_a_foreign_tag_survives(self):
        self.assertEqual({"Netflix", "date-night"},
                         set(self.merge(["Netflix", "date-night"], ["Netflix"], {"Netflix"})))

    def test_a_stale_managed_tag_is_removed(self):
        self.assertEqual({"Netflix"},
                         set(self.merge(["Netflix", "Recently Added"], ["Netflix"], {"Netflix", "Recently Added"})))

    def test_matching_is_case_insensitive_like_jellyfin(self):
        self.assertEqual({"Netflix"},
                         set(self.merge(["recently added"], ["Netflix"], {"Recently Added", "Netflix"})))

    def test_desired_tags_keep_tentacle_s_spelling_and_are_not_duplicated(self):
        self.assertEqual(["Netflix"], self.merge(["netflix"], ["Netflix"], {"Netflix"}))


class ManagedTagsAreRemembered(unittest.TestCase):
    def setUp(self):
        import models.database as mdb
        self.mdb = mdb
        engine = create_engine(f"sqlite:///{tempfile.mkdtemp()}/t.db", connect_args={"check_same_thread": False})
        mdb.Base.metadata.create_all(engine)
        self.db = sessionmaker(bind=engine)()
        self.addCleanup(self.db.close)

    def test_a_tag_is_still_known_after_the_last_title_lost_it(self):
        from services.jellyfin import managed_tags
        m = self.mdb.Movie(tmdb_id=1, title="A", source="radarr", tags=["My List", "Radarr"])
        self.db.add(m)
        self.db.commit()
        self.assertEqual({"My List", "Radarr"}, managed_tags(self.db))
        m.tags = ["Radarr"]                                   # the list was deleted
        self.db.commit()
        self.assertIn("My List", managed_tags(self.db),
                      "forgotten, so Refresh Tags would now treat it as a user's tag and never remove it")


class RefreshTagsEndpoint(unittest.TestCase):
    def setUp(self):
        import models.database as mdb
        self.mdb = mdb
        engine = create_engine(f"sqlite:///{tempfile.mkdtemp()}/t.db", connect_args={"check_same_thread": False})
        mdb.Base.metadata.create_all(engine)
        self.db = sessionmaker(bind=engine)()
        self.addCleanup(self.db.close)
        for k, v in (("jellyfin_url", "http://jf:8096"), ("jellyfin_api_key", "k")):
            mdb.set_setting(self.db, k, v)
        self.db.add(mdb.Movie(tmdb_id=10, title="Film", year="2000", source="radarr", tags=["Radarr", "Old List"]))
        self.db.commit()
        from services.jellyfin import managed_tags
        managed_tags(self.db)                                  # "Old List" has been pushed at some point
        self.db.query(mdb.Movie).one().tags = ["Radarr"]       # ...and the list is gone now
        self.db.commit()
        self.writes = []

    def _refresh(self, jf_tags):
        from routers import sync as sync_router
        outer = self

        class FakeJf:
            def __init__(self, *a, **k):
                pass

            def get_tmdb_lookup_with_fallback(self, media_type="Movie"):
                if media_type == "Movie":
                    return {10: {"Id": "jf10", "Tags": list(jf_tags)}}, {}
                return {}, {}

            @staticmethod
            def _normalize_title(t):
                return t

            def set_item_tags(self, item_id, tags):
                outer.writes.append((item_id, list(tags)))
                return True

        with mock.patch("services.jellyfin.JellyfinService", FakeJf), \
                mock.patch.object(sync_router, "refresh_recently_added_tags", lambda db: (0, 0)):
            return sync_router.refresh_tags(db=self.db)

    def test_a_hand_made_tag_survives_and_a_stale_tentacle_tag_goes(self):
        self._refresh(["Radarr", "Old List", "date-night", "youtube"])
        self.assertEqual(1, len(self.writes))
        self.assertEqual({"Radarr", "date-night", "youtube"}, set(self.writes[0][1]))

    def test_nothing_is_written_when_nothing_would_change(self):
        self._refresh(["Radarr", "date-night"])
        self.assertEqual([], self.writes, "a POST per item per click, for nothing")


if __name__ == "__main__":
    unittest.main()
