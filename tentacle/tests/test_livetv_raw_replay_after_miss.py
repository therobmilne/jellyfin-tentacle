"""After a re-dial that could not be joined, a ping-pong goes back to joining.

Run from the tentacle/ directory:  python -m unittest discover -s tests

No join is allowed within _SPLICE_AFTER_GAP of a gap: a later, longer replay
could reach back over it and drop seconds the stream never had. But every miss
restarted that window -- also a re-dial too soon after a gap (itself a miss)
and a connection that broke while its replay was held, neither of which skips
anything. On an account that re-dials every ~13 s the window then never ran
out: the first miss of any kind made the rest of the recording repeat ~22 s
per reconnect, as before the splice. Now only a miss that may have skipped
something restarts it, and a join never reaches over one.
"""
import asyncio
import random
import unittest
from collections import Counter
from unittest.mock import patch

import httpx

from test_livetv_raw_reconnect import _Body, _dropped, _play
from test_livetv_open_single_fetch import PANEL, TOKENIZED, _redirect, _resp
from test_livetv_raw_replay_splice import numbers, run, script

RATE = 10                   # video packets per second of media in these streams
TOO_SOON = "too soon after an earlier gap to join safely"
BROKE = "the connection broke before the join point"


def deliver(sp, data, size=5000):
    """One connection through the splicer the way the raw stream generator
    drives it: feed() -> whole packets out -> sent(); broke() at its end."""
    out, pending = bytearray(), b""
    for i in range(0, len(data), size):
        pending += sp.feed(data[i:i + size])
        if len(pending) >= 131072:
            cut = len(pending) - len(pending) % 188
            sp.sent(pending[:cut])
            out += pending[:cut]
            pending = pending[cut:]
    pending += sp.broke()
    cut = len(pending) - len(pending) % 188
    sp.sent(pending[:cut])
    out += pending[:cut]
    return bytes(out)


class Stream:
    """A splicer on a clock that the test moves, and what went downstream."""

    def __init__(self, first):
        import routers.livetv as livetv
        self.now = 0.0
        self.why = []
        self.sp = livetv._ReplaySplicer(lambda: self.now, self.why.append)
        self.out = bytearray(deliver(self.sp, run(*first)))

    def redial(self, t, a, b):
        """A re-dialled connection opened at t delivering video packets a..b-1."""
        self.now = t
        joined = self.sp.splices
        self.sp.redialled()
        self.out += deliver(self.sp, run(a, b))
        return "joined" if self.sp.splices > joined else "missed"


def pingpong(s, t, until, period=13.0, outage=1.5, replay=220, broken=()):
    """Re-dials every `period` s; each connection starts `replay` packets (22 s)
    behind live. The k-th in `broken` dies while its replay arrives."""
    results = []
    while t < until:
        t += outage
        start = int(t * RATE) - replay
        end = int((t + period - outage) * RATE)
        if len(results) in broken:
            end = start + 100           # killed after 10 s of the 22 s replay
        results.append((t, s.redial(t, start, end)))
        t = t + 2.0 if len(results) - 1 in broken else t + period - outage
    return results


class JoiningResumes(unittest.TestCase):
    def test_after_a_real_gap_a_pingpong_joins_again_once_the_window_has_passed(self):
        s = Stream((0, 400))
        # a 40 s outage, longer than the provider's 22 s buffer: a real gap
        self.assertEqual("missed", s.redial(80.0, 580, 915))
        results = pingpong(s, 91.5, 400.0)
        within = [r for t, r in results if t < 80.0 + 120.0]
        after = [r for t, r in results if t >= 80.0 + 120.0]
        self.assertEqual(["missed"] * len(within), within, "no join may come within 120 s of a gap")
        self.assertTrue(len(after) >= 10)
        self.assertEqual(["joined"] * len(after), after,
                         "a re-dial too soon after a gap restarted the window: the ping-pong never joined again")
        got = numbers(bytes(s.out))
        last = results[-1][0]
        self.assertEqual(set(), set(range(580, int((last + 11.5) * RATE))) - set(got), "content lost")
        tail = got[got.index(int((200.0 + 13.0) * RATE)):]
        self.assertEqual(sorted(set(tail)), tail, "joined re-dials repeated content")

    def test_a_connection_that_breaks_during_its_replay_does_not_stop_joining(self):
        s = Stream((0, 400))
        results = pingpong(s, 40.0, 300.0, broken=(3,))
        self.assertEqual(["joined"] * 3 + ["missed"] + ["joined"] * (len(results) - 4),
                         [r for _, r in results], "a broken hold restarted the window")
        self.assertEqual(1, s.why.count(BROKE))
        counts = Counter(numbers(bytes(s.out)))
        broke_at = results[3][0]
        start = int(broke_at * RATE) - 220
        self.assertEqual(set(range(start, start + 100)), {n for n, c in counts.items() if c > 1},
                         "only what the broken connection held may go out twice")
        self.assertEqual(set(range(int((results[-1][0] + 11.5) * RATE))), set(counts), "content lost")

    def test_a_redial_too_soon_after_a_gap_passes_its_bytes_straight_through(self):
        s = Stream((0, 1000))
        s.now = 10.0
        s.sp.redialled()
        deliver(s.sp, run(1200, 1500))                   # a real gap: the window starts
        s.now = 20.0
        s.sp.redialled()
        fresh = run(1400, 1800)
        for i in range(0, len(fresh), 3000):
            piece = fresh[i:i + 3000]
            self.assertEqual(piece, s.sp.feed(piece), "a re-dial that cannot be joined was held back")
        self.assertEqual(b"", s.sp.broke())
        self.assertEqual((0, 2), (s.sp.splices, s.sp.misses))
        self.assertEqual(10.0, s.sp._last_gap, "it carried the drop point: nothing skipped, the window stays")

    def test_a_too_soon_redial_is_watched_within_bounds_only(self):
        """What a too-soon re-dial keeps to look for the drop point is bounded;
        past the bound the window starts again (it may have skipped something)."""
        import routers.livetv as livetv
        s = Stream((0, 1000))
        s.redial(10.0, 1200, 1500)                       # a real gap at 10 s
        s.now = 20.0
        s.sp.redialled()
        with patch.object(livetv, "_SPLICE_MAX_BYTES", 50_000):
            fresh = run(1500, 2500)                      # no drop point in it
            for i in range(0, len(fresh), 5000):
                s.sp.feed(fresh[i:i + 5000])
                self.assertLessEqual(len(s.sp._held or b""), 55_000)
            self.assertIsNone(s.sp._held, "a watch past its bound was still kept")
        self.assertEqual(20.0, s.sp._last_gap)


class TheWindowStillGuards(unittest.TestCase):
    """A miss that may have skipped something still starts the window. These
    run with a realistic tail (the context check covers a fraction of a
    second, a replay ~22 s), where only the window keeps a later, longer
    replay from reaching back over a gap."""

    def setUp(self):
        import routers.livetv as livetv
        p = patch.object(livetv, "_SPLICE_ANCHOR_MAX", 16)
        p.start()
        self.addCleanup(p.stop)

    def test_a_redial_too_soon_after_a_gap_that_is_a_gap_itself_starts_the_window_again(self):
        s = Stream((0, 3000))
        s.redial(10.0, 3500, 3700)              # gap 1: 3000..3499 never came
        s.redial(100.0, 4000, 4200)             # too soon, and a gap: 3700..3999 never came
        # 125 s after gap 1, 35 s after gap 2: a replay reaching back over gap 2
        self.assertEqual("missed", s.redial(135.0, 3650, 4400))
        self.assertEqual(set(), set(range(3650, 4400)) - set(numbers(bytes(s.out))),
                         "a join reached back over a gap: packets the stream never had were dropped")

    def test_a_connection_that_breaks_before_it_shows_what_it_is_cannot_hide_a_gap(self):
        s = Stream((0, 3000))
        s.redial(10.0, 3500, 3520)              # a real gap, broken before the probe could tell
        s.redial(12.0, 3505, 4500)              # starts inside what the broken connection sent
        s.redial(25.0, 2900, 4700)              # a longer replay, reaching back over the gap
        self.assertEqual(set(), set(range(2900, 4700)) - set(numbers(bytes(s.out))),
                         "a join reached back over a gap: packets the stream never had were dropped")

    def test_a_join_after_a_broken_hold_goes_on_from_the_drop_before_it(self):
        s = Stream((0, 3000))
        self.assertEqual("missed", s.redial(10.0, 3500, 3520))   # broken: sent, but set aside
        self.assertEqual("joined", s.redial(12.0, 2950, 3000))   # ends right at the drop
        self.assertEqual("joined", s.redial(14.0, 2900, 4000))
        got = numbers(bytes(s.out))
        self.assertEqual(set(), set(range(2900, 4000)) - set(got),
                         "a join reached back over a gap: packets the stream never had were dropped")
        self.assertEqual(list(range(3000, 4000)), got[-1000:])


class ThroughTheStream(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import routers.livetv as livetv
        livetv._recent_streams.clear()

    async def test_a_replay_that_breaks_is_followed_by_a_join(self):
        import routers.livetv as livetv
        with self.assertLogs("routers.livetv", "WARNING") as logs:
            body, *_ = await _play(script([run(0, 1000), run(700, 900), run(800, 1500), run(1400, 2000)]))
        self.assertEqual(list(range(1000)) + list(range(700, 900)) + list(range(1000, 2000)), numbers(body))
        last = livetv._recent_streams[-1]
        self.assertEqual((3, 2, 1), (last["reconnects"], last["splices"], last["splice_misses"]))
        self.assertEqual(1, sum(BROKE in m for m in logs.output))
        self.assertEqual(0, sum(TOO_SOON in m for m in logs.output))

    async def test_joining_resumes_once_the_window_after_a_gap_has_passed(self):
        """Through the raw stream generator, on a clock each connection moves
        to its opening time: a real gap, then re-dials every 13 s."""
        import routers.livetv as livetv
        from test_livetv_open_single_fetch import FakeClient
        now = [0.0]

        class At(_Body):
            def __init__(self, t, pieces, then):
                super().__init__(pieces, then)
                self._t = t

            async def __aiter__(self):
                now[0] = self._t
                async for piece in super().__aiter__():
                    yield piece

        def conn(t, a, b, last=False):
            data = run(a, b)
            return httpx.Response(200, headers={"content-type": "video/mp2t"},
                                  stream=At(t, [data[i:i + 5000] for i in range(0, len(data), 5000)],
                                            None if last else _dropped()),
                                  request=httpx.Request("GET", TOKENIZED))
        conns, t = [conn(0.0, 0, 400), conn(80.0, 580, 915)], 91.5
        opened = []
        while t < 330.0:
            t += 1.5
            opened.append(t)
            conns.append((t, int(t * RATE) - 220, int((t + 11.5) * RATE)))
            t += 11.5
        conns[2:] = [conn(o, a, b, i == len(conns) - 3) for i, (o, a, b) in enumerate(conns[2:])]
        s = {PANEL: [_redirect()] * len(conns) + [_resp(404, PANEL)], TOKENIZED: conns}
        real_init = livetv._ReplaySplicer.__init__

        def init(self, clock, on_gap=None):
            real_init(self, lambda: now[0], on_gap)
        real_sleep = asyncio.sleep

        async def fast_sleep(delay):
            await real_sleep(0)
        with patch.object(livetv._ReplaySplicer, "__init__", init), \
                patch("httpx.AsyncClient", lambda **kw: FakeClient(s, [], [], **kw)), \
                patch("routers.livetv.is_safe_url", lambda *a, **k: True), \
                patch("asyncio.sleep", fast_sleep), \
                self.assertLogs("routers.livetv", "WARNING") as logs:
            response = await livetv._stream_proxy_inner(
                channel_id=1, user_agent="T/1", stream_url=PANEL, _release_sem=lambda: None, guard=None)
            body = b"".join([p async for p in response.body_iterator])
        last = livetv._recent_streams[-1]
        too_soon = sum(1 for o in opened if o < 200.0)
        after = len(opened) - too_soon
        self.assertTrue(after >= 8)
        self.assertEqual((1 + len(opened), after, 1 + too_soon),
                         (last["reconnects"], last["splices"], last["splice_misses"]),
                         "after one gap the re-dials were never joined again")
        self.assertEqual(too_soon, sum(TOO_SOON in m for m in logs.output))
        got = numbers(body)
        self.assertEqual(set(), set(range(580, int((opened[-1] + 11.5) * RATE))) - set(got), "content lost")


class PingPongProperty(unittest.TestCase):
    """Random ping-pongs (re-dials 6-16 s apart, 15-30 s replays) with real
    gaps, connections that die during their replay, and too-soon re-dials,
    at a realistic tail length. After every connection:
    L  nothing is dropped that was not sent before (by packet number);
    D  a joined re-dial repeats nothing, except what a broken hold sent;
    W  a miss restarts the window only when it may have skipped something:
       never a too-soon re-dial that delivered the drop point, never a
       broken hold;
    R  joining resumes: a re-dial that delivers the drop point, more than
       _SPLICE_AFTER_GAP after the last miss that restarted the window, is
       joined."""

    SEEDS = 300

    def test_random_pingpongs(self):
        import routers.livetv as livetv
        for seed in range(self.SEEDS):
            rng = random.Random(seed)
            with patch.object(livetv, "_SPLICE_ANCHOR_MAX", rng.choice([16, 64])):
                self._one(seed, rng)

    def _one(self, seed, rng):
        first = rng.randint(300, 900)
        s = Stream((0, first))
        times = Counter(range(first))    # how often each packet went out where the stream is
        seq = list(range(first))         # (a broken hold's bytes are not where the stream is)
        t = first / RATE
        restarted = None                 # when the window last (re)started
        for k in range(rng.randint(15, 40)):
            pos = seq[-1]
            contig = next((i for i in range(1, len(seq)) if seq[-i - 1] != seq[-i] - 1), len(seq))
            if rng.random() < 0.12:
                outage = rng.uniform(25.0, 60.0)    # longer than any replay: a real gap
            else:
                outage = rng.uniform(0.5, 3.0)
            t += outage
            start = int(t * RATE) - rng.randint(150, 300)
            end = int((t + rng.uniform(6.0, 16.0)) * RATE)
            died = rng.random() < 0.15
            if died:
                end = rng.randint(start + 1, max(start + 1, min(end, pos)))  # dies during its replay
            # it carries the drop point, and what was sent just before it is one stretch
            carries = start <= pos - 12 and end > pos and contig >= 50
            joinable = (carries and len(run(start, pos + 1)) + 5000 < s.sp._bound()
                        and all(times[n] == 1 for n in range(start, start + 50)))
            gap_before, why_before = s.sp._last_gap, len(s.why)
            at = len(s.out)
            result = s.redial(t, start, end)
            out = numbers(bytes(s.out[at:]))
            why = s.why[why_before:]
            dropped = set(range(start, end)) - set(out)
            ctx = f"seed {seed} conn {k}: t {t:.1f} {start}..{end} pos {pos} {result} {why}"
            self.assertEqual(set(), dropped - set(times), f"{ctx}: dropped packets never sent")  # L
            if result == "joined":
                self.assertEqual(set(), set(out) & set(times), f"{ctx}: a joined re-dial repeated content")  # D
            moved = s.sp._last_gap != gap_before
            if why == [BROKE] or (why == [TOO_SOON] and carries):
                self.assertFalse(moved, f"{ctx}: the window restarted on a miss that skipped nothing")  # W
            if joinable and (restarted is None or t - restarted >= 120.0):
                self.assertEqual("joined", result, f"{ctx}: not joined")                         # R
            if moved:
                restarted = t
            if why != [BROKE]:
                times.update(out)
                seq += out
            t = t + 1.0 if died else end / RATE

if __name__ == "__main__":
    unittest.main()
