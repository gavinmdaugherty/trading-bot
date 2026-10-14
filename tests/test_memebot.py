"""Offline tests for memebot. Run:  python -m unittest discover -s tests"""
import base64
import hashlib
import http.client
import json
import os
import socket
import struct
import sys
import threading
import time
import unittest
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import memebot as m  # noqa: E402


def demo_engine(**kw):
    src = m.DemoSource()
    pump = m.DemoPump(m.CFG, clock=time.time)
    eng = m.Engine(m.CFG, m.AGENTS, src, demo=True, pump=pump, fomo=m.DemoFomo(src), **kw)
    pump.clock = lambda: eng.t
    return eng


class CurveMath(unittest.TestCase):
    def test_round_trip_returns_input(self):
        vsol, vtok = 45.0, 800e6
        out = m.curve_buy(2.0, vsol, vtok)
        back = m.curve_sell(out, vsol + 2.0, vtok - out)
        self.assertAlmostEqual(back, 2.0, places=9)

    def test_progress(self):
        self.assertEqual(m.curve_progress(m.PUMP_V_TOK0), 0.0)
        self.assertAlmostEqual(m.curve_progress(m.PUMP_V_TOK0 - m.PUMP_REAL_TOK0), 100.0)


class Watcher(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0
        self.w = m.PumpWatcher(m.CFG, clock=lambda: self.now)

    def create(self, mint="M1", dev="DEV", init=30e6):
        self.w.handle(dict(txType="create", mint=mint, traderPublicKey=dev, symbol="ABC", name="abc",
                           initialBuy=init, solAmount=1.0, vSolInBondingCurve=31.0,
                           vTokensInBondingCurve=m.PUMP_V_TOK0 - init, pool="pump"))

    def trade(self, side, trader, sol, bal, vsol, vtok, mint="M1"):
        self.w.handle(dict(txType=side, mint=mint, traderPublicKey=trader, solAmount=sol,
                           newTokenBalance=bal, vSolInBondingCurve=vsol, vTokensInBondingCurve=vtok,
                           pool="pump"))

    def test_create_trade_and_stats(self):
        self.create()
        self.assertEqual(self.w.sub_q, {"M1"})
        for i in range(10):
            self.now += 5
            self.trade("buy", f"W{i}", 1.0, 5e6, 32.0 + i, 1.0e9 - i * 5e6)
        self.trade("sell", "W0", 0.5, 0, 41.5, 0.96e9)
        q = self.w.get("M1")
        self.assertEqual((q["buys_5m"], q["sells_5m"], q["uniq_5m"], q["buyers"]), (10, 1, 10, 10))
        self.assertAlmostEqual(q["net_sol_5m"], 9.5)
        self.assertAlmostEqual(q["dev_pct"], 3.0)
        self.assertAlmostEqual(q["top_pct"], 0.5)
        self.assertFalse(q["dev_sold"])
        self.assertGreater(q["price"], 0)

    def test_dev_sell_and_migration(self):
        self.create()
        self.trade("sell", "DEV", 1.0, 0, 30.0, m.PUMP_V_TOK0)
        self.assertTrue(self.w.get("M1")["dev_sold"])
        self.assertEqual(self.w.pop_events(), [])  # nobody bought it: flagged, but not worth a log line
        self.w.handle(dict(mint="M1", txType="migrate", pool="pump-amm"))
        migs = self.w.pop_migrations()
        self.assertEqual([g["mint"] for g in migs], ["M1"])
        self.w.handle(dict(mint="M1", txType="migrate"))  # duplicate is ignored
        self.assertEqual(self.w.pop_migrations(), [])

    def test_ignores_untracked_and_amm_trades(self):
        self.trade("buy", "X", 1.0, 1.0, 40.0, 9e8, mint="NOPE")
        self.create()
        self.w.handle(dict(txType="buy", mint="M1", traderPublicKey="X", solAmount=5,
                           vSolInBondingCurve=99, vTokensInBondingCurve=1, pool="pump-amm"))
        self.assertEqual(self.w.get("M1")["vsol"], 31.0)
        self.assertIsNone(self.w.get("NOPE"))

    def test_prune_keeps_held(self):
        self.create("A")
        self.create("B")
        self.now += m.CFG["pump_watch_secs"] + 1
        self.w.prune(self.now, keep={"A"})
        self.assertEqual(set(self.w.tokens), {"A"})
        self.assertIn("B", self.w.unsub_q)


class FomoParsing(unittest.TestCase):
    chains = ["solana", "base", "ethereum", "bsc"]

    def test_shapes(self):
        sol = "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
        d = {"data": [{"rank": 1, "network": "solana", "address": sol, "symbol": "POPCAT"},
                      {"rank": 2, "token": {"chain": "Base", "tokenAddress": "0xAbC"}, "symbol": "B"},
                      {"rank": 3, "network": "monad", "address": "0xdef"},
                      {"rank": 4, "mint": sol[::-1]}]}
        out = m.parse_fomo(d, self.chains)
        self.assertEqual([(o["chain"], o["rank"]) for o in out], [("solana", 1), ("base", 2), ("solana", 4)])
        self.assertEqual(m.parse_fomo([{"address": sol, "chain": "sol"}], self.chains)[0]["rank"], 1)
        self.assertEqual(m.parse_fomo({"data": {"tokens": [{"address": sol, "network": "solana"}]}},
                                      self.chains)[0]["addr"], sol)
        self.assertEqual(m.parse_fomo({"error": "unauthorized"}, self.chains), [])


class WebSocketClient(unittest.TestCase):
    """A tiny RFC 6455 server that behaves like PumpPortal."""

    def test_handshake_frames_ping_and_subscribe(self):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        got = []

        def frame(op, data, fin=True):
            n = len(data)
            head = bytes([(0x80 if fin else 0) | op])
            head += bytes([n]) if n < 126 else (bytes([126]) + struct.pack(">H", n))
            return head + data

        def read_frame(c, buf):
            while len(buf) < 2:
                buf += c.recv(4096)
            n = buf[1] & 0x7F
            off = 2
            if n == 126:
                while len(buf) < 4:
                    buf += c.recv(4096)
                n, off = struct.unpack(">H", buf[2:4])[0], 4
            while len(buf) < off + 4 + n:
                buf += c.recv(4096)
            mask, data = buf[off:off + 4], buf[off + 4:off + 4 + n]
            return buf[0] & 0x0F, bytes(b ^ mask[i % 4] for i, b in enumerate(data)), buf[off + 4 + n:]

        def serve():
            c, _ = srv.accept()
            req = b""
            while b"\r\n\r\n" not in req:
                req += c.recv(4096)
            key = [ln.split(b":", 1)[1].strip() for ln in req.split(b"\r\n")
                   if ln.lower().startswith(b"sec-websocket-key")][0]
            acc = base64.b64encode(hashlib.sha1(key + m.WebSocket.GUID.encode()).digest())
            c.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      b"Sec-WebSocket-Accept: " + acc + b"\r\n\r\n")
            buf = b""
            op, data, buf = read_frame(c, buf)
            got.append((op, json.loads(data)))
            c.sendall(frame(0x9, b"hi"))  # ping: the client must pong and keep going
            big = json.dumps({"txType": "create", "mint": "X" * 300}).encode()
            c.sendall(frame(0x1, big[:100], fin=False) + frame(0x0, big[100:]))  # fragmented
            op, data, buf = read_frame(c, buf)
            got.append((op, data))
            c.sendall(frame(0x8, b""))
            c.close()

        th = threading.Thread(target=serve, daemon=True)
        th.start()
        old = {k: os.environ.pop(k, None) for k in ("HTTP_PROXY", "http_proxy")}
        try:
            ws = m.WebSocket(f"ws://127.0.0.1:{port}/api/data", timeout=5)
            ws.connect()
            ws.send(json.dumps({"method": "subscribeNewToken"}))
            msg = json.loads(ws.recv())
            self.assertEqual(msg["mint"], "X" * 300)
            with self.assertRaises(ConnectionError):
                ws.recv()
            ws.close()
        finally:
            for k, v in old.items():
                if v is not None:
                    os.environ[k] = v
        th.join(5)
        srv.close()
        self.assertEqual(got[0], (0x1, {"method": "subscribeNewToken"}))
        self.assertEqual(got[1], (0xA, b"hi"))


class EngineDemo(unittest.TestCase):
    def test_demo_run_trades_on_every_feed(self):
        eng = demo_engine()
        for _ in range(120):
            eng.cycle()
        s = eng.snapshot()
        self.assertEqual(eng.errors, 0, [l for l in eng.state["log"] if l["tag"] == "WARN"])
        srcs = {r["src"] for st in eng.state["agents"].values() for r in st["trades"]}
        self.assertTrue({"pump", "dex"} <= srcs, srcs)
        self.assertGreater(s["pump"]["seen"], 50)
        self.assertTrue(s["fomo"]["board"])
        for st in eng.state["agents"].values():
            self.assertGreaterEqual(st["cash"], -1e-9)
        # the books balance: equity = start + realised P&L + unrealised
        for name, st in eng.state["agents"].items():
            unreal = sum(eng._value(p) - p["size"] for p in st["positions"].values())
            self.assertAlmostEqual(eng._equity(st), st["start"] + st["pnl"] + unreal, places=6)

    def test_partial_take_profit_accounting(self):
        eng = demo_engine()
        st = eng.state["agents"]["Atlas"]
        st["cash"] -= 20
        k = m.tkey("base", "0xabc")
        st["positions"][k] = dict(chain="base", addr="0xabc", symbol="T", units=1000.0, size=20.0, size0=20.0,
                                  ref_price=0.02, peak=0.02, opened=eng.t, entry_liq=1e6, sell_tax=0.0,
                                  last_price=0.02, last_liq=1e6, miss=0, venue="dex", src="dex")
        pos = st["positions"][k]
        pos.update(last_price=0.025, peak=0.025)  # +25%: past Atlas' tp1 of 20%
        eng._check_exit("Atlas", k, pos, 0.025, liq=1e6)
        self.assertTrue(pos["tp1_done"])
        self.assertAlmostEqual(pos["units"], 500.0)
        self.assertAlmostEqual(pos["size"], 10.0)
        pos.update(last_price=0.0199)  # falls back below entry: break-even stop closes the rest
        eng._check_exit("Atlas", k, pos, 0.0199, liq=1e6)
        self.assertNotIn(k, st["positions"])
        rows = st["trades"][-2:]
        self.assertEqual([r["reason"] for r in rows], ["partial take-profit", "break-even stop"])
        self.assertEqual(st["wins"], 1)  # a winner overall
        self.assertAlmostEqual(st["pnl"], sum(r["pnl"] for r in rows), places=1)
        self.assertAlmostEqual(st["cash"], 200 + st["pnl"], places=6)

    def test_curve_position_moves_to_dex_on_graduation(self):
        eng = demo_engine()
        w = eng.pump
        w.handle(dict(txType="create", mint="G1", traderPublicKey="D", symbol="G", initialBuy=0,
                      vSolInBondingCurve=30.0, vTokensInBondingCurve=m.PUMP_V_TOK0, pool="pump"))
        q = dict(w.get("G1"), vsol=80.0, vtok=400e6, price=80 / 400e6 * eng.sol_usd, prog=80, mcap=1,
                 buyers=0, net_sol_5m=0)
        self.assertTrue(eng._buy_curve("Crest", q))
        k = m.tkey("solana", "G1")
        w.handle(dict(mint="G1", txType="migrate"))
        eng.fast()
        pos = eng.state["agents"]["Crest"]["positions"][k]
        self.assertEqual(pos["venue"], "dex")
        self.assertIn("grad", eng.universe[k]["tags"])


class RugChecks(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0
        self.w = m.PumpWatcher(m.CFG, clock=lambda: self.now)

    def create(self, mint, dev="DEV", sym="ABC", vtok=m.PUMP_V_TOK0):
        self.w.handle(dict(txType="create", mint=mint, traderPublicKey=dev, symbol=sym, initialBuy=0,
                           vSolInBondingCurve=30.0, vTokensInBondingCurve=vtok, uri="https://ipfs.io/ipfs/x"))

    def buy(self, mint, w, sol, amt, vtok):
        self.w.handle(dict(txType="buy", mint=mint, traderPublicKey=w, solAmount=sol, tokenAmount=amt,
                           newTokenBalance=amt, vSolInBondingCurve=40.0, vTokensInBondingCurve=vtok))

    def test_unseen_launch_block_buys_count_as_bundle(self):
        self.create("B")
        # 80M tokens left the curve before our first trade: 30M in that trade, 50M unseen
        self.now += 4
        self.buy("B", "W1", 1.0, 30e6, m.PUMP_V_TOK0 - 80e6)
        q = self.w.get("B")
        self.assertAlmostEqual(q["bundle_pct"], 5.0)
        self.assertAlmostEqual(q["sniper_pct"], 3.0)

    def test_serial_creator_copycat_and_bot_buys(self):
        self.create("A1", dev="RUG", sym="CAT")
        self.now += 10
        self.create("A2", dev="RUG", sym="cat")
        q1, q2 = self.w.get("A1"), self.w.get("A2")
        self.assertEqual((q1["creator_launches"], q2["creator_launches"]), (0, 1))
        self.assertEqual((q1["copycat"], q2["copycat"]), (False, True))
        vt = m.PUMP_V_TOK0
        for i in range(8):
            vt -= 1e6
            self.buy("A1", f"W{i}", 0.137, 1e6, vt)
        q = self.w.get("A1")
        self.assertEqual(q["same_amt"], 1.0)  # every buy exactly 0.137 SOL: a bot farm
        self.buy("A1", "WHALE", 12.0, 5e6, vt - 5e6)
        self.assertGreater(self.w.get("A1")["whale"], 0.7)

    def test_metadata_and_reused_links(self):
        self.create("S1")
        self.create("S2")
        self.w._set_meta("S1", dict(twitter="https://x.com/elonmusk/", telegram="", website=""))
        self.assertEqual(self.w.get("S1")["meta_state"], 2)
        self.assertEqual(self.w.get("S1")["socials"], 1)
        self.assertFalse(self.w.get("S1")["dup_social"])
        self.w._set_meta("S2", dict(twitter="https://X.com/elonmusk", telegram="", website=""))
        self.assertTrue(self.w.get("S1")["dup_social"] and self.w.get("S2")["dup_social"])

    def test_meta_queued_after_enough_buys(self):
        self.create("Q")
        vt = m.PUMP_V_TOK0
        for i in range(m.CFG["meta_min_buys"]):
            vt -= 1e6
            self.buy("Q", f"W{i}", 0.1 + i / 100, 1e6, vt)
        self.assertEqual(list(self.w.meta_q), ["Q"])


class RiskPresets(unittest.TestCase):
    def test_presets_loosen_spark_and_keep_rug_checks(self):
        import copy
        for lvl in ("medium", "high"):
            spark = next(a for a in m.apply_risk(copy.deepcopy(m.AGENTS), lvl) if a["name"] == "Spark")
            base = next(a for a in m.AGENTS if a["name"] == "Spark")
            self.assertLess(spark["min_prog"], base["min_prog"])
            self.assertTrue(spark["exit_dev_sell"] and spark["skip_copycat"])
            self.assertLessEqual(spark["max_bundle_pct"], 20)

    def test_quick_buy_buttons_are_not_bot_buys(self):
        self.assertTrue(m.is_preset_buy(0.1) and m.is_preset_buy(0.099) and m.is_preset_buy(1.0))
        self.assertFalse(m.is_preset_buy(0.137))
        w = m.PumpWatcher(m.CFG, clock=lambda: 0.0)
        w.handle(dict(txType="create", mint="P", traderPublicKey="d", symbol="P", initialBuy=0,
                      vSolInBondingCurve=30.0, vTokensInBondingCurve=m.PUMP_V_TOK0))
        vt = m.PUMP_V_TOK0
        for i in range(10):
            vt -= 1e6
            w.handle(dict(txType="buy", mint="P", traderPublicKey=f"W{i}", solAmount=0.1, tokenAmount=1e6,
                          vSolInBondingCurve=30.0 + i / 10, vTokensInBondingCurve=vt))
        self.assertEqual(w.get("P")["same_amt"], 0.0)


class Learning(unittest.TestCase):
    @staticmethod
    def q(**kw):
        q = dict(age=60, prog=20, real_sol=8, buyers=40, uniq_5m=20, buys_5m=40, sells_5m=20, net_sol_5m=5,
                 buys_60s=8, holders=40, top_pct=3, top10_pct=20, bundle_pct=2, sniper_pct=5, dev_pct=1,
                 whale=0.2, same_amt=0.1, ath_drop=-5, socials=1, mcap=8000)
        q.update(kw)
        return q

    def test_learns_a_pattern_and_proves_it_out_of_sample(self):
        import random, tempfile
        rnd = random.Random(1)
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "learn.json")
            L = m.Learner(path, min_n=200, proof_n=30)
            for i in range(1500):
                net = rnd.uniform(-8, 12)          # the hidden truth: strong inflow wins
                x = m.Learner.features(self.q(net_sol_5m=net, buyers=rnd.randint(5, 200)))
                picked = L.ready() and L.ev(x) >= 0.03
                L.learn(x, 0.4 if net > 4 else -0.25, picked)
            good = L.ev(m.Learner.features(self.q(net_sol_5m=10)))
            bad = L.ev(m.Learner.features(self.q(net_sol_5m=-6)))
            self.assertGreater(good, 0.03)
            self.assertLess(bad, 0.0)
            self.assertTrue(L.proven())
            L.save()
            L2 = m.Learner(path, min_n=200, proof_n=30)       # survives a restart
            self.assertEqual(L2.n, 1500)
            self.assertAlmostEqual(L2.ev(m.Learner.features(self.q(net_sol_5m=10))), good)

    def test_a_losing_model_never_gets_proven(self):
        L = m.Learner(None, min_n=50, proof_n=10)
        for i in range(400):
            L.learn(m.Learner.features(self.q()), -0.1, True)
        self.assertTrue(L.ready())
        self.assertFalse(L.proven())


class FakeLive:
    def __init__(self):
        self.calls = []

    def buy(self, mint, usd, ref_price, slippage_bps=None, max_dev_pct=None):
        self.calls.append(("buy", mint, usd, slippage_bps, max_dev_pct))
        return dict(ok=True, spent_usd=usd, raw=int(usd / ref_price * 1e6), decimals=6, sig="SIG")

    def sell(self, mint, raw):
        self.calls.append(("sell", mint, raw))
        return dict(ok=True, got_usd=6.0, sig="SIG2", sold_raw=raw)

    def close_empty(self, mint):
        self.calls.append(("close", mint))
        return "SIG3"

    def wallet(self):
        return dict(pub="x", usdc=15.0, sol=0.05)


class LiveSpark(unittest.TestCase):
    def setUp(self):
        self.cfg = dict(m.CFG, live_usd=15.0)
        self.live = FakeLive()
        src = m.DemoSource()
        pump = m.DemoPump(self.cfg, clock=time.time)
        self.eng = m.Engine(self.cfg, m.AGENTS, src, demo=True, pump=pump, live=self.live,
                            live_names=["Spark"])
        pump.clock = lambda: self.eng.t
        self.eng.wallet = self.live.wallet()

    def q(self, **kw):
        q = dict(addr="MINT1", symbol="GO", price=50 / 900e6 * self.eng.sol_usd, vsol=50.0, vtok=900e6,
                 mcap=1e4, prog=20.0, buyers=40, net_sol_5m=6.0)
        q.update(kw)
        return q

    def test_live_curve_buy_uses_curve_tolerances_and_closes_account(self):
        self.assertIn("Spark LIVE", self.eng.acfg)
        self.assertTrue(self.eng._buy_curve("Spark LIVE", self.q()))
        kind, mint, usd, bps, dev = self.live.calls[0]
        self.assertEqual((kind, mint, usd), ("buy", "MINT1", 5.0))  # $15 bankroll: minimum $5 order
        self.assertEqual((bps, dev), (self.cfg["live_curve_slippage_bps"], self.cfg["live_curve_max_dev_pct"]))
        st = self.eng.state["agents"]["Spark LIVE"]
        k = m.tkey("solana", "MINT1")
        pos = st["positions"][k]
        self.assertTrue(pos["live"] and pos["venue"] == "curve")
        self.assertAlmostEqual(st["cash"], 10.0)
        # +35% is past tp1 (30%), but half of $5 is below the $5 minimum order: no partial sell
        self.eng._check_exit("Spark LIVE", k, pos, pos["ref_price"] * 1.35)
        self.assertFalse(pos.get("tp1_done"))
        self.eng._check_exit("Spark LIVE", k, pos, pos["ref_price"] * 0.7)  # stop-loss
        self.assertNotIn(k, st["positions"])
        self.assertEqual([c[0] for c in self.live.calls], ["buy", "sell", "close"])
        self.assertAlmostEqual(st["cash"], 16.0)

    def test_no_route_yet_retries_in_20s(self):
        self.live.buy = lambda *a, **k: dict(ok=False, stage="quote", err="no route")
        self.assertFalse(self.eng._buy_curve("Spark LIVE", self.q()))
        k = m.tkey("solana", "MINT1")
        self.assertEqual(self.eng._slot_reason("Spark LIVE", k, 5), "cooldown")
        self.eng.t += 21
        self.assertIsNone(self.eng._slot_reason("Spark LIVE", k, 5))
        self.assertFalse(self.eng.live_halt)

    def test_daily_cap_off_by_default_and_optional(self):
        st = self.eng.state["agents"]["Spark LIVE"]
        st["day_pnl"], st["capped"] = -14.0, True
        self.eng._exits()
        self.assertFalse(st["capped"])  # default 0: no daily cap, even with a saved flag
        self.eng.cfg["daily_loss_pct"] = 15.0
        self.eng._exits()
        self.assertTrue(st["capped"])
        self.assertEqual(self.eng._slot_reason("Spark LIVE", "x", 5), "daily loss cap hit")

    def test_degen_profit_lock_at_1_3x(self):
        a = self.eng.acfg["Spark LIVE"]
        a.update(m.RISK["degen"])
        self.assertTrue(self.eng._buy_curve("Spark LIVE", self.q()))
        k = m.tkey("solana", "MINT1")
        st = self.eng.state["agents"]["Spark LIVE"]
        pos = st["positions"][k]
        ref = pos["ref_price"]
        pos["peak"] = ref * 1.25
        self.eng._check_exit("Spark LIVE", k, pos, ref * 1.10)   # never reached +33%: no lock yet
        self.assertIn(k, st["positions"])
        pos["peak"] = ref * 1.36
        self.eng._check_exit("Spark LIVE", k, pos, ref * 1.32)   # above the floor: keep riding
        self.assertIn(k, st["positions"])
        self.eng._check_exit("Spark LIVE", k, pos, ref * 1.29)   # fell back to 1.3x: sell
        self.assertNotIn(k, st["positions"])
        self.assertEqual(st["trades"][-1]["reason"], "profit lock 1.3x")

    def test_live_usd_sets_the_bankroll_each_run(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            cfg = dict(self.cfg, state_file=os.path.join(d, "state.json"))
            def engine(usd):
                c = dict(cfg, live_usd=usd)
                pump = m.DemoPump(c, clock=time.time)
                e = m.Engine(c, m.AGENTS, m.DemoSource(), demo=False, pump=pump, live=self.live,
                             live_names=["Spark"], live_only=True)
                return e, e.state["agents"]["Spark LIVE"]
            e, st = engine(15.0)
            st["cash"], st["pnl"] = 4.99, -10.01   # an earlier run lost $10
            e._save()
            e, st = engine(25.0)                   # the owner topped up and starts with --live-usd 25
            self.assertEqual((st["cash"], st["start"]), (25.0, 25.0))
            self.assertEqual(st["pnl"], -10.01)     # history is kept
            self.assertTrue(any("bankroll set to $25.00" in l["msg"] for l in e.state["log"]))

    def test_pumpportal_paywall_notice_requests_onchain_feed(self):
        w = m.PumpWatcher(m.CFG, clock=lambda: 0.0)
        w.handle({"message": "'subscribeTokenTrade' and 'subscribeAccountTrade' now require an API key"})
        self.assertTrue(w.want_rpc)

    def test_live_spark_waits_for_learning_then_proof(self):
        q = dict(self.q(), migrated=False, complete=False, from_create=True, last_trade=self.eng.t,
                 age=120, dev_sold=False, dev_pct=1, dev_init_pct=1, creator_launches=0, bundle_pct=1,
                 sniper_pct=2, top_pct=2, top10_pct=15, copycat=False, dup_social=False, real_sol=20,
                 holders=60, uniq_5m=30, buys_5m=50, sells_5m=20, same_amt=0.1, whale=0.1, buys_60s=10,
                 ath_drop=-5, meta_state=2, socials=1)
        L = self.eng.learner
        self.assertIsNotNone(L)
        self.assertTrue(self.eng._why_not_curve("Spark LIVE", q).startswith("learning (0/"))
        x = m.Learner.features(q)
        for _ in range(L.min_n):
            L.learn(x, 0.3, False)
        self.assertEqual(self.eng._why_not_curve("Spark LIVE", q), "model not proven yet")
        for _ in range(L.proof_n):
            L.learn(x, 0.3, True)
        self.assertIsNone(self.eng._why_not_curve("Spark LIVE", q))
        self.assertEqual(self.eng._why_not_curve("Spark LIVE", dict(q, bundle_pct=60)), "bundled launch")

    def test_shadow_trades_open_and_teach(self):
        w = self.eng.pump
        w.handle(dict(txType="create", mint="SH", traderPublicKey="d", symbol="SH", initialBuy=0,
                      vSolInBondingCurve=40.0, vTokensInBondingCurve=800e6))
        q = dict(w.get("SH"), buyers=30, last_trade=self.eng.t, age=60)
        self.eng._open_shadows([q])
        self.assertEqual(len(self.eng.shadows), 1)
        self.eng._open_shadows([q])                          # not again within 45s
        self.assertEqual(len(self.eng.shadows), 1)
        w.handle(dict(txType="buy", mint="SH", traderPublicKey="b", solAmount=30, tokenAmount=1,
                      vSolInBondingCurve=80.0, vTokensInBondingCurve=400e6))  # price x4: take-profit
        self.eng._shadow_tick()
        self.assertEqual(len(self.eng.shadows), 0)
        self.assertEqual(self.eng.learner.n, 1)
        self.assertGreater(self.eng.learner.recent[-1][0], 0.5)

    def test_live_blocked_when_wallet_sol_low(self):
        self.eng.wallet = dict(pub="x", usdc=15.0, sol=0.001)
        self.assertEqual(self.eng._live_reason("Spark LIVE"), "wallet SOL too low for fees")
        self.eng.live_halt = True
        self.assertEqual(self.eng._live_reason("Spark LIVE"), "live trading halted")

    def test_why_not_reports_rug_reasons(self):
        base = dict(self.q(), migrated=False, complete=False, from_create=True, last_trade=self.eng.t,
                    age=120, dev_sold=False, dev_pct=1, dev_init_pct=1, creator_launches=0, bundle_pct=1,
                    sniper_pct=2, top_pct=2, top10_pct=15, copycat=False, dup_social=False, real_sol=20,
                    holders=60, uniq_5m=30, buys_5m=50, sells_5m=20, same_amt=0.1, whale=0.1, buys_60s=10,
                    ath_drop=-5, meta_state=2, socials=1)
        self.assertIsNone(self.eng._why_not_curve("Spark", base))
        for change, why in [(dict(bundle_pct=20), "bundled launch"), (dict(creator_launches=3), "serial launcher"),
                            (dict(same_amt=0.6), "bot-sized identical buys"), (dict(socials=0), "no socials"),
                            (dict(dup_social=True), "reused social links"), (dict(whale=0.8), "one whale driving it"),
                            (dict(meta_state=1), "checking socials"), (dict(dev_sold=True), "creator sold")]:
            self.assertEqual(self.eng._why_not_curve("Spark", dict(base, **change)), why)


class CloseEmpty(unittest.TestCase):
    def test_builds_close_instruction_for_empty_account_only(self):
        try:
            from solders.keypair import Keypair
            from solders.transaction import Transaction
        except ImportError:
            self.skipTest("solders not installed")
        kp = Keypair()
        ex = m.SolanaLive.__new__(m.SolanaLive)
        ex.kp, ex.pub, ex.cfg = kp, str(kp.pubkey()), m.CFG
        sent = []
        prog = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        empty, full = str(Keypair().pubkey()), str(Keypair().pubkey())

        def acc(pk, amount):
            return {"pubkey": pk, "account": {"owner": prog, "data": {"parsed": {"info": {
                "owner": ex.pub, "tokenAmount": {"amount": str(amount)}}}}}}

        def rpc(method, params):
            if method == "getTokenAccountsByOwner":
                return {"value": [acc(empty, 0), acc(full, 5)]}
            if method == "getLatestBlockhash":
                return {"value": {"blockhash": "11111111111111111111111111111111"}}
            sent.append(params[0])
            return "SIG"
        ex._rpc = rpc
        self.assertEqual(ex.close_empty("MintAAA"), "SIG")
        tx = Transaction.from_bytes(base64.b64decode(sent[0]))
        keys = [str(k) for k in tx.message.account_keys]
        self.assertIn(empty, keys)
        self.assertNotIn(full, keys)
        self.assertEqual(len(tx.message.instructions), 1)
        self.assertEqual(bytes(tx.message.instructions[0].data), bytes([9]))
        self.assertIsNone(ex.close_empty(m.SolanaLive.USDC))


class OnChainFeed(unittest.TestCase):
    """pump.fun Anchor events, encoded the way the program logs them."""

    @staticmethod
    def key(n):
        return bytes([n]) * 32

    def trade(self, mint, user, sol, tok, is_buy, vsol, vtok):
        return (m.RpcPumpFeed.TRADE + self.key(mint) + struct.pack("<QQB", int(sol * 1e9), int(tok * 1e6), is_buy)
                + self.key(user) + struct.pack("<qQQ", 0, int(vsol * 1e9), int(vtok * 1e6)) + bytes(64))

    def create(self, mint, user, sym):
        s = lambda x: struct.pack("<I", len(x)) + x.encode()
        return (m.RpcPumpFeed.CREATE + s("Name") + s(sym) + s("https://ipfs.io/ipfs/abc")
                + self.key(mint) + self.key(9) + self.key(user) + bytes(40))

    def note(self, sig, *events, err=None):
        logs = ["Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]"]
        logs += ["Program data: " + base64.b64encode(e).decode() for e in events]
        return {"params": {"result": {"value": {"signature": sig, "err": err, "logs": logs}}}}

    def test_base58(self):
        self.assertEqual(m.b58encode(bytes(32)), "1" * 32)
        self.assertEqual(m.b58encode(bytes([0, 0, 1])), "112")
        try:
            from solders.pubkey import Pubkey
        except ImportError:
            return
        raw = bytes(range(32))
        self.assertEqual(m.b58encode(raw), str(Pubkey.from_bytes(raw)))

    def test_create_trades_bundle_and_dedupe(self):
        now = [1000.0]
        w = m.PumpWatcher(m.CFG, clock=lambda: now[0])
        feed = m.RpcPumpFeed(w)
        mint, dev = m.b58encode(self.key(1)), m.b58encode(self.key(2))
        # launch transaction: create + the creator's buy + a bundled buy in the same transaction
        feed.on_notification(self.note("SIG1", self.create(1, 2, "MOON"),
                                       self.trade(1, 2, 1.0, 30e6, 1, 31.0, 1043e6),
                                       self.trade(1, 3, 2.0, 55e6, 1, 33.0, 988e6)))
        q = w.get(mint)
        self.assertEqual(q["symbol"], "MOON")
        self.assertAlmostEqual(q["dev_init_pct"], 3.0)
        self.assertAlmostEqual(q["bundle_pct"], 5.5)   # seen directly on-chain, in the launch block
        self.assertEqual((q["buyers"], q["n_buys"]), (1, 1))
        self.assertEqual(w.tokens[mint]["creator"], dev)
        # a failed transaction changes nothing
        feed.on_notification(self.note("SIG2", self.trade(1, 4, 5.0, 100e6, 1, 38.0, 888e6), err={"x": 1}))
        self.assertEqual(w.get(mint)["n_buys"], 1)
        # the same trade from PumpPortal and from the chain counts once
        now[0] += 30
        feed.on_notification(self.note("SIG3", self.trade(1, 5, 1.0, 20e6, 1, 34.0, 968e6)))
        w.handle(dict(txType="buy", mint=mint, traderPublicKey=m.b58encode(self.key(5)), solAmount=1.0,
                      tokenAmount=20e6, vSolInBondingCurve=34.0, vTokensInBondingCurve=968e6, signature="SIG3"))
        self.assertEqual(w.get(mint)["n_buys"], 2)
        # the creator selling is caught
        feed.on_notification(self.note("SIG4", self.trade(1, 2, 0.9, 30e6, 0, 33.1, 998e6)))
        self.assertTrue(w.get(mint)["dev_sold"])
        # graduation
        feed.on_notification(self.note("SIG5", m.RpcPumpFeed.COMPLETE + self.key(7) + self.key(1) + self.key(9) + bytes(8)))
        self.assertEqual([g["mint"] for g in w.pop_migrations()], [mint])

    def test_ignores_garbage(self):
        w = m.PumpWatcher(m.CFG, clock=lambda: 0.0)
        feed = m.RpcPumpFeed(w)
        feed.on_notification({"params": {"result": {"value": {"signature": "S", "err": None,
                              "logs": ["Program data: !!notbase64", "Program data: " + base64.b64encode(m.RpcPumpFeed.TRADE + b"short").decode(),
                                       "Program log: hi"]}}}})
        feed.on_notification({"jsonrpc": "2.0", "result": 5, "id": 1})
        self.assertEqual(w.tokens, {})

    def test_ws_url(self):
        old = {k: os.environ.pop(k, None) for k in ("SOLANA_WSS_URL", "SOLANA_RPC_URL")}
        try:
            self.assertEqual(m.RpcPumpFeed.ws_url(), "wss://api.mainnet-beta.solana.com")
            os.environ["SOLANA_RPC_URL"] = "https://mainnet.helius-rpc.com/?api-key=K"
            self.assertEqual(m.RpcPumpFeed.ws_url(), "wss://mainnet.helius-rpc.com/?api-key=K")
            os.environ["SOLANA_WSS_URL"] = "wss://x.example"
            self.assertEqual(m.RpcPumpFeed.ws_url(), "wss://x.example")
        finally:
            for k in ("SOLANA_WSS_URL", "SOLANA_RPC_URL"):
                os.environ.pop(k, None)
                if old[k] is not None:
                    os.environ[k] = old[k]

    def test_notices_are_recorded(self):
        w = m.PumpWatcher(m.CFG, clock=lambda: 0.0)
        w.handle({"message": "Successfully subscribed to keys."})
        w.handle({"errors": "API key required for token trades"})
        self.assertEqual(w.status()["notices"][-1], "API key required for token trades")
        self.assertEqual(w.status()["kinds"]["(notice)"], 2)


class Dashboard(unittest.TestCase):
    def test_nan_is_sanitised(self):
        self.assertEqual(m.finite({"a": [float("nan"), 1.0, float("inf")]}), {"a": [None, 1.0, None]})

    def test_guards(self):
        eng = demo_engine()
        srv = ThreadingHTTPServer(("127.0.0.1", 0), None)
        port = srv.server_address[1]
        srv.RequestHandlerClass = m.make_handler(eng, port)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            def req(method, path, headers=None):
                c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                c.request(method, path, headers=headers or {})
                r = c.getresponse()
                return r.status, r.read()
            self.assertEqual(req("GET", "/")[0], 200)
            code, body = req("GET", "/state.json")
            self.assertEqual(code, 200)
            self.assertIn("pump", json.loads(body))
            self.assertEqual(req("GET", "/", {"Host": "evil.example:80"})[0], 403)
            self.assertEqual(req("POST", "/api/pause")[0], 403)
            self.assertFalse(eng.paused)
            self.assertEqual(req("POST", "/api/pause", {"X-MemeBot": "1"})[0], 200)
            self.assertTrue(eng.paused)
        finally:
            srv.shutdown()
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
