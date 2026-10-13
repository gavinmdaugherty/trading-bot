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
        self.assertEqual(self.w.pop_events()[0][0], "RISK")
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


class Dashboard(unittest.TestCase):
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
