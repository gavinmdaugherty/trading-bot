#!/usr/bin/env python3
"""
MemeBot - multi-chain memecoin paper-trading engine with a live dashboard.

    python memebot.py            real market data, simulated fills (paper trading)
    python memebot.py --demo     synthetic data, to preview the dashboard offline
    python memebot.py --reset    wipe saved state and start over
    python memebot.py --live --live-agents Bolt --live-usd 50
                                 ALSO trade real money on Solana with a live twin of Bolt

Then open http://127.0.0.1:8000

What it does
  * Discovers new tokens on Solana / Base / Ethereum / BSC (DexScreener public API)
  * Runs six strategy agents side by side, each with its own balance and rules
  * Screens tokens with safety checks (RugCheck on Solana, honeypot.is on EVM)
  * Fills paper trades with a realistic cost model: price impact from pool size,
    DEX fee, latency slippage and token buy/sell tax
  * Writes everything to state.json and serves the dashboard from the same process

Without --live nothing here places real orders. With --live, the chosen agents trade
real USDC on Solana (Jupiter) from a dedicated wallet; every other agent stays paper.
Standard library only (Python 3.9+).
"""
import argparse
import base64
import json
import math
import os
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
CFG = dict(
    chains=["solana", "base", "ethereum", "bsc"],
    poll_seconds=30,
    port=8000,
    state_file="state.json",
    stop_file="STOP",            # create this file to block all new entries
    start_balance=200.0,         # per agent, USD
    min_trade_usd=10.0,
    max_liq_frac=0.02,           # never size a trade above 2% of pool liquidity
    universe_max=240,            # tokens tracked at once (8 API calls per cycle)
    use_safety_checks=True,
    safety_per_cycle=6,          # max safety lookups per cycle (rate limits)
    safety_ttl=1800,
    max_tax_pct=12.0,            # skip tokens with a sell tax above this
    dex_fee_pct=0.30,            # per side
    extra_slip_pct=1.00,         # per side: latency / MEV allowance
    default_tax_pct=0.0,
    cooldown_min=60,             # no re-entry into the same token for this long
    daily_loss_pct=15.0,         # an agent stops opening trades after losing this much in a day
    max_new_per_cycle=2,         # per agent
    # ---- live trading (Solana only, off unless you start with --live) ----
    live_usd=50.0,               # bankroll per live agent: the most it can ever deploy
    live_min_trade_usd=5.0,
    live_max_trade_usd=10.0,     # hard cap per order
    live_slippage_bps=300,       # buys: 3% tolerance
    live_sell_slippage_bps=[500, 1000, 2500],   # sells retry with wider tolerance
    live_max_price_dev_pct=8.0,  # refuse a buy if the route price is this far above market
    live_max_priority_lamports=500_000,         # cap on the priority fee per transaction
    live_confirm_secs=60,
    live_max_fails=4,            # consecutive failed transactions before live trading halts
    live_min_sol=0.02,           # SOL needed in the wallet for network fees
)

# Six strategy variants. Percent fields are percent; ages are minutes.
AGENTS = [
    dict(name="Atlas",  style="Steady momentum",
         min_liq=50_000, min_vol_h1=20_000, min_chg_h1=5,  max_chg_h1=80,  min_chg_m5=-2,
         min_age=60,  max_age=1440, min_buy_ratio=1.10, min_vol_liq=0.2,
         tp=40,  sl=-20, trail_act=25, trail=12, max_hold=180, pos_pct=0.10, max_open=4),
    dict(name="Bolt",   style="Fast scalp",
         min_liq=40_000, min_vol_h1=30_000, min_chg_h1=3,  max_chg_h1=60,  min_chg_m5=1,
         min_age=45,  max_age=720,  min_buy_ratio=1.20, min_vol_liq=0.4,
         tp=15,  sl=-8,  trail_act=10, trail=5,  max_hold=45,  pos_pct=0.10, max_open=5),
    dict(name="Cinder", style="Early runner",
         min_liq=30_000, min_vol_h1=25_000, min_chg_h1=15, max_chg_h1=250, min_chg_m5=0,
         min_age=20,  max_age=240,  min_buy_ratio=1.30, min_vol_liq=0.5,
         tp=100, sl=-25, trail_act=40, trail=20, max_hold=240, pos_pct=0.08, max_open=4),
    dict(name="Drift",  style="Established, calmer",
         min_liq=150_000, min_vol_h1=40_000, min_chg_h1=2, max_chg_h1=40,  min_chg_m5=-1,
         min_age=360, max_age=4320, min_buy_ratio=1.05, min_vol_liq=0.1,
         tp=25,  sl=-12, trail_act=15, trail=8,  max_hold=360, pos_pct=0.12, max_open=4),
    dict(name="Ember",  style="Dip buyer",
         min_liq=80_000, min_vol_h1=30_000, min_chg_h1=-30, max_chg_h1=-6, min_chg_m5=0.5,
         min_age=120, max_age=2880, min_buy_ratio=1.00, min_vol_liq=0.2,
         tp=20,  sl=-15, trail_act=12, trail=7,  max_hold=120, pos_pct=0.08, max_open=3),
    dict(name="Flux",   style="Volume breakout",
         min_liq=60_000, min_vol_h1=120_000, min_chg_h1=4, max_chg_h1=120, min_chg_m5=0,
         min_age=45,  max_age=1440, min_buy_ratio=1.15, min_vol_liq=1.0,
         tp=35,  sl=-15, trail_act=20, trail=10, max_hold=120, pos_pct=0.10, max_open=4),
]

CHAIN_IDS = {"ethereum": 1, "bsc": 56, "base": 8453}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def utcday(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")


def tkey(chain, addr):
    return f"{chain}:{addr if chain == 'solana' else addr.lower()}"


def get_json(url, timeout=15):
    req = urllib.request.Request(
        url, headers={"User-Agent": "memebot/1.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def num(d, *path, default=0.0):
    for k in path:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
    try:
        return float(d) if d is not None else default
    except (TypeError, ValueError):
        return default


def norm_pair(p):
    base = p.get("baseToken") or {}
    price = num(p, "priceUsd")
    if not price or not base.get("address"):
        return None
    created = num(p, "pairCreatedAt") / 1000.0
    return dict(
        chain=p.get("chainId"), addr=base["address"], symbol=base.get("symbol") or "?",
        price=price, liq=num(p, "liquidity", "usd"),
        vol_h1=num(p, "volume", "h1"), vol_h24=num(p, "volume", "h24"),
        chg_m5=num(p, "priceChange", "m5"), chg_h1=num(p, "priceChange", "h1"),
        chg_h24=num(p, "priceChange", "h24"),
        buys_h1=num(p, "txns", "h1", "buys"), sells_h1=num(p, "txns", "h1", "sells"),
        fdv=num(p, "fdv"), created=created or None, pair=p.get("pairAddress"))


def impact_pct(size_usd, liq_usd):
    """Constant-product price impact: trading x into a pool with liquidity L
    (L/2 on each side) moves price by roughly 2x/L."""
    return min(60.0, 200.0 * size_usd / max(liq_usd, 1.0))


# --------------------------------------------------------------------------- #
# Data sources
# --------------------------------------------------------------------------- #
class LiveSource:
    API = "https://api.dexscreener.com"

    def tick(self, t):
        pass

    def discover(self, chains):
        out = set()
        for ep in ("/token-profiles/latest/v1", "/token-boosts/latest/v1",
                   "/token-boosts/top/v1"):
            try:
                data = get_json(self.API + ep)
            except Exception:
                continue
            for it in data if isinstance(data, list) else []:
                if it.get("chainId") in chains and it.get("tokenAddress"):
                    out.add((it["chainId"], it["tokenAddress"]))
        return list(out)

    def pairs(self, chain, addrs):
        """Most liquid pair for each requested token address (max 30 per call)."""
        data = get_json(f"{self.API}/tokens/v1/{chain}/{','.join(addrs)}")
        wanted = {a if chain == "solana" else a.lower(): a for a in addrs}
        best = {}
        for raw in data if isinstance(data, list) else []:
            p = norm_pair(raw)
            if not p or p["chain"] != chain:
                continue
            k = p["addr"] if chain == "solana" else p["addr"].lower()
            if k in wanted and (k not in best or p["liq"] > best[k]["liq"]):
                best[k] = p
        return {wanted[k]: p for k, p in best.items()}

    def safety(self, chain, addr):
        if chain == "solana":
            d = get_json(f"https://api.rugcheck.xyz/v1/tokens/{addr}/report/summary")
            danger = [r.get("name", "?") for r in (d.get("risks") or [])
                      if str(r.get("level", "")).lower() == "danger"]
            if danger:
                return dict(ok=False, note="rugcheck: " + ", ".join(danger[:2]),
                            buy_tax=None, sell_tax=None)
            lp = d.get("lpLockedPct")
            note = "rugcheck clean" + (f", LP locked {float(lp):.0f}%" if lp is not None else "")
            return dict(ok=True, note=note, buy_tax=None, sell_tax=None)
        cid = CHAIN_IDS.get(chain)
        if cid:
            d = get_json(f"https://api.honeypot.is/v2/IsHoneypot?address={addr}&chainID={cid}")
            hp = (d.get("honeypotResult") or {}).get("isHoneypot")
            sim = d.get("simulationResult") or {}
            bt, st = sim.get("buyTax"), sim.get("sellTax")
            if hp:
                return dict(ok=False, note="honeypot: sell simulation failed",
                            buy_tax=None, sell_tax=None)
            if st is not None and float(st) > CFG["max_tax_pct"]:
                return dict(ok=False, note=f"sell tax {float(st):.0f}%",
                            buy_tax=bt, sell_tax=st)
            return dict(ok=True, note=f"sell sim ok, tax {float(bt or 0):.0f}/{float(st or 0):.0f}%",
                        buy_tax=bt, sell_tax=st)
        return dict(ok=None, note="no checker for this chain", buy_tax=None, sell_tax=None)


class DemoSource:
    """Synthetic random-walk market. Only for previewing the dashboard offline."""

    def __init__(self, seed=7):
        self.rng = random.Random(seed)
        self.tokens = {}
        self.n = 0
        self.t = time.time()

    def tick(self, t):
        self.t = t
        for tok in self.tokens.values():
            self._step(tok)

    def _step(self, tok):
        r = self.rng
        tok["price"] *= math.exp(tok["drift"] + tok["vol"] * r.gauss(0, 1))
        if tok["rug"] and r.random() < 0.03:
            tok["price"] *= 0.08
            tok["liq"] *= 0.1
        tok["hist"].append(tok["price"])
        tok["hist"] = tok["hist"][-61:]

    def discover(self, chains):
        r = self.rng
        for _ in range(r.choice([0, 1, 1, 2, 3])):
            self.n += 1
            chain = r.choice(chains)
            tok = dict(chain=chain, addr=f"DEMO{self.n:04d}", symbol=f"TKN{self.n}",
                       price=r.uniform(1e-6, 1e-3), liq=r.uniform(30e3, 300e3),
                       created=self.t - r.uniform(0, 4) * 3600, drift=r.gauss(-0.002, 0.003),
                       vol=r.uniform(0.01, 0.05), rug=r.random() < 0.15, hist=[])
            for _ in range(60):
                self._step(tok)
            self.tokens[(chain, tok["addr"])] = tok
        return list(self.tokens)

    def pairs(self, chain, addrs):
        out = {}
        for a in addrs:
            tok = self.tokens.get((chain, a))
            if not tok:
                continue
            h = tok["hist"]
            chg_h1 = (h[-1] / h[0] - 1) * 100
            chg_m5 = (h[-1] / h[-5] - 1) * 100
            up = chg_h1 > 0
            sells = self.rng.randint(80, 400)
            buys = int(sells * (self.rng.uniform(1.0, 1.6) if up else self.rng.uniform(0.6, 1.1)))
            out[a] = dict(chain=chain, addr=a, symbol=tok["symbol"], price=tok["price"],
                          liq=tok["liq"], vol_h1=tok["liq"] * self.rng.uniform(0.1, 1.6),
                          vol_h24=tok["liq"] * 5, chg_m5=chg_m5, chg_h1=chg_h1, chg_h24=chg_h1,
                          buys_h1=buys, sells_h1=sells, fdv=tok["liq"] * 20,
                          created=tok["created"], pair=a)
        return out

    def safety(self, chain, addr):
        tok = self.tokens[(chain, addr)]
        if tok["rug"] and self.rng.random() < 0.6:
            return dict(ok=False, note="demo: honeypot flagged", buy_tax=None, sell_tax=None)
        return dict(ok=True, note="demo: checks passed", buy_tax=0, sell_tax=0)


class SolanaLive:
    """Real swaps on Solana through Jupiter, settled in USDC.

    Safety properties:
      * every swap is simulated by the RPC node before it is broadcast (preflight)
      * buys are refused if the route price is far above the market price
      * actual fills are measured from wallet balances, not assumed from the quote
      * the private key is read from an environment variable and never logged
    """
    USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    TOKEN_PROGRAMS = ("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                      "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")

    def __init__(self, cfg):
        try:
            from solders.keypair import Keypair
            from solders.transaction import VersionedTransaction
        except ImportError:
            raise SystemExit("Live trading needs one extra package. Run:  pip install solders")
        self._VT = VersionedTransaction
        secret = os.environ.get("MEMEBOT_SOLANA_KEY", "").strip()
        if not secret:
            raise SystemExit("Set the MEMEBOT_SOLANA_KEY environment variable to the private key of a "
                             "dedicated bot wallet (see the setup steps).")
        try:
            self.kp = (Keypair.from_bytes(bytes(json.loads(secret))) if secret.startswith("[")
                       else Keypair.from_base58_string(secret))
        except Exception:
            raise SystemExit("MEMEBOT_SOLANA_KEY is not a valid Solana private key "
                             "(expected base58, or a JSON array of 64 numbers).")
        self.pub = str(self.kp.pubkey())
        self.cfg = cfg
        self.rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        key = os.environ.get("JUPITER_API_KEY", "").strip()
        default = "https://api.jup.ag/swap/v1" if key else "https://lite-api.jup.ag/swap/v1"
        self.jup = os.environ.get("JUPITER_API_URL", default).rstrip("/")
        self.hdrs = {"x-api-key": key} if key else {}

    # ---- plumbing ----
    def _http(self, url, body=None, headers=None, timeout=20):
        h = {"User-Agent": "memebot/1.0", "Accept": "application/json"}
        h.update(headers or {})
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            h["Content-Type"] = "application/json"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h),
                                        timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            host = urllib.parse.urlparse(url).netloc
            try:
                detail = e.read()[:150].decode("utf-8", "replace")
            except Exception:
                detail = ""
            raise RuntimeError(f"HTTP {e.code} from {host} {detail}".strip())
        except urllib.error.URLError as e:
            raise RuntimeError(f"network error: {e.reason}")

    def _rpc(self, method, params):
        d = self._http(self.rpc, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if d.get("error"):
            raise RuntimeError(f"rpc {method}: {str(d['error'])[:200]}")
        return d["result"]

    # ---- wallet reads ----
    def token_balance(self, mint):
        r = self._rpc("getTokenAccountsByOwner",
                      [self.pub, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        raw, dec = 0, None
        for acc in r["value"]:
            ta = acc["account"]["data"]["parsed"]["info"]["tokenAmount"]
            raw += int(ta["amount"])
            dec = ta["decimals"]
        return raw, dec

    def usdc(self):
        return self.token_balance(self.USDC)[0] / 1e6

    def sol(self):
        return self._rpc("getBalance", [self.pub, {"commitment": "confirmed"}])["value"] / 1e9

    def decimals(self, mint):
        return int(self._rpc("getTokenSupply", [mint])["value"]["decimals"])

    def wallet(self):
        return dict(pub=self.pub[:4] + "..." + self.pub[-4:], usdc=self.usdc(), sol=self.sol())

    def holdings(self):
        """Every non-zero token balance in the wallet except USDC (for the startup report)."""
        out = []
        for prog in self.TOKEN_PROGRAMS:
            r = self._rpc("getTokenAccountsByOwner",
                          [self.pub, {"programId": prog}, {"encoding": "jsonParsed", "commitment": "confirmed"}])
            for acc in r["value"]:
                info = acc["account"]["data"]["parsed"]["info"]
                if info["mint"] != self.USDC and int(info["tokenAmount"]["amount"]) > 0:
                    out.append((info["mint"], info["tokenAmount"]["uiAmountString"]))
        return out

    # ---- swaps ----
    def _quote(self, in_mint, out_mint, amount, slippage_bps):
        q = urllib.parse.urlencode(dict(inputMint=in_mint, outputMint=out_mint, amount=int(amount),
                                        slippageBps=int(slippage_bps), restrictIntermediateTokens="true"))
        return self._http(f"{self.jup}/quote?{q}", headers=self.hdrs)

    def _execute(self, quote):
        """Build, sign, simulate (preflight), broadcast and wait for confirmation."""
        c = self.cfg
        body = {"quoteResponse": quote, "userPublicKey": self.pub, "dynamicComputeUnitLimit": True,
                "prioritizationFeeLamports": {"priorityLevelWithMaxLamports": {
                    "priorityLevel": "high", "maxLamports": int(c["live_max_priority_lamports"])}}}
        sw = self._http(f"{self.jup}/swap", body, self.hdrs, timeout=30)
        tx = self._VT.from_bytes(base64.b64decode(sw["swapTransaction"]))
        signed = self._VT(tx.message, [self.kp])
        b64 = base64.b64encode(bytes(signed)).decode()
        sig = self._rpc("sendTransaction", [b64, {"encoding": "base64", "skipPreflight": False,
                                                  "preflightCommitment": "confirmed", "maxRetries": 3}])
        deadline = time.time() + c["live_confirm_secs"]
        while time.time() < deadline:
            time.sleep(2)
            st = self._rpc("getSignatureStatuses", [[sig], {"searchTransactionHistory": False}])["value"][0]
            if st:
                if st.get("err"):
                    raise RuntimeError(f"transaction failed on-chain: {st['err']}")
                if st.get("confirmationStatus") in ("confirmed", "finalized"):
                    return sig
        raise TimeoutError(f"not confirmed within {c['live_confirm_secs']}s (signature {sig})")

    def _changed(self, read, before, tries=5):
        """Balance reads can lag a moment behind confirmation; poll until the value moves."""
        v = read()
        for _ in range(tries):
            if v != before:
                return v
            time.sleep(2)
            v = read()
        return v

    def buy(self, mint, usd, ref_price):
        c = self.cfg
        try:
            dec = self.decimals(mint)
            q = self._quote(self.USDC, mint, round(usd * 1e6), c["live_slippage_bps"])
            out = int(q["outAmount"])
            if out <= 0:
                return dict(ok=False, stage="quote", err="empty quote")
            implied = (int(q["inAmount"]) / 1e6) / (out / 10 ** dec)
            dev = (implied / ref_price - 1) * 100
            if dev > c["live_max_price_dev_pct"]:
                return dict(ok=False, stage="quote", err=f"route price {dev:.1f}% above market")
        except Exception as e:
            return dict(ok=False, stage="quote", err=str(e)[:160])
        try:
            usdc0 = self.usdc()
            tok0 = self.token_balance(mint)[0]
            try:
                sig = self._execute(q)
            except TimeoutError as e:
                time.sleep(15)  # the transaction may still land; check the wallet before giving up
                if self.token_balance(mint)[0] <= tok0:
                    raise
                sig = str(e)[-90:]
            tok1 = self._changed(lambda: self.token_balance(mint)[0], tok0)
            usdc1 = self._changed(self.usdc, usdc0)
        except Exception as e:
            return dict(ok=False, stage="tx", err=str(e)[:200])
        raw = tok1 - tok0
        if raw <= 0:
            return dict(ok=False, stage="tx", err="confirmed but no tokens received")
        spent = usdc0 - usdc1
        return dict(ok=True, spent_usd=spent if spent > 0 else usd, raw=raw, decimals=dec, sig=sig)

    def sell(self, mint, raw_expected):
        c = self.cfg
        try:
            raw_now, _ = self.token_balance(mint)
        except Exception as e:
            return dict(ok=False, stage="tx", err=str(e)[:160])
        if raw_now <= 0:
            return dict(ok=True, got_usd=0.0, sig=None, note="wallet holds none of this token, closed at 0")
        amount = min(raw_now, raw_expected) if raw_expected else raw_now
        last = "unknown error"
        for bps in c["live_sell_slippage_bps"]:
            try:
                q = self._quote(mint, self.USDC, amount, bps)
                usdc0 = self.usdc()
                try:
                    sig = self._execute(q)
                except TimeoutError as e:
                    time.sleep(15)
                    if self.token_balance(mint)[0] >= raw_now:
                        raise
                    sig = str(e)[-90:]
                usdc1 = self._changed(self.usdc, usdc0)
                return dict(ok=True, got_usd=max(0.0, usdc1 - usdc0), sig=sig)
            except Exception as e:
                last = str(e)[:160]
        return dict(ok=False, stage="tx", err=last)


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class Engine:
    def __init__(self, cfg, agents, src, demo=False, live=None, live_names=()):
        self.cfg, self.src, self.demo, self.live = cfg, src, demo, live
        self.acfg = {a["name"]: a for a in agents}
        for n in live_names:  # each live agent is a real-money twin of a paper agent
            twin = dict(self.acfg[n], name=f"{n} LIVE", style=f"Live twin of {n}", live=True)
            self.acfg[twin["name"]] = twin
        self.live_halt = False
        self.live_fails = 0
        self.wallet = None
        self._cache = None
        self.lock = threading.RLock()
        self.universe, self.safety, self.dropped = {}, {}, set()
        self.t = time.time()
        self.paused = False
        self.errors = 0
        self.cycles = 0
        self.last_cycle = None
        self.state = self._load() or self._fresh()
        for name in self.acfg:
            self.state["agents"].setdefault(name, self._fresh_agent(name))
        for st in self.state["agents"].values():
            for pos in st["positions"].values():
                self._track(pos["chain"], pos["addr"])
        self._cache = self.snapshot()

    # ---- persistence ------------------------------------------------------ #
    def _fresh_agent(self, name):
        live = bool(self.acfg[name].get("live"))
        s = self.cfg["live_usd"] if live else self.cfg["start_balance"]
        return dict(cash=s, start=s, positions={}, trades=[], wins=0, losses=0, pnl=0.0,
                    best=None, day=utcday(self.t), day_pnl=0.0, capped=False, cool={}, live=live)

    def _fresh(self):
        return dict(started=self.t, agents={n: self._fresh_agent(n) for n in self.acfg},
                    equity=[], log=[], scanned=0)

    def _load(self):
        if self.demo or not os.path.exists(self.cfg["state_file"]):
            return None
        try:
            with open(self.cfg["state_file"]) as f:
                return json.load(f)
        except Exception:
            return None

    def _save(self):
        if self.demo:
            return
        tmp = self.cfg["state_file"] + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f)
        os.replace(tmp, self.cfg["state_file"])

    # ---- small utilities ---------------------------------------------------- #
    def emit(self, tag, msg):
        log = self.state["log"]
        log.append(dict(t=self.t, tag=tag, msg=msg))
        del log[:-400]

    def _err(self, msg):
        with self.lock:
            self.errors += 1
            self.emit("WARN", msg)

    def _track(self, chain, addr):
        k = tkey(chain, addr)
        if k in self.universe or k in self.dropped:
            return False
        self.universe[k] = dict(chain=chain, addr=addr, first=self.t, pair=None, miss=0)
        return True

    def _roll_day(self, st):
        d = utcday(self.t)
        if st["day"] != d:
            st.update(day=d, day_pnl=0.0, capped=False)

    def _value(self, pos, price, liq):
        """What the position would net if sold now (after impact, fee, slippage, tax)."""
        gross = pos["units"] * price
        c = self.cfg
        cost = impact_pct(gross, liq) + c["dex_fee_pct"] + c["extra_slip_pct"] + pos["sell_tax"]
        return gross * (1 - min(cost, 95.0) / 100.0)

    def _equity(self, st):
        return st["cash"] + sum(self._value(p, p["last_price"], p["last_liq"])
                                for p in st["positions"].values())

    # ---- entry rules ---------------------------------------------------------- #
    def _wants(self, name, p):
        a, st, c = self.acfg[name], self.state["agents"][name], self.cfg
        live = a.get("live")
        if live and (p["chain"] != "solana" or self.live_halt or self.live is None):
            return False
        min_t = c["live_min_trade_usd"] if live else c["min_trade_usd"]
        k = tkey(p["chain"], p["addr"])
        if k in st["positions"] or self.t - st["cool"].get(k, 0) < c["cooldown_min"] * 60:
            return False
        if len(st["positions"]) >= a["max_open"] or st["capped"] or st["cash"] < min_t:
            return False
        if not p["created"]:
            return False
        age = (self.t - p["created"]) / 60.0
        if not (a["min_age"] <= age <= a["max_age"]):
            return False
        if p["liq"] < a["min_liq"] or p["vol_h1"] < a["min_vol_h1"]:
            return False
        if not (a["min_chg_h1"] <= p["chg_h1"] <= a["max_chg_h1"]) or p["chg_m5"] < a["min_chg_m5"]:
            return False
        if p["buys_h1"] / max(p["sells_h1"], 1.0) < a["min_buy_ratio"]:
            return False
        if p["vol_h1"] / p["liq"] < a["min_vol_liq"]:
            return False
        if p["fdv"] and p["fdv"] / p["liq"] > 300:  # valuation far beyond what the pool can support
            return False
        return True

    def _buy(self, name, p, sf):
        a, st, c = self.acfg[name], self.state["agents"][name], self.cfg
        live = a.get("live")
        min_t = c["live_min_trade_usd"] if live else c["min_trade_usd"]
        size = min(st["cash"], max(min_t, a["pos_pct"] * self._equity(st)),
                   p["liq"] * c["max_liq_frac"])
        if live:
            size = min(size, c["live_max_trade_usd"])
        if size < min_t:
            return False
        if live:
            return self._live_buy(name, p, size)
        buy_tax = float(sf.get("buy_tax") or 0)
        sell_tax = float(sf.get("sell_tax") or c["default_tax_pct"])
        cost = impact_pct(size, p["liq"]) + c["dex_fee_pct"] + c["extra_slip_pct"]
        units = size * (1 - buy_tax / 100.0) / (p["price"] * (1 + cost / 100.0))
        k = tkey(p["chain"], p["addr"])
        st["cash"] -= size
        st["positions"][k] = dict(
            chain=p["chain"], addr=p["addr"], symbol=p["symbol"], units=units, size=size,
            ref_price=p["price"], peak=p["price"], opened=self.t, entry_liq=p["liq"],
            sell_tax=sell_tax, last_price=p["price"], last_liq=p["liq"], miss=0)
        self.emit("EXEC", f"{name} BUY ${p['symbol']} ${size:.2f} on {p['chain']} @ {p['price']:.8g}")
        return True

    def _live_fail(self, msg):
        self.live_fails += 1
        self.emit("WARN", msg)
        if self.live_fails >= self.cfg["live_max_fails"] and not self.live_halt:
            self.live_halt = True
            self.emit("RISK", "LIVE trading halted after repeated failed transactions. "
                              "No new live entries. Check your open positions and restart to resume.")

    def _live_buy(self, name, p, size):
        st, k = self.state["agents"][name], tkey(p["chain"], p["addr"])
        st["cool"][k] = self.t  # never retry the same token immediately, whatever happens
        res = self.live.buy(p["addr"], size, p["price"])
        if not res["ok"]:
            if res.get("stage") == "tx":
                self._live_fail(f"{name} buy ${p['symbol']} failed: {res['err']}")
            else:
                self.emit("RISK", f"{name} skipped ${p['symbol']}: {res['err']}")
            return False
        self.live_fails = 0
        spent, units = res["spent_usd"], res["raw"] / 10 ** res["decimals"]
        fill = spent / units
        st["cash"] -= spent
        st["positions"][k] = dict(
            chain=p["chain"], addr=p["addr"], symbol=p["symbol"], units=units, size=spent,
            ref_price=fill, peak=fill, opened=self.t, entry_liq=p["liq"], sell_tax=0.0,
            last_price=p["price"], last_liq=p["liq"], miss=0, live=True,
            raw=res["raw"], decimals=res["decimals"], sig=res["sig"])
        self.emit("EXEC", f"{name} BUY ${p['symbol']} ${spent:.2f} LIVE tx {str(res['sig'])[:10]}")
        return True

    # ---- exit rules ------------------------------------------------------------ #
    def _sell(self, name, k, reason, writeoff=False):
        st = self.state["agents"][name]
        pos = st["positions"][k]
        if pos.get("live"):
            res = self.live.sell(pos["addr"], pos["raw"])
            if not res["ok"]:
                pos["pending_exit"] = reason
                pos["sell_fails"] = pos.get("sell_fails", 0) + 1
                self._live_fail(f"{name} could not sell ${pos['symbol']}: {res['err']} (will retry)")
                return
            self.live_fails = 0
            proceeds = res["got_usd"]
            if res.get("note"):
                self.emit("WARN", f"${pos['symbol']}: {res['note']}")
        else:
            proceeds = 0.0 if writeoff else self._value(pos, pos["last_price"], pos["last_liq"])
        st["positions"].pop(k)
        pnl = proceeds - pos["size"]
        st["cash"] += proceeds
        st["pnl"] += pnl
        st["day_pnl"] += pnl
        st["cool"][k] = self.t
        if pnl >= 0:
            st["wins"] += 1
        else:
            st["losses"] += 1
        if pnl > 0 and (not st["best"] or pnl > st["best"]["pnl"]):
            st["best"] = dict(pnl=pnl, symbol=pos["symbol"])
        st["trades"].append(dict(t=self.t, agent=name, symbol=pos["symbol"], chain=pos["chain"],
                                 pnl=round(pnl, 2), ret=round(pnl / pos["size"] * 100, 1),
                                 reason=reason, held=round((self.t - pos["opened"]) / 60, 1)))
        del st["trades"][:-300]
        self.emit("FILL", f"{name} SOLD ${pos['symbol']} {pnl:+.2f} ({reason})")

    def _exits(self):
        fresh_count = 0
        for name, st in self.state["agents"].items():
            a = self.acfg.get(name)
            if not a:
                continue
            self._roll_day(st)
            for k, pos in list(st["positions"].items()):
                if pos.get("pending_exit"):  # a live sell failed earlier: keep retrying
                    self._sell(name, k, pos["pending_exit"])
                    continue
                u = self.universe.get(k)
                p = u["pair"] if u and u["miss"] == 0 else None
                if not p:
                    pos["miss"] += 1
                    if pos["miss"] >= 10:
                        self._sell(name, k, "no price data for 10 cycles", writeoff=not pos.get("live"))
                    continue
                fresh_count += 1
                pos.update(miss=0, last_price=p["price"], last_liq=p["liq"],
                           peak=max(pos["peak"], p["price"]))
                ret = (p["price"] / pos["ref_price"] - 1) * 100
                peak_ret = (pos["peak"] / pos["ref_price"] - 1) * 100
                held = (self.t - pos["opened"]) / 60.0
                if ret >= a["tp"]:
                    self._sell(name, k, "take-profit")
                elif ret <= a["sl"]:
                    self._sell(name, k, "stop-loss")
                elif peak_ret >= a["trail_act"] and p["price"] <= pos["peak"] * (1 - a["trail"] / 100):
                    self._sell(name, k, "trailing stop")
                elif p["liq"] < pos["entry_liq"] * 0.5:
                    self._sell(name, k, "liquidity drop")
                elif held >= a["max_hold"]:
                    self._sell(name, k, "time limit")
            cap = st["start"] * self.cfg["daily_loss_pct"] / 100.0
            if st["day_pnl"] <= -cap and not st["capped"]:
                st["capped"] = True
                self.emit("WARN", f"{name} hit its daily loss cap, no new entries until tomorrow")

    def _prune(self):
        held = {k for st in self.state["agents"].values() for k in st["positions"]}
        max_age = max(a["max_age"] for a in self.acfg.values()) * 60 + 3600
        for k in list(self.universe):
            if k in held:
                continue
            u = self.universe[k]
            p = u["pair"]
            if u["miss"] >= 3 or (p and p["created"] and self.t - p["created"] > max_age):
                del self.universe[k]
                self.dropped.add(k)
        extra = len(self.universe) - self.cfg["universe_max"]
        if extra > 0:
            for k in sorted((k for k in self.universe if k not in held),
                            key=lambda k: self.universe[k]["first"])[:extra]:
                del self.universe[k]
                self.dropped.add(k)
        if len(self.dropped) > 5000:
            self.dropped.clear()

    # ---- one full cycle -------------------------------------------------------- #
    def cycle(self):
        c = self.cfg
        self.t = self.t + 60.0 if self.demo else time.time()
        self.src.tick(self.t)
        if self.live:
            try:
                self.wallet = self.live.wallet()
            except Exception as e:
                self._err(f"wallet check failed: {str(e)[:100]}")

        # 1) network: discover + refresh (lock not held)
        try:
            found = self.src.discover(c["chains"])
        except Exception as e:
            found = []
            self._err(f"discovery failed: {e}")
        with self.lock:
            new = sum(self._track(ch, ad) for ch, ad in found)
            todo = {}
            for u in self.universe.values():
                todo.setdefault(u["chain"], []).append(u["addr"])
        fresh = {}
        for chain, addrs in todo.items():
            for i in range(0, len(addrs), 30):
                try:
                    for ad, p in self.src.pairs(chain, addrs[i:i + 30]).items():
                        fresh[tkey(chain, ad)] = p
                except Exception as e:
                    self._err(f"price fetch failed on {chain}: {e}")
                if not self.demo:
                    time.sleep(0.25)

        # 2) apply data, run exits, find candidates
        with self.lock:
            for k, u in self.universe.items():
                p = fresh.get(k)
                if p:
                    u["pair"], u["miss"] = p, 0
                else:
                    u["miss"] += 1
            live = [u["pair"] for u in self.universe.values() if u["miss"] == 0 and u["pair"]]
            min_liq = min(a["min_liq"] for a in self.acfg.values())
            self.state["scanned"] = len(live)
            self.emit("SCAN", f"{len(live)} pairs tracked, "
                              f"{sum(1 for p in live if p['liq'] >= min_liq)} with liq > "
                              f"${min_liq / 1000:.0f}k, {new} new")
            self._exits()
            self._prune()
            entries_open = not self.paused and not os.path.exists(c["stop_file"])
            cands = []
            if entries_open:
                for p in live:
                    if any(self._wants(n, p) for n in self.acfg):
                        cands.append(p)

        # 3) network: safety checks for candidates (lock not held)
        if c["use_safety_checks"]:
            need = [p for p in cands
                    if self.t - self.safety.get(tkey(p["chain"], p["addr"]), {}).get("t", -1e12)
                    > c["safety_ttl"]]
            for p in sorted(need, key=lambda p: -p["vol_h1"])[:c["safety_per_cycle"]]:
                try:
                    sf = self.src.safety(p["chain"], p["addr"])
                except Exception as e:
                    sf = dict(ok=None, note=f"check failed: {str(e)[:60]}", buy_tax=None, sell_tax=None)
                self.safety[tkey(p["chain"], p["addr"])] = dict(sf, t=self.t, logged=False)

        # 4) entries + bookkeeping
        with self.lock:
            bought = {n: 0 for n in self.acfg}
            for p in sorted(cands, key=lambda p: -p["vol_h1"]):
                k = tkey(p["chain"], p["addr"])
                if c["use_safety_checks"]:
                    sf = self.safety.get(k)
                    if sf is None:
                        continue
                else:
                    sf = dict(ok=True, note="checks off", buy_tax=None, sell_tax=None, logged=True)
                if sf["ok"] is False:
                    if not sf["logged"]:
                        sf["logged"] = True
                        self.emit("RISK", f"${p['symbol']} blocked: {sf['note']}")
                    continue
                for name in self.acfg:
                    if bought[name] >= c["max_new_per_cycle"] or not self._wants(name, p):
                        continue
                    if self.acfg[name].get("live") and sf["ok"] is not True:
                        continue  # real money only on tokens that positively passed the checks
                    if not sf["logged"]:
                        sf["logged"] = True
                        tag = "RISK" if sf["ok"] else "WARN"
                        self.emit(tag, f"${p['symbol']} {sf['note']}" +
                                  ("" if sf["ok"] else ", proceeding (paper)"))
                    if self._buy(name, p, sf):
                        bought[name] += 1
            total = sum(self._equity(st) for st in self.state["agents"].values())
            eq = self.state["equity"]
            eq.append([self.t, round(total, 2)])
            del eq[:-6000]
            self.cycles += 1
            self.last_cycle = self.t
            self._save()
            self._cache = self.snapshot()

    def latest(self):
        """Last completed snapshot, served without waiting on a trade in progress."""
        d = dict(self._cache or {})
        d["paused"] = self.paused
        d["stopfile"] = os.path.exists(self.cfg["stop_file"])
        d["halted"] = self.live_halt
        return d

    # ---- dashboard snapshot ----------------------------------------------------- #
    def snapshot(self):
        with self.lock:
            S, agents, positions, trades = self.state, [], [], []
            total = start = wins = losses = 0
            best = None
            for name, st in S["agents"].items():
                if name not in self.acfg:
                    continue
                eq = self._equity(st)
                total += eq
                start += st["start"]
                wins += st["wins"]
                losses += st["losses"]
                if st["best"] and (not best or st["best"]["pnl"] > best["pnl"]):
                    best = dict(st["best"], agent=name)
                agents.append(dict(name=name, style=self.acfg[name]["style"], equity=eq,
                                   live=bool(self.acfg[name].get("live")),
                                   pnl=eq - st["start"], pnl_pct=(eq / st["start"] - 1) * 100,
                                   wins=st["wins"], losses=st["losses"], open=len(st["positions"]),
                                   capped=st["capped"]))
                for pos in st["positions"].values():
                    val = self._value(pos, pos["last_price"], pos["last_liq"])
                    positions.append(dict(
                        agent=name, symbol=pos["symbol"], chain=pos["chain"], size=pos["size"],
                        value=val, ret=(pos["last_price"] / pos["ref_price"] - 1) * 100,
                        held=(self.t - pos["opened"]) / 60))
                trades.extend(st["trades"][-25:])
            trades.sort(key=lambda x: -x["t"])
            eqs = S["equity"]
            stride = max(1, len(eqs) // 500)
            series = eqs[::stride]
            if eqs and series[-1] is not eqs[-1]:
                series.append(eqs[-1])
            return dict(
                mode="demo" if self.demo else ("live" if self.live else "paper"),
                wallet=self.wallet, halted=self.live_halt, paused=self.paused,
                stopfile=os.path.exists(self.cfg["stop_file"]), now=self.t,
                started=S["started"], cycles=self.cycles, errors=self.errors,
                tracked=S["scanned"], total=total, start=start, pnl=total - start,
                pnl_pct=((total / start - 1) * 100) if start else 0, wins=wins, losses=losses,
                best=best, agents=sorted(agents, key=lambda a: -a["equity"]),
                positions=sorted(positions, key=lambda x: -x["size"]), trades=trades[:30],
                log=S["log"][-70:][::-1], equity=series)


# --------------------------------------------------------------------------- #
# Dashboard
# --------------------------------------------------------------------------- #
HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MemeBot</title>
<style>
:root{
  --bg:#12080b; --bg2:#1c0d12; --panel:#1b1115; --line:#3a2229; --text:#f3e8ea; --mute:#a68f95;
  --up:#5eead4; --down:#ff8d8d; --amber:#ffc58a; --blue:#8ab4ff; --violet:#c4a2ff; --warn:#ffb86b;
  --mono:ui-monospace,"JetBrains Mono","SF Mono",Menlo,Consolas,monospace;
  --sans:"Inter","Segoe UI",system-ui,-apple-system,Helvetica,Arial,sans-serif;
}
*{box-sizing:border-box;margin:0}
html{background:var(--bg)}
body{font-family:var(--sans);color:var(--text);min-height:100vh;padding:20px 22px 36px;
  background:radial-gradient(1100px 520px at 85% -10%,#3a1220 0,transparent 70%),
             radial-gradient(900px 500px at -10% 0,#1f0f2a 0,transparent 65%),var(--bg);
  max-width:1280px;margin:0 auto}
header{display:flex;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:18px}
.logo{width:46px;height:46px;border-radius:15px;background:linear-gradient(145deg,#fff,#c9c3cc);
  display:grid;place-items:center;box-shadow:0 6px 22px #0008}
.logo i{width:6px;height:15px;background:#1a1216;border-radius:4px;margin:0 4px;display:inline-block}
h1{font-size:30px;font-weight:700;letter-spacing:-.02em;line-height:1}
.sub{color:var(--mute);font-size:13px;margin-top:4px}
.spacer{flex:1}
.pill{display:flex;align-items:center;gap:8px;padding:9px 14px;border:1px solid var(--line);
  border-radius:999px;background:#ffffff08;font-size:13px;color:var(--text)}
.dot{width:9px;height:9px;border-radius:50%;background:var(--up);box-shadow:0 0 10px var(--up)}
.dot.off{background:var(--warn);box-shadow:0 0 10px var(--warn)}
.dot.live{background:#ff5d5d;box-shadow:0 0 10px #ff5d5d}
.banner.live{border-color:#7a2f2f;background:#3a1416;color:#ffc9c9}
.agent.livecard{border-color:#ff7a7a66}
button.pill{cursor:pointer;font-family:inherit}
button.pill:hover{background:#ffffff14}
button.pill:focus-visible{outline:2px solid var(--amber);outline-offset:2px}
.banner{border:1px solid #6b4a2a;background:#3a2412;color:#ffd9a8;border-radius:12px;padding:10px 14px;
  font-size:13px;margin-bottom:14px;display:none}
.grid{display:grid;gap:14px}
.stats{grid-template-columns:repeat(4,1fr)}
.mid{grid-template-columns:1.25fr 1fr;margin-top:14px}
.card{background:linear-gradient(180deg,#ffffff07,#ffffff03);border:1px solid var(--line);
  border-radius:18px;padding:16px 18px;min-width:0}
.label{color:var(--mute);font-size:12.5px;margin-bottom:8px}
.big{font-size:34px;font-weight:700;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.small{color:var(--mute);font-size:12.5px;margin-top:6px}
.up{color:var(--up)} .down{color:var(--down)}
.bar{border-left:3px solid var(--up);padding-left:14px}
.bar.neg{border-left-color:var(--down)}
.donutwrap{display:flex;align-items:center;gap:16px}
.donutwrap svg{flex:none}
.wl{font-size:13px;line-height:1.7;color:var(--mute)}
.chartcard{height:380px;display:flex;flex-direction:column}
.chartcard canvas{flex:1;width:100%;min-height:0}
.row{display:flex;justify-content:space-between;align-items:baseline;gap:10px;margin-bottom:8px}
.log{font-family:var(--mono);font-size:12.5px;line-height:1.75;overflow:auto;height:324px;padding-right:6px}
.log div{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tag{display:inline-block;min-width:54px}
.t-SCAN{color:var(--blue)} .t-EXEC{color:var(--amber)} .t-FILL{color:var(--up)}
.t-RISK{color:var(--violet)} .t-WARN{color:var(--warn)}
.agents{grid-template-columns:repeat(6,1fr);margin-top:14px}
.agent{position:relative;overflow:hidden}
.agent .nm{font-weight:650;font-size:16px}
.agent .st{color:var(--mute);font-size:12px;margin-top:2px;min-height:15px}
.agent .eq{font-size:22px;font-weight:700;margin-top:12px;font-variant-numeric:tabular-nums}
.agent .meta{color:var(--mute);font-size:12px;margin-top:4px;line-height:1.5}
.agent .meter{height:5px;border-radius:5px;background:#ffffff12;margin-top:12px;overflow:hidden}
.agent .meter b{display:block;height:100%;border-radius:5px}
.agent.lead{border-color:var(--up)}
.crown{position:absolute;right:14px;top:14px;font-size:11px;color:var(--bg);background:var(--up);
  border-radius:99px;padding:2px 8px;font-weight:650}
.tables{grid-template-columns:1fr 1fr;margin-top:14px}
table{width:100%;border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums}
th{color:var(--mute);font-weight:500;text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
td{padding:7px 8px;border-bottom:1px solid #ffffff0c;white-space:nowrap}
.tw{overflow:auto;max-height:300px}
.empty{color:var(--mute);font-size:13px;padding:18px 4px}
@media (max-width:1100px){.agents{grid-template-columns:repeat(3,1fr)}.stats{grid-template-columns:repeat(2,1fr)}
  .mid,.tables{grid-template-columns:1fr}}
@media (max-width:560px){.agents{grid-template-columns:repeat(2,1fr)}.big{font-size:28px}}
</style></head>
<body>
<header>
  <div class="logo"><i></i><i></i></div>
  <div><h1>MemeBot</h1><div class="sub" id="sub">connecting...</div></div>
  <div class="spacer"></div>
  <div class="pill" id="cyc">0 cycles</div>
  <button class="pill" id="pause" type="button">Pause entries</button>
  <div class="pill"><span class="dot" id="dot"></span><span id="mode">PAPER</span></div>
</header>
<div class="banner" id="banner"></div>

<section class="grid stats">
  <div class="card bar" id="balcard"><div class="label">Balance</div><div class="big" id="bal">-</div><div class="small" id="balsub"></div></div>
  <div class="card bar" id="pnlcard"><div class="label">Total P&amp;L</div><div class="big" id="pnl">-</div><div class="small" id="pnlsub"></div></div>
  <div class="card"><div class="label">Largest win</div><div class="big" id="best">-</div><div class="small" id="bestsub"></div></div>
  <div class="card"><div class="label">Win rate</div>
    <div class="donutwrap">
      <svg width="64" height="64" viewBox="0 0 64 64"><circle cx="32" cy="32" r="26" fill="none" stroke="#ffffff14" stroke-width="7"/>
        <circle id="ring" cx="32" cy="32" r="26" fill="none" stroke="var(--up)" stroke-width="7" stroke-linecap="round"
          stroke-dasharray="0 999" transform="rotate(-90 32 32)"/>
        <text id="wrtxt" x="32" y="37" text-anchor="middle" fill="#f3e8ea" font-size="15" font-weight="700">-</text></svg>
      <div class="wl" id="wl">no closed trades yet</div>
    </div></div>
</section>

<section class="grid mid">
  <div class="card chartcard"><div class="row"><div class="label" style="margin:0">Balance history, all agents</div><div class="small" id="range" style="margin:0"></div></div><canvas id="chart"></canvas></div>
  <div class="card"><div class="row"><div class="label" style="margin:0">Activity log</div><div class="small" id="logn" style="margin:0"></div></div><div class="log" id="log"></div></div>
</section>

<section class="grid agents" id="agents"></section>

<section class="grid tables">
  <div class="card"><div class="label">Open positions</div><div class="tw" id="pos"></div></div>
  <div class="card"><div class="label">Recent trades</div><div class="tw" id="trd"></div></div>
</section>

<script>
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const money=(n,d=2)=>(n<0?"-":"")+"$"+Math.abs(n).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d});
const smoney=(n,d=2)=>(n<0?"-":"+")+"$"+Math.abs(n).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d});
const pct=n=>(n>=0?"+":"")+n.toFixed(1)+"%";
const cls=n=>n>=0?"up":"down";
const COLORS=["#ffb48a","#ffc58a","#8ab4ff","#5eead4","#c4a2ff","#ff8fb1"];
const hhmm=t=>new Date(t*1000).toTimeString().slice(0,8);
let last=null;

function drawChart(series){
  const cv=$("#chart"),dpr=window.devicePixelRatio||1,w=cv.clientWidth,h=cv.clientHeight;
  if(!w||!h)return; cv.width=w*dpr; cv.height=h*dpr;
  const x=cv.getContext("2d"); x.scale(dpr,dpr); x.clearRect(0,0,w,h);
  x.font="12px ui-monospace,Menlo,monospace"; x.fillStyle="#a68f95";
  if(series.length<2){x.fillText("Collecting data. The first points appear after a couple of cycles.",8,24);return}
  const vs=series.map(p=>p[1]); let lo=Math.min(...vs),hi=Math.max(...vs);
  if(hi-lo<1){hi+=1;lo-=1} const pad=(hi-lo)*.15; lo-=pad; hi+=pad;
  const X=i=>6+i/(series.length-1)*(w-12), Y=v=>h-22-(v-lo)/(hi-lo)*(h-44);
  const up=vs[vs.length-1]>=vs[0], col=up?"#5eead4":"#ff8d8d";
  x.strokeStyle="#ffffff10"; x.lineWidth=1;
  for(let i=0;i<4;i++){const y=22+i*(h-44)/3; x.beginPath(); x.moveTo(0,y); x.lineTo(w,y); x.stroke();
    x.fillStyle="#a68f95"; x.fillText(money(hi-(hi-lo)*i/3,0),8,y-4)}
  const g=x.createLinearGradient(0,0,0,h); g.addColorStop(0,col+"55"); g.addColorStop(1,col+"00");
  x.beginPath(); x.moveTo(X(0),Y(vs[0])); vs.forEach((v,i)=>x.lineTo(X(i),Y(v)));
  x.lineTo(X(vs.length-1),h); x.lineTo(X(0),h); x.closePath(); x.fillStyle=g; x.fill();
  x.beginPath(); vs.forEach((v,i)=>i?x.lineTo(X(i),Y(v)):x.moveTo(X(i),Y(v)));
  x.strokeStyle=col; x.lineWidth=2.2; x.lineJoin="round"; x.stroke();
  const lx=X(vs.length-1),ly=Y(vs[vs.length-1]); x.beginPath(); x.arc(lx,ly,4.5,0,7); x.fillStyle=col; x.fill();
}

function render(s){
  last=s;
  const demo=s.mode==="demo";
  const isLive=s.mode==="live", held=s.paused||s.stopfile;
  $("#sub").textContent=demo?"Simulated market, for previewing the dashboard":
    (isLive?"Live agents trade real funds on Solana. All other agents are paper":"Paper trading on live market data. No real orders are placed");
  $("#mode").textContent=demo?"DEMO":(held?"ENTRIES PAUSED":(isLive?"LIVE":"PAPER"));
  $("#dot").className="dot"+(held?" off":(isLive?" live":""));
  $("#cyc").textContent=s.cycles+" cycles, "+s.tracked+" tokens tracked";
  $("#pause").textContent=s.paused?"Resume entries":"Pause entries";
  const b=$("#banner");
  b.className="banner";
  if(demo){b.style.display="block";b.textContent="Simulated data. These prices are random and say nothing about real markets."}
  else if(isLive){b.style.display="block";b.className="banner live";
    b.textContent=(s.halted?"LIVE TRADING HALTED after repeated failed transactions. Check open positions and restart. ":"Real funds at risk. ")+
      (s.wallet?"Wallet "+s.wallet.pub+": "+money(s.wallet.usdc)+" USDC, "+s.wallet.sol.toFixed(4)+" SOL.":"Reading wallet balance...")}
  else if(s.errors>0&&s.cycles<3){b.style.display="block";b.textContent="Some data requests failed. Check the activity log and your internet connection."}
  else b.style.display="none";

  $("#bal").textContent=money(s.total);
  $("#balsub").textContent="started at "+money(s.start,2)+", split across "+s.agents.length+" agents";
  $("#pnl").textContent=smoney(s.pnl); $("#pnl").className="big "+cls(s.pnl);
  $("#pnlsub").textContent=pct(s.pnl_pct)+" since start";
  $("#pnlcard").className="card bar"+(s.pnl<0?" neg":""); $("#balcard").className="card bar"+(s.pnl<0?" neg":"");
  if(s.best){$("#best").textContent=smoney(s.best.pnl); $("#best").className="big up"; $("#bestsub").textContent="$"+s.best.symbol+" by "+s.best.agent}
  else{$("#best").textContent="none yet"; $("#best").className="big"; $("#bestsub").textContent="waiting for a winning close"}
  const n=s.wins+s.losses, wr=n?s.wins/n:0;
  $("#wrtxt").textContent=n?Math.round(wr*100)+"%":"-";
  $("#ring").setAttribute("stroke-dasharray",(wr*163.4)+" 999");
  $("#wl").innerHTML=n?`${s.wins} wins<br>${s.losses} losses`:"no closed trades yet";

  drawChart(s.equity);
  if(s.equity.length>1){const a=s.equity[0][0],z=s.equity[s.equity.length-1][0];$("#range").textContent=hhmm(a)+" to "+hhmm(z)}
  $("#logn").textContent=s.log.length+" recent events";
  $("#log").innerHTML=s.log.map(e=>`<div><span style="color:#a68f95">${hhmm(e.t)}</span> <span class="tag t-${esc(e.tag)}">[${esc(e.tag)}]</span> ${esc(e.msg)}</div>`).join("");

  const mx=Math.max(...s.agents.map(a=>a.equity),1);
  $("#agents").innerHTML=s.agents.map((a,i)=>{
    const lead=i===0&&(a.wins+a.losses>0);
    return `<div class="card agent${lead?" lead":""}${a.live?" livecard":""}">${lead?'<span class="crown">leading</span>':""}
      <div class="nm">${esc(a.name)}</div><div class="st">${esc(a.style)}</div>
      <div class="eq">${money(a.equity)}</div>
      <div class="meta"><span class="${cls(a.pnl)}">${smoney(a.pnl)} (${pct(a.pnl_pct)})</span><br>${a.wins}W / ${a.losses}L, ${a.open} open${a.capped?"<br>daily cap hit":""}</div>
      <div class="meter"><b style="width:${Math.max(4,a.equity/mx*100)}%;background:${COLORS[i%6]}"></b></div></div>`}).join("");

  $("#pos").innerHTML=s.positions.length?`<table><tr><th>Agent</th><th>Token</th><th>Chain</th><th>Size</th><th>Now</th><th>Move</th><th>Held</th></tr>`+
    s.positions.map(p=>`<tr><td>${esc(p.agent)}</td><td>$${esc(p.symbol)}</td><td>${esc(p.chain)}</td><td>${money(p.size)}</td><td>${money(p.value)}</td><td class="${cls(p.ret)}">${pct(p.ret)}</td><td>${Math.round(p.held)}m</td></tr>`).join("")+"</table>"
    :'<div class="empty">No open positions. Entries appear here once a token passes an agent\'s filters and the safety checks.</div>';
  $("#trd").innerHTML=s.trades.length?`<table><tr><th>Time</th><th>Agent</th><th>Token</th><th>P&amp;L</th><th>Return</th><th>Exit</th></tr>`+
    s.trades.map(t=>`<tr><td>${hhmm(t.t)}</td><td>${esc(t.agent)}</td><td>$${esc(t.symbol)}</td><td class="${cls(t.pnl)}">${smoney(t.pnl)}</td><td class="${cls(t.ret)}">${pct(t.ret)}</td><td>${esc(t.reason)}</td></tr>`).join("")+"</table>"
    :'<div class="empty">No closed trades yet. Results show here after the first exit.</div>';
}

async function poll(){
  try{const r=await fetch("/state.json",{cache:"no-store"}); render(await r.json())}
  catch(e){$("#sub").textContent="Lost connection to the bot. Is memebot.py still running?"}
}
$("#pause").addEventListener("click",async()=>{await fetch("/api/pause",{method:"POST"}); poll()});
window.addEventListener("resize",()=>last&&drawChart(last.equity));
poll(); setInterval(poll,3000);
</script>
</body></html>
"""


def make_handler(engine):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy",
                             "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/state.json":
                self._send(200, json.dumps(engine.latest()).encode(), "application/json")
            elif path in ("/", "/index.html"):
                self._send(200, HTML.encode(), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self):
            if self.path == "/api/pause":
                engine.paused = not engine.paused
                self._send(200, json.dumps({"paused": engine.paused}).encode(), "application/json")
            else:
                self._send(404, b"not found", "text/plain")

        def log_message(self, *a):
            pass

    return Handler


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def start_live(args):
    """All the checks that must pass before any real order can be placed."""
    names = [n.strip() for n in args.live_agents.split(",") if n.strip()]
    valid = {a["name"] for a in AGENTS}
    if not names:
        raise SystemExit("Choose which agent trades live, for example:  --live --live-agents Bolt")
    bad = [n for n in names if n not in valid]
    if bad:
        raise SystemExit(f"Unknown agent(s): {', '.join(bad)}. Choose from: {', '.join(sorted(valid))}")
    CFG["live_usd"] = args.live_usd
    ex = SolanaLive(CFG)
    try:
        usdc, sol = ex.usdc(), ex.sol()
    except Exception as e:
        raise SystemExit(f"Could not read the wallet balance ({e}). Check SOLANA_RPC_URL and your connection.")
    need = args.live_usd * len(names)
    print(f"\nLIVE TRADING CHECK\n  wallet        {ex.pub}\n  balance       {usdc:,.2f} USDC, {sol:.4f} SOL")
    print(f"  live agents   {', '.join(names)} ({args.live_usd:,.2f} USDC each, {need:,.2f} USDC at most in total)")
    print(f"  per order     {CFG['live_min_trade_usd']:.0f}-{CFG['live_max_trade_usd']:.0f} USDC, Solana tokens only, "
          f"settled in USDC through Jupiter")
    if usdc < need:
        raise SystemExit(f"\nThe wallet holds {usdc:,.2f} USDC but {need:,.2f} is needed. "
                         f"Fund the wallet with USDC or lower --live-usd.")
    if sol < CFG["live_min_sol"]:
        raise SystemExit(f"\nThe wallet holds {sol:.4f} SOL; at least {CFG['live_min_sol']} is needed for network fees.")
    try:
        extra = ex.holdings()
        if extra:
            print("  note          the wallet already holds other tokens, which the bot will NOT manage:")
            for mint, amt in extra[:8]:
                print(f"                {amt} of {mint}")
    except Exception:
        pass
    print("\nThis will place REAL orders with REAL money. You can lose all of it.")
    if input('Type  I UNDERSTAND  to continue: ').strip() != "I UNDERSTAND":
        raise SystemExit("Cancelled. Nothing was traded.")
    return ex, names


def main():
    ap = argparse.ArgumentParser(description="MemeBot paper-trading engine + dashboard")
    ap.add_argument("--demo", action="store_true", help="synthetic market, runs offline")
    ap.add_argument("--reset", action="store_true", help="delete saved state first")
    ap.add_argument("--port", type=int, default=CFG["port"])
    ap.add_argument("--poll", type=float, default=None, help="seconds between cycles")
    ap.add_argument("--live", action="store_true", help="enable REAL trading on Solana (see --live-agents)")
    ap.add_argument("--live-agents", default="", help="comma-separated agents to mirror with real money, e.g. Bolt")
    ap.add_argument("--live-usd", type=float, default=CFG["live_usd"],
                    help="USDC bankroll per live agent: the most each can ever deploy")
    args = ap.parse_args()

    if args.live and args.demo:
        raise SystemExit("--live cannot be combined with --demo")
    if args.reset and os.path.exists(CFG["state_file"]):
        os.remove(CFG["state_file"])

    # Never abandon real positions: if the saved state holds open live trades, live mode is required.
    if os.path.exists(CFG["state_file"]) and not args.demo:
        try:
            with open(CFG["state_file"]) as f:
                saved = json.load(f)
            open_live = {n: [p["symbol"] for p in a["positions"].values()]
                         for n, a in saved.get("agents", {}).items() if a.get("live") and a["positions"]}
        except Exception:
            open_live = {}
        if open_live and not args.live:
            raise SystemExit("Open LIVE positions exist: " + "; ".join(f"{n}: {', '.join(s)}" for n, s in open_live.items())
                             + "\nStart again with --live (and the same --live-agents) so the bot can manage and sell them. "
                               "Do not delete state.json.")

    live, live_names = None, []
    if args.live:
        live, live_names = start_live(args)

    poll = args.poll if args.poll is not None else (1.0 if args.demo else CFG["poll_seconds"])
    src = DemoSource() if args.demo else LiveSource()
    engine = Engine(CFG, AGENTS, src, demo=args.demo, live=live, live_names=live_names)

    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(engine))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    mode = "DEMO" if args.demo else ("LIVE + PAPER" if live else "PAPER")
    print(f"MemeBot running ({mode} mode). Dashboard: http://127.0.0.1:{args.port}   (Ctrl+C to stop)")

    try:
        while True:
            try:
                engine.cycle()
            except Exception as e:  # keep the bot alive through bad data
                engine._err(f"cycle error: {e}")
            time.sleep(poll)
    except KeyboardInterrupt:
        with engine.lock:
            engine._save()
        print("\nStopped. State saved. Open live positions stay in your wallet until the bot sells them.")


if __name__ == "__main__":
    main()
