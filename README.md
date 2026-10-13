# MemeBot

A memecoin paper-trading engine with a live dashboard. It watches **DexScreener**, **pump.fun** and **Fomo** at the same time and runs ten strategy agents side by side, each with its own $200 paper balance. It uses only the Python standard library (3.9+), and everything is in one file.

```bash
python memebot.py --demo     # offline preview with synthetic data
python memebot.py            # real market data, paper fills
```

Then open http://127.0.0.1:8000.

## What it watches

| Feed | How | Setup |
|---|---|---|
| DexScreener | New and boosted tokens on Solana, Base, Ethereum and BSC, refreshed every 30s | none |
| pump.fun | Every launch and every bonding-curve trade in real time, plus graduations to PumpSwap, over the free [PumpPortal](https://pumpportal.fun) websocket | none (`PUMPPORTAL_API_KEY` optional) |
| Fomo (fomo.family) | The tokens Fomo users are trading right now, from the trending board on [FOMO API](https://fomoapi.io), an independent API for Fomo data | `FOMO_API_KEY` (free tier) |

Use `--no-pump` or `--no-fomo` to turn a feed off. If `FOMO_API_KEY` isn't set, the Fomo watcher stays off and the bot tells you at startup.

## The agents

| Agent | Feed | Idea |
|---|---|---|
| Atlas, Bolt, Cinder, Drift, Ember, Flux | DEX | The original six: momentum, scalp, early runner, calmer, dip buyer, volume breakout |
| **Spark** | pump.fun curve | A launch 45s to 10min old, curve 8 to 55% filled, real buying from many distinct wallets |
| **Crest** | pump.fun curve | Launches 60 to 93% of the way to graduation with strong net inflow |
| **Grad** | graduates | Tokens that graduated from pump.fun within the last 2 hours, traded on their new DEX pool |
| **Herd** | Fomo | Tokens in the top 25 of Fomo's trending board that also pass the DEX filters |

Before a pump.fun entry, these checks are computed live from the trade stream:
- the creator has not sold
- the creator holds only a small share of supply
- no single wallet, and not the top 10 together, holds too much
- the volume isn't a few wallets trading back and forth
- the price is not already sliding off its high

If the creator sells while an agent holds the token, the curve agents exit at once. A curve position that graduates moves over to DEX pricing automatically.

## Better fills and exits

- **Curve fills** use exact constant-product math on pump.fun's virtual reserves, with the 1.25% curve fee and a slippage allowance for snipers who get in ahead of you.
- **DEX fills** use the original cost model: price impact from pool size, DEX fee, latency slippage and token tax.
- **Scaling out:** several agents sell half at a first target, then hold the rest with a break-even stop and a trailing stop.
- pump.fun entries and exits run on a 2-second loop. The DEX loop runs every 30 seconds.

## Dashboard

- A pump.fun panel lists the hottest live launches: curve progress, buyers, buys and sells over 5 minutes, net SOL flow, and flags such as "dev sold".
- A Fomo trending panel shows each token's 1h move and liquidity.
- Source badges show where each position and trade came from.
- The win-rate card adds profit factor and max drawdown. P&L is broken down by source.

## Alerts (optional)

Set any of the following to get a message on every entry and exit, and when live trading halts:

```bash
export TELEGRAM_BOT_TOKEN=...  TELEGRAM_CHAT_ID=...
export DISCORD_WEBHOOK_URL=...
```

## Live trading

This works the same way as before. Only DEX agents on Solana can have a live twin, for example `--live --live-agents Bolt,Grad`. The bonding-curve agents are always paper.

## Tests

```bash
python -m unittest discover -s tests
```

The tests cover:
- the curve math
- the pump.fun message handling
- the websocket client, against a local mock server with fragmented frames, ping/pong and close
- Fomo response parsing
- partial take-profit accounting
- the curve-to-DEX hand-over on graduation
- the dashboard's request guards
- a full demo run

## Caveats

- PumpPortal and FOMO API are third-party services. Their message formats can change, so the parsers are written defensively. If a feed goes quiet, check the activity log.
- Paper results are optimistic for pump.fun. Real snipers and bots are faster than a 2-second loop.
