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

pump.fun also has a second, independent source: the bot can read pump.fun's own events straight from the chain over a Solana RPC websocket. This turns on by itself if PumpPortal sends launches but no trades. Use `--pump-rpc` to turn it on from the start; it also sees bundled launch-block buys directly. The free public RPC may throttle it, so a free [Helius](https://helius.dev) key works better: put `SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=...` in `.env`.

Use `--no-pump` or `--no-fomo` to turn a feed off. If `FOMO_API_KEY` isn't set, the Fomo watcher stays off and the bot tells you at startup.

## The agents

| Agent | Feed | Idea |
|---|---|---|
| Atlas, Bolt, Cinder, Drift, Ember, Flux | DEX | The original six: momentum, scalp, early runner, calmer, dip buyer, volume breakout |
| **Spark** | pump.fun curve | A launch 45s to 10min old, curve 8 to 55% filled, real buying from many distinct wallets |
| **Crest** | pump.fun curve | Launches 60 to 93% of the way to graduation with strong net inflow |
| **Grad** | graduates | Tokens that graduated from pump.fun within the last 2 hours, traded on their new DEX pool |
| **Herd** | Fomo | Tokens in the top 25 of Fomo's trending board that also pass the DEX filters |

Before a pump.fun entry, these checks are computed live from the trade stream. Each one blocks the buy:

| Check | Catches |
|---|---|
| Bundle detection | Supply bought in the launch block. Those buys happen before anyone can subscribe to the token, so the bot finds them by comparing the curve at creation with the curve at the first trade it sees. |
| Sniper holdings | How much the wallets that bought in the first 20 seconds still hold |
| Serial launcher | A creator wallet that has launched other tokens this session |
| Creator | The creator has sold, or holds too much |
| Concentration | One wallet, or the top 10 together, holding too much |
| Copycat ticker | An older launch with the same ticker is already live |
| Socials | No Twitter, Telegram or website in the metadata, or links reused from another launch (a common scam pattern) |
| Bot activity | Many identical-size buys, a few wallets churning volume, or one whale making most of the move |
| Momentum | Thin curve, too few holders, fading buys over the last minute, dumping off the high |

`--risk low|medium|high|degen` sets how picky Spark is (default `degen`). `degen` is for trenching: it buys launches from 8 seconds old with real buying and exits faster. Once a trade is up 33%, it sells if the price falls back to 1.3x, so it can't give back that profit. `low` uses the strictest settings, and `high` lets through younger, thinner launches. Every level keeps the hard rug checks: creator selling, bundled supply, serial launchers, copycat tickers and reused socials. Common quick-buy sizes (0.1, 0.5, 1 SOL and so on) don't count as bot buys.

If the creator sells while an agent holds the token, the curve agents exit at once. A curve position that graduates moves over to DEX pricing automatically.

**Live pump.fun trading:** `--live --live-agents Spark --live-usd 15` makes Spark trade real USDC. Jupiter routes each buy to the bonding curve, with a 10% slippage cap, and a buy is refused if the route price is more than 10% above the curve price. After a full sell, the bot closes the empty token account to get its rent (about 0.002 SOL) back. With a small bankroll it skips partial take-profits that would be below the $5 minimum order.

## Spark learns as it goes

Spark trains a small model on what actually happens to pump.fun launches:

1. **Shadow trades:** every launch that passes the hard rug checks is paper-followed several times, at different moments, with Spark's real exit rules, costs and reference price. That gives hundreds of examples an hour without spending anything.
2. **Model:** an online logistic model learns P(profit) from about 20 signals at entry: inflow, buyers, curve progress, holder spread, bundle, snipers, momentum, socials and so on. It combines that with the average win and loss to estimate the expected return after costs.
3. **Trading on it:** after 200 shadow trades the model replaces Spark's hand-set thresholds, and Spark buys only when it expects at least +3%.
4. **Real money needs proof:** Spark LIVE waits until the model's own picks, scored before their results were known, have made money on average over at least 30 trades. A model that keeps losing is never cleared to trade.

**Memory.** Spark judges every launch it followed when the launch ends:
- **ran:** it graduated, or got at least halfway up the curve
- **rugged:** the creator or the first-seconds wallets sold out, and it gave back most of its gain since launch
- **dud:** anything else

Each verdict is credited to the creator and to every wallet that bought in the first 20 seconds. From that, Spark skips creators who rugged before and launches where several known rug wallets bought early. Wallets that keep getting into runners early count as "smart money", and the model learns how much weight to give them. The memory is saved in `memory.json` (kept out of git) and builds up across restarts.

**Hot themes.** Every judged launch also credits the words in its name and ticker. Spark tracks which themes are running lately compared with the average; a theme's record halves every 6 hours, so stale narratives fade. A launch's theme strength is one of the model's signals.

**AI brain (optional).** With `pip install anthropic` and `ANTHROPIC_API_KEY=...` in `.env`, Claude reads promising launches (15+ buyers, past the hard checks): name, ticker, description and socials, plus the current hot themes. It scores each one 0–10 for memeability, narrative fit and scam-likeness. Those scores are model features, so Spark learns from results how much to trust them. A scam score of 9 or more is an outright veto. Each launch is rated once, capped at 30 an hour.
- **Model:** the default is Claude Opus 5.5, about $0.01 per rating, so roughly $7 a day at the full cap. Set `MEMEBOT_BRAIN_MODEL=claude-haiku-5-5` to cut that about 40×, or `MEMEBOT_BRAIN_PER_HOUR` to change the cap.
- **Refusals:** on Opus and Sonnet models, server-side refusal fallbacks are switched on (`fallbacks: "default"`).
- **Untrusted text:** token text is treated as untrusted data in the prompt.

The hard rug checks always apply. What Spark has learned is saved in `learn.json` (kept out of git), so it survives restarts. Delete that file to start learning from scratch.

## Better fills and exits

- **Curve fills** use exact constant-product math on pump.fun's virtual reserves, with the 1.25% curve fee and a slippage allowance for snipers who get in ahead of you.
- **DEX fills** use the original cost model: price impact from pool size, DEX fee, latency slippage and token tax.
- **Scaling out:** several agents sell half at a first target, then hold the rest with a break-even stop and a trailing stop.
- pump.fun entries and exits run on a 2-second loop. The DEX loop runs every 30 seconds.

## Dashboard

- A status line shows what the bot is doing right now, how fresh the data is and when the next scan runs. Status lights show the state of DexScreener, pump.fun and Fomo, and a banner explains any connection problem.
- Each agent card shows why it isn't buying, for example "too young (40), bundled launch (3)".
- The page keeps your scroll position while it refreshes.
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

Any agent can have a live twin that trades real USDC on Solana, for example `--live --live-agents Spark --live-usd 15`. There is no daily loss cap unless you set one with `--daily-loss 20` (a percentage of the bankroll). Add `--live-only` to skip the paper agents, so the dashboard shows only your real balance. The paper agents don't learn or feed into the live ones; they're only there for comparison. Put the wallet key in a `.env` file next to `memebot.py` (`MEMEBOT_SOLANA_KEY=...`). Never commit that file; `.gitignore` already excludes it.

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

## Troubleshooting

All settings can go in a `.env` file next to `memebot.py`, one `NAME=value` per line.

| You see | Do this |
|---|---|
| "No wallet key found" | Create `.env` containing `MEMEBOT_SOLANA_KEY=...` |
| "Jupiter is not answering" at startup | Get a free key at portal.jup.ag and add `JUPITER_API_KEY=...` to `.env` |
| pump.fun shows launches but 0 trades | The bot switches to the on-chain feed on its own. If that also fails, add a Helius `SOLANA_RPC_URL`, or start with `--pump-debug` and share `pump_debug.jsonl` |
| "DexScreener error" light | Your network or firewall is blocking api.dexscreener.com |
| An agent isn't buying | Its card lists the top reasons, for example "too young (40), bundled launch (3)" |

