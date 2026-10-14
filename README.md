# MemeBot

A self-learning pump.fun trading bot, **Spark**, with a live dashboard. It watches **DexScreener**, **pump.fun** and **Fomo** at the same time. Seven other agents work as Spark's scouts: each studies one side of every launch and reports to Spark, which makes the call. It uses only the Python standard library (3.9+), and everything is in one file.

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

## The scouts

By default only Spark trades. The other agents work for it, each studying one side of every launch that gets past the hard rug checks:

| Scout | Studies |
|---|---|
| **Atlas** | Holders: how spread out they are, how many top holders are fresh wallets that never traded another launch, and groups that bought within the same 1.5 seconds after the launch rush |
| **Bolt** | Flow: whether buying is speeding up or fading over the last 30 seconds, buy sizes, sell pressure, distinct buyers |
| **Cinder** | Creator: wallet age, past launches, past runners and rugs, how much the creator holds |
| **Drift** | Socials: which links (X profile, X community, tweet, Telegram, website), the description, copycat tickers, reused links |
| **Ember** | Dump watch: snipers and bundles still holding, big holders' net selling, known rug wallets, distance from the high |
| **Flux** | Trends: how its theme is doing, and whether it matches what is trending on Fomo or up hard on DEXes |
| **Crest** | Graduation: how fast the curve is filling, and when it would graduate at that pace |

Each scout returns a score from -1 to +1 with a written note. How Spark uses them:
- **Model:** the scores are model features, so Spark learns from results how far to trust each scout.
- **Vetoes:** two red flags block a buy outright. Atlas vetoes 5 or more top holders who bought together and hold 15% or more. Ember vetoes big holders net-dumping 10% or more of supply while money flows out.
- **Before the model is ready:** three red scouts are a no.

The dashboard's scout panel shows:
- the launch Spark is weighing right now, with every scout's note and Spark's verdict
- Spark's recent calls
- each scout's record

Scouting reads only data already in memory, so a full review takes well under a tenth of a millisecond per launch. Spark's decision loop also runs as soon as new trades arrive, up to 4 times a second, instead of on a fixed 1-second tick.

Use `--paper-agents` to have the other agents paper-trade as well, as before.

## The agents (with --paper-agents)

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

1. **Shadow trades:** every launch that passes the hard rug checks is paper-followed every 20 seconds for as long as Spark could buy it, with Spark's real exit rules, costs and reference price. Practice therefore covers every moment a real buy can happen, including the late ones. That gives hundreds of examples an hour without spending anything.
2. **Model:** an online logistic model learns P(profit) from about 35 signals at entry: inflow, buyers, curve progress, holder spread, bundle, snipers, momentum, socials and so on. It combines that with the average win and loss to estimate the expected return after costs. Newer signals:
   - **creator wallet age:** how long ago the creator's wallet first appeared. Wallets funded minutes before the launch are a classic rug sign. Looked up through your RPC for launches that reach 12 buys, at most 90 an hour (about 10 Helius credits each); `dev_lookups_per_hour` in `CFG` changes the cap
   - **unique buyers in the last minute** and **buyers per minute** since launch: real demand versus one bot buying again and again
   - **veteran buyers:** the share of buyers who have traded other launches recently. Bundles and sybil farms use fresh wallets
   - **smart wallets buying now:** known winners from memory among this launch's recent buyers, not only its first 20 seconds
   - **big holders selling:** tokens sold in the last 5 minutes by wallets that held 1% or more

   New signals sit out until they have enough data to be measured, so adding them never upsets what Spark already learned.
3. **Trading on it:** after 200 shadow trades the model replaces Spark's hand-set thresholds, and Spark buys only when it expects at least +3%.
4. **Real money needs proof:** Spark LIVE waits until the model's own picks, scored before their results were known, have made money on average over at least 30 trades. A model that keeps losing is never cleared to trade. Picks are booked the way Spark really trades:
   - no more open at once than Spark's position limit
   - the same buy-rate limit and per-token cooldown
   - each pick scored with the selling style Spark would actually use

   So the proof measures trades Spark could have made, not every good moment it saw.
5. **Reality check:** practice trades start out assuming a 4.25% cost per buy and per sell. Every real fill is measured against the price at the moment Spark decided: what a buy really paid over that price, and what a sell really fetched under it, including slow fills, failed routes and other snipers. After 5 real buys (or sells), practice trades and paper Spark switch to the median of the latest 40 real fills, so Spark learns what is profitable after real-world costs. The dashboard's "Reality check" shows both numbers and the typical fill time for each route.
6. **Automatic stop:** after each closed real trade, Spark compares its latest 20 real trades with its practice picks. If the real ones lose money on average and trail practice by more than 15 points (after at least 10 real trades), Spark LIVE is **benched**: no new real buys, open positions are still managed, and it keeps practising. It buys again by itself once 30 new practice picks, priced with the real costs, average a profit. `--unbench` lifts a bench straight away.

**Learned exits.** Every practice trade runs four selling styles side by side on the same launch: scalp, Spark's own exits, swing and runner. Spark keeps a running score for each style at each stage of the curve (early, mid, late), with recent results counting most. A new position uses whichever style has clearly beaten Spark's own exits: at least 30 practice trades and more than 1% better. The style shows as a badge on the position and in the "How it sells" chips.

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

The hard rug checks always apply. What Spark has learned is saved in `learn.json` (kept out of git), so it survives restarts. `learn.json`, `memory.json`, `state.json` and `spark-report.html` always live next to `memebot.py`, whichever folder you start it from; files an older version saved in the starting folder are moved there once, with a note at startup. Delete that file to start learning from scratch.

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

## Public practice record

Every 10 minutes Spark writes `spark-report.html`, a standalone page with its practice-trade record:
- win rate and average return
- the running sum of the model's blind picks
- the selling style it uses at each curve stage
- the last 50 practice trades

The page is also served at `/report` on the dashboard. It holds no wallet address, keys or controls, so it is safe to upload anywhere or screenshot. Real-money trades are left out unless you start with `--report-live`.

## Alerts (optional)

Set any of the following to get a message on every entry and exit, and when live trading halts:

```bash
export TELEGRAM_BOT_TOKEN=...  TELEGRAM_CHAT_ID=...
export DISCORD_WEBHOOK_URL=...
```

## Live trading

Any agent can have a live twin that trades real money on Solana, for example `--live --live-agents Spark --live-usd 15`.

**Direct pump.fun route (`--direct`).** By default Spark pays in USDC through Jupiter, which needs a quote and a swap request and only routes a token once it knows about it. With `--direct`, pump.fun buys go straight to pump.fun and are paid in SOL:
- PumpPortal's local-transaction API builds the transaction in one request. Spark signs it on your machine, so the key never leaves it.
- It works from a launch's first block.
- PumpPortal charges 0.5% per trade. The reality check measures that along with everything else, and the dashboard shows typical fill times for both routes so you can compare.
- Sells go back to SOL, so the wallet needs SOL beyond the 0.02 kept for fees. With too little SOL, or if the route errors, Spark uses Jupiter instead. After 3 errors in a row it switches the direct route off for the run.
- A price that runs past the slippage limit is treated as a skip, not a fault.
- `python memebot.py --check` simulates a 0.001 SOL direct buy without sending it. There is no daily loss cap unless you set one with `--daily-loss 20` (a percentage of the bankroll). Add `--live-only` to skip the paper agents, so the dashboard shows only your real balance. The paper agents don't learn or feed into the live ones; they're only there for comparison. Put the wallet key in a `.env` file next to `memebot.py` (`MEMEBOT_SOLANA_KEY=...`). Never commit that file; `.gitignore` already excludes it.

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

Run `python memebot.py --check` first. It tests DexScreener, your Solana RPC, the on-chain feed, PumpPortal, Jupiter, the wallet, Fomo and the AI brain without trading, and prints the fix for anything that fails.

All settings can go in a `.env` file next to `memebot.py`, one `NAME=value` per line.

| You see | Do this |
|---|---|
| "No wallet key found" | Create `.env` containing `MEMEBOT_SOLANA_KEY=...` |
| "Jupiter is not answering" at startup | Get a free key at portal.jup.ag and add `JUPITER_API_KEY=...` to `.env` |
| pump.fun shows launches but 0 trades | The bot switches to the on-chain feed on its own. If that also fails, add a Helius `SOLANA_RPC_URL`, or start with `--pump-debug` and share `pump_debug.jsonl` |
| "DexScreener error" light | Your network or firewall is blocking api.dexscreener.com |
| An agent isn't buying | Its card lists the top reasons, for example "too young (40), bundled launch (3)" |

