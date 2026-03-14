# Chatbot Codebase Overview

## Runtime boot flow
- `main.py` is the process entry point. It configures Rich logging, reads `BOT_TOKEN` from environment, optionally runs `tables.py` migration helper, builds the bot via `build_bot_from_env()`, and starts the Discord client with graceful SIGINT/SIGTERM shutdown handling.
- `bot.py` defines `PulseBot`, auto-discovers cogs in the `cogs/` tree by looking for `setup()` functions, ensures DB schema (`Base.metadata.create_all(checkfirst=True)`), syncs slash commands, and runs a heartbeat background task.

## Data layer
- `db/engine.py` builds a singleton SQLAlchemy async engine/sessionmaker from env vars (`DB_HOST`, `DB_NAME`, etc.) and supports sanitizing host/port values.
- `db/models.py` is the central schema and includes tables for users/xp/wallets, activity and voice sessions, anti-raid sentinel logs, jobs/stamina/tools, items/effects/shop/lootboxes, crowns, and the Business system.
- `services/db.py` exposes shared async sessionmaker access used across cogs.

## Shared domain services
- XP and activity: `services/xp.py`, `services/xp_award.py`, and `services/activity_rules.py` implement level curves, xp bar rendering, cooldowned XP awarding, and chat/voice XP conversion rules.
- Jobs/work: `services/jobs_core.py`, `services/job_progression.py`, `services/jobs_balance.py`, `services/jobs_embeds.py`, and `services/jobs_views.py` define job metadata, progression/prestige math, balancing formulas, embed builders, and interactive views.
- Economy/items: `services/items_catalog.py`, `services/items_inventory.py`, `services/items_models.py`, `services/stamina.py`, and `services/users.py` handle item definitions and effects, inventory CRUD, stamina regen/use, and user row creation.
- Utility/config: `services/config.py`, `services/vip.py`, and `services/message_counter.py` centralize env config and helper logic.

## Cog responsibilities (Discord-facing features)
- Core/economy/admin: `ping.py`, `economy.py`, `CurrencyAdminCog.py`, `profile.py`, `daily.py`, `work.py`, `jobs.py`, `shop.py`, `inventory.py`, `stamina_tick.py`.
- Games/events: `coinflip.py`, `slots.py`, `pickpocket.py`, `lootbox.py`, `dropparty.py`, `TwentyOne.py`.
- Activity/rankings: `activity_listener.py`, `activity_tracker.py`, `vctime.py`, `leaderboards.py`, `leaderboard_sync.py`, `monthly_activity_champions.py`, `LevelRewards.py`, `crowns.py`.
- Ops/moderation: `sentinel.py`, `tickets.py`, `vip_sync.py`, `debug_progression.py`, `job_xp_migration.py`.
- Business subsystem: `cogs/Business/` has `core.py` (types/snapshots), `runtime.py` (tick/run engine), and `cog.py` (UI + `/business` command).

## Configuration and persisted JSON
- `data/` stores runtime JSON payloads like sentinel config, daily claims, level reward markers, and leaderboard snapshots for guilds.

