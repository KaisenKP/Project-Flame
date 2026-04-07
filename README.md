# FlameBot

FlameBot is now the **primary bot identity** for this repository.

This transition keeps the existing production-tested Discord bot systems in place while introducing a clean project framing that makes FlameBot the active runtime layer.

## Repository strategy

- **Primary runtime:** `main.py` + `bot.py` now boot FlameBot by default.
- **Preserved foundation:** Existing cogs, services, DB models, and utility modules remain intact and reusable.
- **Legacy compatibility:** Historical `Pulse*` class names are retained as compatibility aliases so older imports/extensions can still resolve during migration.

## Project layout

- `main.py` — process entrypoint and startup lifecycle.
- `bot.py` — FlameBot runtime implementation, extension loading, command sync, and background orchestration.
- `cogs/` — active command/event modules.
- `services/` — reusable domain services and startup infrastructure.
- `db/` — SQLAlchemy engine and model layer.
- `legacy/` — migration notes and identity mapping for retained legacy naming.

## Migration intent

This repository intentionally avoids destructive rewrites. Legacy systems are preserved so features can be reactivated, migrated, or selectively reused without reconstructing architecture from scratch.
