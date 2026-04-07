# Legacy Preservation Notes

The previous bot identity (Project Pulse / CatBot naming) is intentionally preserved as background compatibility within the codebase.

## What is preserved

- Core runtime flow (startup diagnostics, extension loader, background tasks).
- Existing cogs/services/data model architecture.
- Backward-compatible class aliases in `bot.py`:
  - `PulseBot` -> `FlameBot`
  - `PulseCommandTree` -> `FlameCommandTree`

## Why this exists

This allows incremental migration and future feature reuse without deleting reliable systems.
