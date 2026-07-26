"""Application runtime primitives for FlameBot.

The Discord cogs remain compatibility-facing modules while the runtime,
configuration, and deployment boundaries live in this package.
"""

from .config import BotSettings

__all__ = ["BotSettings"]
