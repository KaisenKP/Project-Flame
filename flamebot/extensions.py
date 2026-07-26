from __future__ import annotations

import fnmatch
from collections.abc import Iterable


# Keep extension ownership explicit. Filesystem discovery made a newly added
# setup() function production code by accident and made startup order opaque.
DEFAULT_MODULES: tuple[str, ...] = (
    "activity_listener",
    "admin_restart",
    "ban",
    "community_tools",
    "embed",
    "features",
    "moderation",
    "ping",
    "selfroles",
    "sentinel",
    "tickets",
    "youtube_notifications",
)


def configured_extensions(
    *,
    package: str = "cogs",
    allow_patterns: Iterable[str] = (),
    deny_patterns: Iterable[str] = (),
) -> tuple[str, ...]:
    """Return the registered extensions after optional deployment filters."""

    allow = tuple(pattern for pattern in allow_patterns if pattern)
    deny = tuple(pattern for pattern in deny_patterns if pattern)
    selected = tuple(f"{package}.{module}" for module in DEFAULT_MODULES)
    if allow:
        selected = tuple(
            extension
            for extension in selected
            if any(fnmatch.fnmatch(extension, pattern) for pattern in allow)
        )
    if deny:
        selected = tuple(
            extension
            for extension in selected
            if not any(fnmatch.fnmatch(extension, pattern) for pattern in deny)
        )
    return selected
