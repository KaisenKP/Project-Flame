from __future__ import annotations

from cogs.embed import _is_http_url, _parse_hex_color, _safe_inline


def test_embed_validation_helpers() -> None:
    assert _is_http_url("https://example.com/path")
    assert not _is_http_url("javascript:alert(1)")
    assert _parse_hex_color("#5865F2") == 0x5865F2
    assert _parse_hex_color("bad-color") is None
    assert _safe_inline("hello `world`", 50) == "hello 'world'"
