from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from typing import Iterable, Optional

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps


CARD_SIZE = (1100, 620)


@dataclass(frozen=True)
class JobDisplay:
    slot: int
    label: str


@dataclass(frozen=True)
class ProfileCardPayload:
    username: str
    user_id: int
    vip: bool
    level: int
    xp_into_level: int
    xp_to_next: int
    xp_total: int
    silver: int
    diamonds: int
    stamina_current: int
    stamina_max: int
    jobs: tuple[JobDisplay, ...]
    background_key: str
    avatar_bytes: bytes

    def cache_key(self) -> str:
        h = hashlib.sha256()
        h.update(self.username.encode("utf-8"))
        h.update(str(self.user_id).encode("utf-8"))
        h.update(str(self.vip).encode("utf-8"))
        h.update(str(self.level).encode("utf-8"))
        h.update(str(self.xp_into_level).encode("utf-8"))
        h.update(str(self.xp_to_next).encode("utf-8"))
        h.update(str(self.xp_total).encode("utf-8"))
        h.update(str(self.silver).encode("utf-8"))
        h.update(str(self.diamonds).encode("utf-8"))
        h.update(str(self.stamina_current).encode("utf-8"))
        h.update(str(self.stamina_max).encode("utf-8"))
        h.update(self.background_key.encode("utf-8"))
        for j in self.jobs:
            h.update(f"{j.slot}:{j.label}".encode("utf-8"))
        h.update(hashlib.sha256(self.avatar_bytes).digest())
        return h.hexdigest()


class ProfileCardRenderer:
    def __init__(self) -> None:
        self._font_cache: dict[tuple[int, bool], ImageFont.FreeTypeFont | ImageFont.ImageFont] = {}
        self._bg_cache: dict[str, Image.Image] = {}
        self._avatar_cache: dict[str, Image.Image] = {}
        self._card_cache: dict[str, bytes] = {}

    def render(self, payload: ProfileCardPayload) -> bytes:
        key = payload.cache_key()
        if key in self._card_cache:
            return self._card_cache[key]

        card = self._background(payload.background_key).copy()
        draw = ImageDraw.Draw(card)

        self._draw_glass_panel(card, (36, 28, 1064, 592), radius=34, alpha=100)
        self._draw_header(draw, payload)
        self._draw_avatar(card, payload.avatar_bytes)
        self._draw_level_panel(draw, payload)
        self._draw_stats_panel(draw, payload)

        out = io.BytesIO()
        card.save(out, format="PNG", optimize=True)
        data = out.getvalue()
        self._card_cache[key] = data

        if len(self._card_cache) > 256:
            self._card_cache.pop(next(iter(self._card_cache)))
        return data

    def _draw_header(self, draw: ImageDraw.ImageDraw, payload: ProfileCardPayload) -> None:
        draw.text((286, 64), payload.username, fill=(255, 255, 255), font=self._font(58, bold=True))
        subtitle = "PLAYER CALLING CARD"
        draw.text((290, 128), subtitle, fill=(195, 215, 255), font=self._font(24, bold=False))
        draw.text((290, 156), "Season Rank Snapshot", fill=(154, 183, 234), font=self._font(20, bold=False))
        if payload.vip:
            self._rounded_rect(draw, (860, 56, 1024, 108), 20, fill=(255, 208, 80, 240))
            draw.text((888, 70), "VIP", fill=(65, 38, 8), font=self._font(30, bold=True))

    def _draw_avatar(self, card: Image.Image, avatar_bytes: bytes) -> None:
        ah = hashlib.sha256(avatar_bytes).hexdigest()
        avatar = self._avatar_cache.get(ah)
        if avatar is None:
            raw = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA")
            raw = ImageOps.fit(raw, (220, 220), method=Image.Resampling.LANCZOS)
            mask = Image.new("L", (220, 220), 0)
            mdraw = ImageDraw.Draw(mask)
            mdraw.ellipse((0, 0, 219, 219), fill=255)
            circ = Image.new("RGBA", (220, 220), (0, 0, 0, 0))
            circ.paste(raw, (0, 0), mask)
            avatar = circ
            self._avatar_cache[ah] = avatar
            if len(self._avatar_cache) > 128:
                self._avatar_cache.pop(next(iter(self._avatar_cache)))

        glow = Image.new("RGBA", card.size, (0, 0, 0, 0))
        gdraw = ImageDraw.Draw(glow)
        gdraw.ellipse((80, 156, 360, 436), fill=(111, 171, 255, 90))
        glow = glow.filter(ImageFilter.GaussianBlur(20))
        card.alpha_composite(glow)

        ring = Image.new("RGBA", card.size, (0, 0, 0, 0))
        rdraw = ImageDraw.Draw(ring)
        rdraw.ellipse((106, 182, 334, 410), outline=(255, 255, 255, 220), width=6)
        card.alpha_composite(ring)
        card.alpha_composite(avatar, (110, 186))

    def _draw_level_panel(self, draw: ImageDraw.ImageDraw, payload: ProfileCardPayload) -> None:
        self._rounded_rect(draw, (372, 188, 1020, 360), 24, fill=(18, 28, 48, 190))
        draw.text((404, 216), f"LEVEL {payload.level:,}", fill=(255, 255, 255), font=self._font(42, bold=True))

        bar_box = (408, 280, 986, 318)
        self._rounded_rect(draw, bar_box, 18, fill=(34, 51, 84, 235))

        progress = 0.0
        if payload.xp_to_next > 0:
            progress = max(0.0, min(payload.xp_into_level / payload.xp_to_next, 1.0))

        filled_w = int((bar_box[2] - bar_box[0]) * progress)
        if filled_w > 0:
            fill_box = (bar_box[0], bar_box[1], bar_box[0] + filled_w, bar_box[3])
            self._rounded_rect(draw, fill_box, 18, fill=(76, 173, 255, 255))

        draw.text(
            (410, 326),
            f"XP {payload.xp_into_level:,}/{payload.xp_to_next:,}   •   Total {payload.xp_total:,}",
            fill=(202, 222, 255),
            font=self._font(24, bold=False),
        )

    def _draw_stats_panel(self, draw: ImageDraw.ImageDraw, payload: ProfileCardPayload) -> None:
        self._rounded_rect(draw, (372, 382, 1020, 560), 24, fill=(14, 22, 38, 190))

        draw.text((404, 404), "WALLET", fill=(148, 181, 255), font=self._font(21, bold=True))
        wallet_right = 638
        self._draw_wallet_row(
            draw,
            x=404,
            y=434,
            max_right=wallet_right,
            label="Silver",
            value=self._format_compact(payload.silver),
            accent=(230, 236, 255),
            pill=(70, 95, 170, 214),
            preferred_size=34,
            min_size=20,
            bold=True,
        )
        self._draw_wallet_row(
            draw,
            x=404,
            y=474,
            max_right=wallet_right,
            label="Diamonds",
            value=self._format_compact(payload.diamonds),
            accent=(196, 236, 255),
            pill=(61, 127, 166, 214),
            preferred_size=30,
            min_size=19,
            bold=False,
        )

        draw.line((640, 404, 640, 534), fill=(87, 118, 179, 170), width=2)

        draw.text((648, 404), "STAMINA", fill=(148, 181, 255), font=self._font(21, bold=True))
        stamina_ratio = 0.0 if payload.stamina_max <= 0 else max(0.0, min(payload.stamina_current / payload.stamina_max, 1.0))
        stamina_pct = int(round(stamina_ratio * 100))
        draw.text(
            (648, 442),
            f"{payload.stamina_current:,}/{payload.stamina_max:,}",
            fill=(255, 255, 255),
            font=self._font(36, bold=True),
        )
        draw.text((648, 474), f"{stamina_pct}% charged", fill=(173, 226, 209), font=self._font(21, bold=False))
        bar = (648, 492, 804, 516)
        self._rounded_rect(draw, bar, 10, fill=(45, 59, 88, 230))
        fill_w = int((bar[2] - bar[0]) * stamina_ratio)
        if fill_w > 0:
            if stamina_ratio < 0.25:
                stamina_fill = (255, 138, 138, 255)
            elif stamina_ratio < 0.6:
                stamina_fill = (246, 200, 119, 255)
            else:
                stamina_fill = (75, 214, 164, 255)
            self._rounded_rect(draw, (bar[0], bar[1], bar[0] + fill_w, bar[3]), 10, fill=stamina_fill)

        draw.text((828, 404), "JOBS", fill=(148, 181, 255), font=self._font(21, bold=True))
        y = 438
        if payload.jobs:
            for j in payload.jobs[:3]:
                self._rounded_rect(draw, (820, y - 4, 1012, y + 28), 10, fill=(30, 44, 72, 225))
                self._rounded_rect(draw, (826, y, 856, y + 22), 8, fill=(70, 107, 185, 230))
                draw.text((834, y + 1), f"S{j.slot}", fill=(245, 250, 255), font=self._font(17, bold=True))
                label = self._truncate_text(f"{j.label}", self._font(20, bold=False), max_width=146)
                draw.text((862, y), label, fill=(242, 249, 255), font=self._font(20, bold=False))
                y += 34
        else:
            draw.text((828, y), "No job equipped", fill=(242, 249, 255), font=self._font(23, bold=False))

        draw.text((56, 562), f"USER ID {payload.user_id}", fill=(200, 213, 244), font=self._font(18, bold=False))

    def _background(self, key: str) -> Image.Image:
        bg = self._bg_cache.get(key)
        if bg is not None:
            return bg

        w, h = CARD_SIZE
        if key == "royal_sunrise":
            c1, c2, c3 = (79, 46, 137), (218, 92, 123), (255, 200, 120)
        elif key == "season_winter":
            c1, c2, c3 = (10, 46, 92), (27, 108, 181), (140, 220, 255)
        elif key == "store_obsidian":
            c1, c2, c3 = (16, 16, 20), (40, 40, 57), (92, 96, 130)
        else:
            c1, c2, c3 = (22, 34, 66), (48, 87, 167), (94, 170, 245)

        grad = Image.new("RGBA", (w, h), (0, 0, 0, 255))
        gdraw = ImageDraw.Draw(grad)
        for y in range(h):
            t = y / max(h - 1, 1)
            if t < 0.55:
                u = t / 0.55
                r = int(c1[0] + (c2[0] - c1[0]) * u)
                g = int(c1[1] + (c2[1] - c1[1]) * u)
                b = int(c1[2] + (c2[2] - c1[2]) * u)
            else:
                u = (t - 0.55) / 0.45
                r = int(c2[0] + (c3[0] - c2[0]) * u)
                g = int(c2[1] + (c3[1] - c2[1]) * u)
                b = int(c2[2] + (c3[2] - c2[2]) * u)
            gdraw.line((0, y, w, y), fill=(r, g, b, 255))

        overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        odraw = ImageDraw.Draw(overlay)
        odraw.ellipse((-120, -220, 580, 500), fill=(255, 255, 255, 30))
        odraw.ellipse((740, -160, 1360, 420), fill=(255, 255, 255, 24))
        odraw.ellipse((620, 300, 1320, 900), fill=(8, 14, 36, 70))
        grad.alpha_composite(overlay)

        self._bg_cache[key] = grad
        return grad

    def _draw_glass_panel(self, card: Image.Image, box: tuple[int, int, int, int], *, radius: int, alpha: int) -> None:
        panel = Image.new("RGBA", card.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(panel)
        self._rounded_rect(draw, box, radius, fill=(8, 13, 28, alpha))
        card.alpha_composite(panel)

    def _rounded_rect(
        self,
        draw: ImageDraw.ImageDraw,
        box: tuple[int, int, int, int],
        radius: int,
        *,
        fill: tuple[int, int, int, int],
    ) -> None:
        draw.rounded_rectangle(box, radius=radius, fill=fill)

    def _font(self, size: int, *, bold: bool) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        key = (size, bold)
        if key in self._font_cache:
            return self._font_cache[key]

        candidates: Iterable[str]
        if bold:
            candidates = (
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            )
        else:
            candidates = (
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            )

        for c in candidates:
            try:
                font = ImageFont.truetype(c, size=size)
                self._font_cache[key] = font
                return font
            except Exception:
                continue

        fallback = ImageFont.load_default()
        self._font_cache[key] = fallback
        return fallback

    def _format_compact(self, n: int) -> str:
        value = abs(int(n))
        sign = "-" if int(n) < 0 else ""
        if value >= 1_000_000_000_000:
            return f"{sign}{value / 1_000_000_000_000:.2f}T"
        if value >= 1_000_000_000:
            return f"{sign}{value / 1_000_000_000:.2f}B"
        if value >= 1_000_000:
            return f"{sign}{value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"{sign}{value / 1_000:.1f}K"
        return f"{int(n):,}"

    def _text_size(self, draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont) -> tuple[int, int]:
        x0, y0, x1, y1 = draw.textbbox((0, 0), text, font=font)
        return max(0, x1 - x0), max(0, y1 - y0)

    def _fit_font(
        self,
        draw: ImageDraw.ImageDraw,
        *,
        text: str,
        max_width: int,
        preferred_size: int,
        min_size: int,
        bold: bool,
    ) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        for size in range(preferred_size, min_size - 1, -1):
            font = self._font(size, bold=bold)
            w, _ = self._text_size(draw, text, font)
            if w <= max_width:
                return font
        return self._font(min_size, bold=bold)

    def _truncate_text(
        self,
        text: str,
        font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
        *,
        max_width: int,
    ) -> str:
        if not text:
            return text
        dummy = Image.new("RGBA", (8, 8), (0, 0, 0, 0))
        draw = ImageDraw.Draw(dummy)
        w, _ = self._text_size(draw, text, font)
        if w <= max_width:
            return text
        ellipsis = "…"
        lo, hi = 0, len(text)
        best = ellipsis
        while lo <= hi:
            mid = (lo + hi) // 2
            candidate = text[:mid].rstrip() + ellipsis
            cw, _ = self._text_size(draw, candidate, font)
            if cw <= max_width:
                best = candidate
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def _draw_wallet_row(
        self,
        draw: ImageDraw.ImageDraw,
        *,
        x: int,
        y: int,
        max_right: int,
        label: str,
        value: str,
        accent: tuple[int, int, int],
        pill: tuple[int, int, int, int],
        preferred_size: int,
        min_size: int,
        bold: bool,
    ) -> None:
        label_font = self._font(20, bold=True)
        label_text = f"{label}"
        value_text = value
        label_w, _ = self._text_size(draw, label_text, label_font)
        value_max_w = max(24, max_right - (x + label_w + 34))
        value_font = self._fit_font(
            draw,
            text=value_text,
            max_width=value_max_w,
            preferred_size=preferred_size,
            min_size=min_size,
            bold=bold,
        )
        value_w, value_h = self._text_size(draw, value_text, value_font)
        value_x = max(x + label_w + 24, max_right - value_w)
        pill_left = x
        pill_top = y + 1
        pill_right = x + 12
        pill_bottom = y + 29
        self._rounded_rect(draw, (pill_left, pill_top, pill_right, pill_bottom), 6, fill=pill)
        draw.text((x + 18, y), label_text, fill=accent, font=label_font)
        draw.text((value_x, y + max(0, (28 - value_h) // 2)), value_text, fill=(255, 255, 255), font=value_font)
