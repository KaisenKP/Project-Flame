from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from datetime import datetime

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

from services.achievement_catalog import AchievementTier


@dataclass(frozen=True, slots=True)
class AchievementCardPayload:
    username: str
    user_id: int
    avatar_bytes: bytes
    achievement_name: str
    achievement_description: str
    achievement_icon: str
    flavor_text: str
    tier: AchievementTier
    unlocked_at: datetime

    def cache_key(self) -> str:
        h = hashlib.sha256()
        h.update(self.username.encode("utf-8"))
        h.update(str(self.user_id).encode("utf-8"))
        h.update(self.achievement_name.encode("utf-8"))
        h.update(self.achievement_description.encode("utf-8"))
        h.update(self.achievement_icon.encode("utf-8"))
        h.update(self.flavor_text.encode("utf-8"))
        h.update(self.tier.value.encode("utf-8"))
        h.update(self.unlocked_at.isoformat().encode("utf-8"))
        h.update(hashlib.sha256(self.avatar_bytes).digest())
        return h.hexdigest()


class AchievementCardRenderer:
    CARD_SIZE = (1200, 420)

    def __init__(self) -> None:
        self._font_cache: dict[tuple[int, bool], ImageFont.FreeTypeFont | ImageFont.ImageFont] = {}
        self._avatar_cache: dict[str, Image.Image] = {}
        self._card_cache: dict[str, bytes] = {}

    def render(self, payload: AchievementCardPayload) -> bytes:
        key = payload.cache_key()
        if key in self._card_cache:
            return self._card_cache[key]

        tier = self._tier_theme(payload.tier)
        card = self._base_bg(tier["bg1"], tier["bg2"])
        draw = ImageDraw.Draw(card)

        draw.rounded_rectangle((18, 18, 1182, 402), radius=30, outline=tier["accent"], width=6)
        self._draw_glow(card, tier["glow"])
        self._draw_avatar(card, payload.avatar_bytes)

        draw.text((216, 42), "ACHIEVEMENT UNLOCKED", fill=(230, 240, 255), font=self._font(28, bold=True))
        draw.text((216, 84), payload.achievement_name, fill=(255, 255, 255), font=self._font(54, bold=True))
        draw.text((216, 154), payload.achievement_description, fill=(208, 221, 255), font=self._font(28, bold=False))

        draw.rounded_rectangle((798, 70, 1128, 232), radius=26, fill=(8, 14, 30, 170), outline=tier["accent"], width=3)
        draw.text((940, 105), payload.achievement_icon, anchor="mm", fill=(255, 255, 255), font=self._font(88, bold=False))
        draw.text((940, 190), payload.tier.value.upper(), anchor="mm", fill=tier["accent"], font=self._font(24, bold=True))

        draw.text((216, 228), f"Unlocked by {payload.username}", fill=(182, 207, 255), font=self._font(24, bold=False))
        draw.text((216, 268), payload.flavor_text, fill=tier["accent"], font=self._font(26, bold=True))
        stamp = payload.unlocked_at.strftime("%Y-%m-%d %H:%M UTC")
        draw.text((216, 326), f"Unlocked at {stamp}", fill=(174, 190, 224), font=self._font(22, bold=False))

        out = io.BytesIO()
        card.save(out, format="PNG", optimize=True)
        data = out.getvalue()
        self._card_cache[key] = data
        if len(self._card_cache) > 256:
            self._card_cache.pop(next(iter(self._card_cache)))
        return data

    def _base_bg(self, c1: tuple[int, int, int], c2: tuple[int, int, int]) -> Image.Image:
        w, h = self.CARD_SIZE
        bg = Image.new("RGBA", (w, h), (0, 0, 0, 255))
        d = ImageDraw.Draw(bg)
        for y in range(h):
            t = y / max(1, h - 1)
            r = int(c1[0] + (c2[0] - c1[0]) * t)
            g = int(c1[1] + (c2[1] - c1[1]) * t)
            b = int(c1[2] + (c2[2] - c1[2]) * t)
            d.line((0, y, w, y), fill=(r, g, b, 255))
        return bg

    def _draw_glow(self, card: Image.Image, color: tuple[int, int, int, int]) -> None:
        glow = Image.new("RGBA", card.size, (0, 0, 0, 0))
        g = ImageDraw.Draw(glow)
        g.ellipse((730, 20, 1180, 390), fill=color)
        card.alpha_composite(glow.filter(ImageFilter.GaussianBlur(30)))

    def _draw_avatar(self, card: Image.Image, avatar_bytes: bytes) -> None:
        ah = hashlib.sha256(avatar_bytes).hexdigest()
        avatar = self._avatar_cache.get(ah)
        if avatar is None:
            raw = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA")
            raw = ImageOps.fit(raw, (160, 160), method=Image.Resampling.LANCZOS)
            mask = Image.new("L", (160, 160), 0)
            ImageDraw.Draw(mask).ellipse((0, 0, 159, 159), fill=255)
            avatar = Image.new("RGBA", (160, 160), (0, 0, 0, 0))
            avatar.paste(raw, (0, 0), mask)
            self._avatar_cache[ah] = avatar
            if len(self._avatar_cache) > 128:
                self._avatar_cache.pop(next(iter(self._avatar_cache)))

        ring = Image.new("RGBA", card.size, (0, 0, 0, 0))
        rd = ImageDraw.Draw(ring)
        rd.ellipse((32, 124, 208, 300), outline=(255, 255, 255, 230), width=5)
        card.alpha_composite(ring)
        card.alpha_composite(avatar, (40, 132))

    def _tier_theme(self, tier: AchievementTier) -> dict[str, tuple[int, int, int] | tuple[int, int, int, int]]:
        themes = {
            AchievementTier.COMMON: ((18, 31, 56), (36, 58, 91), (133, 190, 255), (120, 190, 255, 80)),
            AchievementTier.RARE: ((22, 34, 72), (46, 72, 126), (126, 206, 255), (110, 210, 255, 90)),
            AchievementTier.EPIC: ((43, 23, 74), (84, 41, 131), (208, 136, 255), (205, 135, 255, 110)),
            AchievementTier.LEGENDARY: ((75, 38, 9), (141, 76, 13), (255, 201, 90), (255, 191, 90, 130)),
            AchievementTier.MYTHIC: ((74, 14, 24), (132, 22, 44), (255, 126, 156), (255, 126, 156, 140)),
        }
        c1, c2, accent, glow = themes[tier]
        return {"bg1": c1, "bg2": c2, "accent": accent, "glow": glow}

    def _font(self, size: int, *, bold: bool) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        key = (size, bold)
        if key in self._font_cache:
            return self._font_cache[key]
        candidates = (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        ) if bold else (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        )
        for c in candidates:
            try:
                f = ImageFont.truetype(c, size=size)
                self._font_cache[key] = f
                return f
            except Exception:
                continue
        fb = ImageFont.load_default()
        self._font_cache[key] = fb
        return fb
