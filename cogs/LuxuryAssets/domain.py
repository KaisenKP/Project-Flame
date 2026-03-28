from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Optional


class AssetCategory(StrEnum):
    VEHICLE = "vehicle"
    BOAT = "boat"
    PROPERTY = "property"
    INDUSTRIAL = "industrial"


class AssetTier(StrEnum):
    ENTRY = "entry"
    MID = "mid"
    LUXURY = "luxury"
    ELITE = "elite"
    LEGENDARY = "legendary"


class LoanStatus(StrEnum):
    ACTIVE = "active"
    OVERDUE = "overdue"
    DEFAULTED = "defaulted"
    REPAID = "repaid"


@dataclass(frozen=True, slots=True)
class AssetDefinition:
    asset_key: str
    name: str
    category: AssetCategory
    tier: AssetTier
    value: int
    price: int
    emoji: str
    flavor_text: str
    sort_order: int
    is_active: bool = True
    icon_asset: Optional[str] = None
    background_asset: Optional[str] = None
    overlay_asset: Optional[str] = None
    badge_key: Optional[str] = None


@dataclass(frozen=True, slots=True)
class OwnedAssetView:
    id: int
    asset_key: str
    purchased_at: datetime
    purchase_price: int
    showcase_slot: Optional[int]
    is_showcased: bool
    is_seized: bool


@dataclass(frozen=True, slots=True)
class LoanCapacitySnapshot:
    total_asset_value: int
    collateral_ratio: float
    borrow_limit: int
    active_loan_balance: int
    available_to_borrow: int


@dataclass(frozen=True, slots=True)
class LoanSnapshot:
    loan_id: int
    principal_amount: int
    interest_rate: float
    total_due: int
    remaining_balance: int
    issued_at: datetime
    due_at: datetime
    status: LoanStatus
    last_interest_applied_at: datetime
    debt_recovery_mode: bool
    recovery_rate_bp: int


@dataclass(frozen=True, slots=True)
class LuxuryOverviewSnapshot:
    wallet_silver: int
    total_asset_value: int
    net_worth: int
    owned_assets_count: int
    showcased_assets_count: int
    active_loan: LoanSnapshot | None


@dataclass(frozen=True, slots=True)
class SeizureResult:
    seized_asset_ids: list[int]
    seized_value_total: int
    remaining_balance_after_seizure: int
