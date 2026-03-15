from dataclasses import dataclass


@dataclass
class PlayerRuntime:
    user_id: int
    display_name: str
    class_key: str
    adventure_level: int
