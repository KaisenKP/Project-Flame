# What this file is: Safe exception types for the Self Roles / Role Picker feature.
# Last change: 2026-05-29 - Initial user-facing error classes.

from __future__ import annotations


class SelfRoleError(RuntimeError):
    def __init__(self, user_message: str):
        super().__init__(user_message)
        self.user_message = user_message


class SelfRoleStorageError(SelfRoleError):
    pass


class SelfRoleSetupError(SelfRoleError):
    pass


class MissingConfiguredRoleError(SelfRoleError):
    def __init__(self) -> None:
        super().__init__(
            "This role picker needs to be refreshed. Please ask an admin to run `/setup_roles`."
        )


class RolePermissionError(SelfRoleError):
    def __init__(self) -> None:
        super().__init__("I could not update that role because the bot does not have permission.")


class RoleHierarchyError(SelfRoleError):
    def __init__(self) -> None:
        super().__init__("I could not manage one of those roles because it is above the bot's highest role.")
