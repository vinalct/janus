from __future__ import annotations

from janus.hooks.ibge import HOOK_ID as IBGE_SIDRA_FLAT_HOOK_ID
from janus.hooks.ibge import LEGACY_HOOK_IDS as IBGE_LEGACY_HOOK_IDS
from janus.hooks.ibge import IbgePibBrasilHook
from janus.strategies.base import SourceHook

IBGE_PIB_BRASIL_HOOK_ID = IBGE_LEGACY_HOOK_IDS[0]


def built_in_hooks() -> tuple[tuple[str, SourceHook], ...]:
    bindings: list[tuple[str, SourceHook]] = [(IBGE_SIDRA_FLAT_HOOK_ID, IbgePibBrasilHook())]
    bindings.extend((hook_id, IbgePibBrasilHook()) for hook_id in IBGE_LEGACY_HOOK_IDS)
    return tuple(bindings)


__all__ = [
    "IBGE_LEGACY_HOOK_IDS",
    "IBGE_PIB_BRASIL_HOOK_ID",
    "IBGE_SIDRA_FLAT_HOOK_ID",
    "IbgePibBrasilHook",
    "built_in_hooks",
]
