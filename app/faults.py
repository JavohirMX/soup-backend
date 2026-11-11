"""Deliberate bugs, used only as negative controls.

A test that never fails proves nothing. Each flag re-introduces one specific
bug so we can check that the chaos harness and the silent-failure scenarios
actually detect it. Enable with FAULTS=no_fencing,naive_claim,...
"""

import os
from dataclasses import dataclass

KNOWN = {
    # Worker writes (heartbeat / finish) match on job id only, not (id, epoch, state).
    # The worker also ignores lost-lease signals, i.e. it trusts that it still owns the job.
    "no_fencing",
    # Claim is SELECT-then-UPDATE without SKIP LOCKED and without re-checking state.
    "naive_claim",
    # Idempotency is SELECT-then-INSERT, and the harness drops the unique constraint.
    "check_then_insert",
    # Lease expiry is computed and compared using the worker's (skewed) local clock.
    "worker_clock",
}


@dataclass(frozen=True)
class Faults:
    enabled: frozenset[str] = frozenset()

    @classmethod
    def parse(cls, spec: str | None) -> "Faults":
        names = frozenset(n.strip() for n in (spec or "").split(",") if n.strip())
        unknown = names - KNOWN
        if unknown:
            raise ValueError(f"unknown faults: {sorted(unknown)}")
        return cls(names)

    @classmethod
    def from_env(cls) -> "Faults":
        return cls.parse(os.environ.get("FAULTS"))

    def __contains__(self, name: str) -> bool:
        return name in self.enabled

    def __str__(self) -> str:
        return ",".join(sorted(self.enabled)) or "none"


NONE = Faults()
