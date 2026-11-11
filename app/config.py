import os
import socket
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Config:
    database_url: str = "postgresql://soup:soup@localhost:55432/soup"
    lease_ttl_s: float = 5.0
    heartbeat_s: float = 1.0
    reap_interval_s: float = 1.0
    poll_s: float = 0.05
    max_attempts: int = 5
    # "db": effect row written in the same transaction as the completion.
    # "file": effect appended to a file *before* the completion commits,
    # modelling a non-transactional external side effect.
    effect_mode: str = "db"
    effect_dir: str = "/tmp/soup-effects"
    effect_commit_delay_ms: float = 20.0
    isolation: str = "read_committed"
    worker_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    # Only consulted when the worker_clock fault is enabled.
    clock_skew_s: float = 0.0

    @classmethod
    def from_env(cls) -> "Config":
        d = cls()
        return cls(
            database_url=os.environ.get("DATABASE_URL", d.database_url),
            lease_ttl_s=_env_float("LEASE_TTL_S", d.lease_ttl_s),
            heartbeat_s=_env_float("HEARTBEAT_S", d.heartbeat_s),
            reap_interval_s=_env_float("REAP_INTERVAL_S", d.reap_interval_s),
            poll_s=_env_float("POLL_S", d.poll_s),
            max_attempts=int(os.environ.get("MAX_ATTEMPTS", d.max_attempts)),
            effect_mode=os.environ.get("EFFECT_MODE", d.effect_mode),
            effect_dir=os.environ.get("EFFECT_DIR", d.effect_dir),
            effect_commit_delay_ms=_env_float("EFFECT_COMMIT_DELAY_MS", d.effect_commit_delay_ms),
            isolation=os.environ.get("DB_ISOLATION", d.isolation),
            worker_id=os.environ.get("WORKER_ID") or d.worker_id,
            clock_skew_s=_env_float("WORKER_CLOCK_SKEW_S", d.clock_skew_s),
        )
