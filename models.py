# models.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

@dataclass
class Job:
    id: str
    command: str
    state: str = "pending"   # pending | processing | completed | failed | dead
    attempts: int = 0
    max_retries: int = 3
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    next_run_at: Optional[str] = None
    exit_code: Optional[int] = None
    error: Optional[str] = None
    worker_id: Optional[str] = None
    lease_until: Optional[str] = None
