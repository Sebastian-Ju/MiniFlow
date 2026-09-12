from __future__ import annotations

import hashlib
import time
from collections import Counter
from math import factorial
from typing import Any

from .broker import MiniFlow


def register_builtin_tasks(flow: MiniFlow) -> None:
    @flow.task("add")
    def add(a: float, b: float) -> dict[str, float]:
        return {"sum": a + b}

    @flow.task("word_count")
    def word_count(text: str) -> dict[str, Any]:
        words = [word.strip(".,!?;:'\"()[]{}").lower() for word in text.split()]
        words = [word for word in words if word]
        counts = Counter(words)
        return {
            "words": len(words),
            "unique_words": len(counts),
            "most_common": counts.most_common(5),
        }

    @flow.task("sha256")
    def sha256(text: str) -> dict[str, str]:
        return {"digest": hashlib.sha256(text.encode("utf-8")).hexdigest()}

    @flow.task("factorial")
    def calculate_factorial(number: int) -> dict[str, int]:
        if not 0 <= number <= 2_000:
            raise ValueError("number must be between 0 and 2000")
        return {"number": number, "factorial": factorial(number)}

    @flow.task("sleep")
    def sleep_task(seconds: float = 1) -> dict[str, float]:
        if not 0 <= seconds <= 10:
            raise ValueError("seconds must be between 0 and 10")
        time.sleep(seconds)
        return {"slept_seconds": seconds}

    @flow.task("unstable_demo")
    def unstable_demo(message: str = "This task intentionally fails") -> None:
        raise RuntimeError(message)
