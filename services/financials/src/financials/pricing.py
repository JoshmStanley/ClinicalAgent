"""USD per million tokens. Verify against https://docs.claude.com before relying on it for billing."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Price:
    input: float
    output: float
    cache_read: float
    cache_write: float


PRICES: dict[str, Price] = {
    "claude-opus-5": Price(5.00, 25.00, 0.50, 6.25),
    "claude-sonnet-5": Price(2.00, 10.00, 0.20, 2.50),
    "claude-haiku-4-5": Price(1.00, 5.00, 0.10, 1.25),
    "claude-fable-5-1": Price(10.00, 50.00, 0.25, 12.50),
}
DEFAULT_PRICE = PRICES["claude-opus-5"]


def cost_usd(model: str, input_tokens: int, output_tokens: int, cache_read: int = 0, cache_write: int = 0) -> float:
    p = PRICES.get(model, DEFAULT_PRICE)
    return (
        input_tokens * p.input + output_tokens * p.output + cache_read * p.cache_read + cache_write * p.cache_write
    ) / 1_000_000
