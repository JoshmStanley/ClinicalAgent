"""USD per million tokens. Verify against https://docs.claude.com before relying on it for billing.

Shared by financials (the usage ledger) and the agent (per-generation cost in
Langfuse) so both price a run the same way.
"""

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


def cost_breakdown(
    model: str, input_tokens: int, output_tokens: int, cache_read: int = 0, cache_write: int = 0
) -> dict[str, float]:
    """USD per usage component plus `total`."""
    p = PRICES.get(model, DEFAULT_PRICE)
    parts = {
        "input": input_tokens * p.input / 1_000_000,
        "output": output_tokens * p.output / 1_000_000,
        "cache_read_input_tokens": cache_read * p.cache_read / 1_000_000,
        "cache_creation_input_tokens": cache_write * p.cache_write / 1_000_000,
    }
    return {**parts, "total": sum(parts.values())}


def cost_usd(model: str, input_tokens: int, output_tokens: int, cache_read: int = 0, cache_write: int = 0) -> float:
    return cost_breakdown(model, input_tokens, output_tokens, cache_read, cache_write)["total"]
