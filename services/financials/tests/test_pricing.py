from financials.pricing import cost_usd


def test_cost_for_opus_5():
    # 1M input + 1M output at $5 / $25
    assert cost_usd("claude-opus-5", 1_000_000, 1_000_000) == 30.0


def test_unknown_model_falls_back_to_default_price():
    assert cost_usd("mystery", 1_000_000, 0) == 5.0
