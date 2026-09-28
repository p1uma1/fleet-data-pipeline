from src.profitability import compute_profit


def test_unprofitable_when_costs_exceed_earnings():
    profit, unprofitable = compute_profit(4200.0, 4200.5, 1500.0)
    assert profit == -1500.5
    assert unprofitable is True


def test_profitable_vehicle():
    profit, unprofitable = compute_profit(8000.0, 2900.0, 400.0)
    assert profit == 4700.0
    assert unprofitable is False


def test_zero_earnings_is_unprofitable_if_any_cost():
    profit, unprofitable = compute_profit(0.0, 100.0, 0.0)
    assert profit == -100.0
    assert unprofitable is True
