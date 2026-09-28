"""Daily profitability math. Kept Spark-free so tests can run without a cluster."""

from __future__ import annotations


def compute_profit(
    earnings: float,
    fuel_cost: float,
    maintenance_cost: float,
) -> tuple[float, bool]:
    profit = round(float(earnings) - float(fuel_cost) - float(maintenance_cost), 2)
    return profit, profit < 0
