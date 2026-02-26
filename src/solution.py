"""Stub da solução do case: cálculo mensal e rolling 3 meses por escola."""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.utils import validate_payments_schema


def compute_overdue_rolling_3m(payments_df: DataFrame) -> DataFrame:
    """Computa inadimplência mensal e rolling 3M por escola.

    Args:
        payments_df: DataFrame de entrada com schema `payments`.

    Returns:
        DataFrame por `school_id` e `month` com colunas:
            - school_id
            - month
            - year_month
            - total_due_amount_month
            - total_overdue_amount_month
            - overdue_ratio_month
            - overdue_ratio_rolling_3m
    """
    validate_payments_schema(payments_df)
    raise NotImplementedError(
        "Implemente a lógica de cálculo mensal e rolling 3M usando PySpark DataFrame API."
    )
