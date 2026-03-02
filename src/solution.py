"""Stub da solução do case: cálculo mensal e rolling 3 meses por escola."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils import validate_payments_schema


def compute_overdue_rolling_3m(payments_df: DataFrame) -> DataFrame:
    """Computa inadimplência mensal e rolling 3M por escola.

    Regras de negócio:
    - Inadimplente no mês: status in ('late', 'open')
    - Adimplente no mês: status == 'paid'
    - Agrupamento por school_id e mês (trunc(due_date, 'month'))
    - Rolling 3M: razão de somas (não média de percentuais)
      overdue_ratio_rolling_3m = sum(overdue_3m) / sum(due_3m)

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

    # Calcula agregações mensais por escola
    monthly_df = (
        payments_df.withColumn("month", F.date_trunc("month", F.col("due_date")))
        .groupBy("school_id", "month")
        .agg(
            F.sum("amount").alias("total_due_amount_month"),
            F.sum(
                F.when(F.col("status").isin("late", "open"), F.col("amount")).otherwise(F.lit(0))
            ).alias("total_overdue_amount_month"),
        )
        .withColumn(
            "overdue_ratio_month",
            F.when(
                F.col("total_due_amount_month") > 0,
                F.col("total_overdue_amount_month") / F.col("total_due_amount_month"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("year_month", F.date_format(F.col("month"), "yyyy-MM"))
    )

    # Window: por escola, ordenado por mês, janela de 3 meses (atual + 2 anteriores)
    window_rolling = (
        Window.partitionBy("school_id").orderBy(F.col("month").cast("long")).rowsBetween(-2, 0)
    )

    # Rolling 3M: razão de somas (não média de percentuais mensais)
    result_df = (
        monthly_df.withColumn(
            "overdue_ratio_rolling_3m",
            F.when(
                F.sum("total_due_amount_month").over(window_rolling) > 0,
                F.sum("total_overdue_amount_month").over(window_rolling)
                / F.sum("total_due_amount_month").over(window_rolling),
            ).otherwise(F.lit(None).cast("double")),
        )
        .select(
            "school_id",
            "month",
            "year_month",
            "total_due_amount_month",
            "total_overdue_amount_month",
            "overdue_ratio_month",
            "overdue_ratio_rolling_3m",
        )
        .orderBy("school_id", "month")
    )

    return result_df
