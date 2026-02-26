"""Testes públicos (contrato) para a solução de rolling 3M."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.solution import compute_overdue_rolling_3m


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Cria SparkSession local para testes.

    Returns:
        SparkSession configurada para ambiente local.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests-overdue-rolling-3m")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def build_input_df(spark: SparkSession):
    """Monta DataFrame determinístico para validar regras de negócio.

    Args:
        spark: Sessão Spark ativa.

    Returns:
        DataFrame de pagamentos com dois cenários de escola.
    """
    schema = T.StructType(
        [
            T.StructField("school_id", T.StringType(), False),
            T.StructField("payment_id", T.StringType(), False),
            T.StructField("due_date", T.DateType(), False),
            T.StructField("paid_date", T.DateType(), True),
            T.StructField("amount", T.DecimalType(18, 2), False),
            T.StructField("status", T.StringType(), False),
        ]
    )

    records = [
        ("SCH-A", "P-001", date(2024, 1, 10), date(2024, 1, 12), Decimal("100.00"), "paid"),
        ("SCH-A", "P-002", date(2024, 1, 15), None, Decimal("50.00"), "late"),
        ("SCH-A", "P-003", date(2024, 2, 5), None, Decimal("80.00"), "open"),
        ("SCH-A", "P-004", date(2024, 4, 8), date(2024, 4, 9), Decimal("120.00"), "paid"),
        ("SCH-A", "P-005", date(2024, 4, 20), None, Decimal("30.00"), "late"),
        ("SCH-B", "P-006", date(2024, 1, 1), date(2024, 1, 2), Decimal("200.00"), "paid"),
        ("SCH-B", "P-007", date(2024, 2, 2), None, Decimal("100.00"), "late"),
        ("SCH-B", "P-008", date(2024, 3, 3), date(2024, 3, 4), Decimal("100.00"), "paid"),
    ]

    return spark.createDataFrame(records, schema=schema)


def test_compute_overdue_rolling_3m_contract(spark: SparkSession) -> None:
    """Valida contrato de saída sem expor gabarito numérico detalhado."""
    input_df = build_input_df(spark)

    try:
        result_df = compute_overdue_rolling_3m(input_df)
    except NotImplementedError:
        pytest.skip("Implemente src/solution.py para executar os testes de contrato.")

    required_columns = {
        "school_id",
        "month",
        "year_month",
        "total_due_amount_month",
        "total_overdue_amount_month",
        "overdue_ratio_month",
        "overdue_ratio_rolling_3m",
    }
    assert required_columns.issubset(set(result_df.columns))

    result_count = result_df.count()
    assert result_count > 0

    distinct_school_month_count = result_df.select("school_id", "month").distinct().count()
    assert distinct_school_month_count == result_count

    expected_school_month_count = (
        input_df.withColumn("month", F.trunc(F.col("due_date"), "month"))
        .select("school_id", "month")
        .distinct()
        .count()
    )
    assert expected_school_month_count == result_count

    assert (
        result_df.filter(
            (F.col("overdue_ratio_month") < 0) | (F.col("overdue_ratio_month") > 1)
        ).count()
        == 0
    )
    assert (
        result_df.filter(
            (F.col("overdue_ratio_rolling_3m") < 0) | (F.col("overdue_ratio_rolling_3m") > 1)
        ).count()
        == 0
    )

    assert (
        result_df.filter(F.col("year_month") != F.date_format(F.col("month"), "yyyy-MM")).count()
        == 0
    )
