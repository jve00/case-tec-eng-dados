"""Utilitários de validação e helpers para o case."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

PAYMENTS_REQUIRED_COLUMNS = (
    "school_id",
    "payment_id",
    "due_date",
    "paid_date",
    "amount",
    "status",
)

PAYMENTS_EXPECTED_TYPES: Mapping[str, T.DataType] = {
    "school_id": T.StringType(),
    "payment_id": T.StringType(),
    "due_date": T.DateType(),
    "paid_date": T.DateType(),
    "amount": T.DecimalType(18, 2),
    "status": T.StringType(),
}


def assert_columns_exist(df: DataFrame, required_columns: Sequence[str]) -> None:
    """Valida se todas as colunas obrigatórias existem no DataFrame.

    Args:
        df: DataFrame de entrada.
        required_columns: Coleção de nomes de colunas obrigatórias.

    Raises:
        ValueError: Quando alguma coluna obrigatória estiver ausente.
    """
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")


def assert_column_types(df: DataFrame, expected_types: Mapping[str, T.DataType]) -> None:
    """Valida os tipos esperados de colunas no schema.

    A validação compara classes de tipos Spark. Para `DecimalType`, também
    verifica precisão e escala.

    Args:
        df: DataFrame de entrada.
        expected_types: Mapa coluna -> tipo esperado.

    Raises:
        ValueError: Quando uma coluna possuir tipo incompatível.
    """
    schema_map = {field.name: field.dataType for field in df.schema.fields}

    for column, expected_type in expected_types.items():
        current_type = schema_map.get(column)
        if current_type is None:
            continue

        if isinstance(expected_type, T.DecimalType):
            if not isinstance(current_type, T.DecimalType):
                raise ValueError(
                    f"Tipo inválido para '{column}'. Esperado DecimalType, recebido {current_type}."
                )
            if (
                current_type.precision != expected_type.precision
                or current_type.scale != expected_type.scale
            ):
                raise ValueError(
                    f"Tipo inválido para '{column}'. Esperado Decimal({expected_type.precision}, "
                    f"{expected_type.scale}), recebido Decimal({current_type.precision}, {current_type.scale})."
                )
            continue

        if type(current_type) is not type(expected_type):
            raise ValueError(
                f"Tipo inválido para '{column}'. Esperado {expected_type}, recebido {current_type}."
            )


def validate_payments_schema(df: DataFrame) -> None:
    """Valida o schema esperado do dataset `payments`.

    Args:
        df: DataFrame de pagamentos.

    Raises:
        ValueError: Quando o schema é incompatível.
    """
    assert_columns_exist(df, PAYMENTS_REQUIRED_COLUMNS)
    assert_column_types(df, PAYMENTS_EXPECTED_TYPES)


def to_decimal_zero() -> F.Column:
    """Retorna literal decimal zero com precisão estável.

    Returns:
        Coluna Spark com valor decimal 0.00.
    """
    return F.lit(Decimal("0.00")).cast("decimal(18,2)")
