"""Gera dataset sintético `payments` para o case técnico."""

from __future__ import annotations

import argparse
import random
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

STATUSES = ("paid", "late", "open")


def parse_args() -> argparse.Namespace:
    """Lê argumentos da CLI.

    Returns:
        Namespace com parâmetros de geração.
    """
    parser = argparse.ArgumentParser(description="Gera dados sintéticos de pagamentos em parquet.")
    parser.add_argument("--out-dir", default="data/payments", help="Diretório de saída parquet.")
    parser.add_argument("--seed", type=int, default=42, help="Seed para reprodutibilidade.")
    parser.add_argument("--months", type=int, default=12, help="Quantidade de meses a gerar.")
    parser.add_argument("--schools", type=int, default=30, help="Quantidade de escolas.")
    parser.add_argument("--rows", type=int, default=12000, help="Quantidade total de pagamentos.")
    parser.add_argument(
        "--start-month",
        default="2024-01",
        help="Mês inicial no formato YYYY-MM.",
    )
    return parser.parse_args()


def parse_start_month(start_month: str) -> date:
    """Converte string YYYY-MM para primeiro dia do mês.

    Args:
        start_month: Data no formato YYYY-MM.

    Returns:
        Data representando o primeiro dia do mês.

    Raises:
        ValueError: Quando formato for inválido.
    """
    parsed = datetime.strptime(start_month, "%Y-%m")
    return date(parsed.year, parsed.month, 1)


def add_months(base_month: date, offset: int) -> date:
    """Retorna o primeiro dia de `offset` meses a partir de `base_month`.

    Args:
        base_month: Data base (primeiro dia do mês).
        offset: Quantidade de meses de deslocamento.

    Returns:
        Data deslocada para o mês desejado.
    """
    month_index = (base_month.year * 12 + (base_month.month - 1)) + offset
    target_year = month_index // 12
    target_month = (month_index % 12) + 1
    return date(target_year, target_month, 1)


def build_month_sequence(start_month: date, months: int) -> list[date]:
    """Monta sequência de meses consecutivos.

    Args:
        start_month: Primeiro mês da série.
        months: Quantidade de meses.

    Returns:
        Lista de primeiros dias de cada mês.
    """
    return [add_months(start_month, offset) for offset in range(months)]


def build_sparse_calendar(
    school_ids: Sequence[str],
    month_sequence: Sequence[date],
    rng: random.Random,
) -> dict[str, list[date]]:
    """Cria calendário por escola com lacunas em parte das escolas.

    Args:
        school_ids: Lista de identificadores de escola.
        month_sequence: Lista completa de meses do período.
        rng: Gerador pseudoaleatório.

    Returns:
        Mapa `school_id -> meses disponíveis`.
    """
    calendar: dict[str, list[date]] = {school_id: list(month_sequence) for school_id in school_ids}

    sparse_count = max(1, int(len(school_ids) * 0.25))
    sparse_schools = rng.sample(list(school_ids), k=sparse_count)

    pool = list(month_sequence[1:-1]) if len(month_sequence) > 2 else list(month_sequence)
    for school_id in sparse_schools:
        if len(pool) <= 1:
            continue
        drop_count = min(2, max(1, len(pool) // 5))
        dropped = set(rng.sample(pool, k=drop_count))
        calendar[school_id] = [month for month in month_sequence if month not in dropped]

    return calendar


def sample_status(rng: random.Random, school_risk: float, month_value: int) -> str:
    """Amostra status com distribuição realista e leve sazonalidade.

    Args:
        rng: Gerador pseudoaleatório.
        school_risk: Fator de risco da escola.
        month_value: Número do mês (1-12).

    Returns:
        Status em {paid, late, open}.
    """
    seasonality = 0.02 if month_value in (1, 2) else 0.0
    late_prob = min(max(0.12 + school_risk + seasonality, 0.05), 0.35)
    open_prob = min(max(0.08 + (school_risk / 2), 0.03), 0.25)
    paid_prob = max(1.0 - late_prob - open_prob, 0.35)

    threshold_1 = paid_prob
    threshold_2 = paid_prob + late_prob
    sampled = rng.random()

    if sampled <= threshold_1:
        return "paid"
    if sampled <= threshold_2:
        return "late"
    return "open"


def sample_amount(rng: random.Random, school_risk: float) -> Decimal:
    """Gera valor de pagamento com distribuição assimétrica.

    Args:
        rng: Gerador pseudoaleatório.
        school_risk: Fator de risco da escola.

    Returns:
        Valor decimal com 2 casas.
    """
    base = rng.lognormvariate(4.7 + (school_risk * 0.5), 0.45)
    clamped = min(max(base, 40.0), 3500.0)
    return Decimal(clamped).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def build_payments_records(
    school_ids: Sequence[str],
    school_calendar: dict[str, list[date]],
    rows: int,
    rng: random.Random,
) -> list[tuple]:
    """Gera registros transacionais sintéticos de pagamentos.

    Args:
        school_ids: Lista de escolas.
        school_calendar: Mapa de meses disponíveis por escola.
        rows: Quantidade total de registros.
        rng: Gerador pseudoaleatório.

    Returns:
        Lista de tuplas no formato do schema `payments`.
    """
    school_risk_map = {school_id: rng.uniform(-0.04, 0.10) for school_id in school_ids}
    records: list[tuple] = []

    for index in range(rows):
        school_id = rng.choice(school_ids)
        available_months = school_calendar[school_id]
        due_month = rng.choice(available_months)
        due_day = rng.randint(1, 28)
        due_date = date(due_month.year, due_month.month, due_day)

        risk = school_risk_map[school_id]
        status = sample_status(rng=rng, school_risk=risk, month_value=due_month.month)
        amount = sample_amount(rng=rng, school_risk=risk)

        paid_date = None
        if status == "paid":
            shift_days = rng.randint(-2, 25)
            paid_date = due_date + timedelta(days=shift_days)

        payment_id = f"PAY-{index + 1:08d}"
        records.append((school_id, payment_id, due_date, paid_date, amount, status))

    return records


def get_payments_schema() -> T.StructType:
    """Retorna schema Spark do dataset `payments`.

    Returns:
        Schema estruturado do dataset de pagamentos.
    """
    return T.StructType(
        [
            T.StructField("school_id", T.StringType(), nullable=False),
            T.StructField("payment_id", T.StringType(), nullable=False),
            T.StructField("due_date", T.DateType(), nullable=False),
            T.StructField("paid_date", T.DateType(), nullable=True),
            T.StructField("amount", T.DecimalType(18, 2), nullable=False),
            T.StructField("status", T.StringType(), nullable=False),
        ]
    )


def main() -> None:
    """Executa geração de dados sintéticos e grava parquet particionado."""
    args = parse_args()

    if args.months < 1:
        raise ValueError("--months deve ser maior ou igual a 1")
    if args.schools < 1:
        raise ValueError("--schools deve ser maior ou igual a 1")
    if args.rows < 1:
        raise ValueError("--rows deve ser maior ou igual a 1")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)
    start_month = parse_start_month(args.start_month)
    month_sequence = build_month_sequence(start_month=start_month, months=args.months)
    school_ids = [f"SCH-{idx:03d}" for idx in range(1, args.schools + 1)]
    school_calendar = build_sparse_calendar(
        school_ids=school_ids,
        month_sequence=month_sequence,
        rng=rng,
    )

    records = build_payments_records(
        school_ids=school_ids,
        school_calendar=school_calendar,
        rows=args.rows,
        rng=rng,
    )

    spark = SparkSession.builder.appName("generate-payments-synthetic-data").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    payments_df = spark.createDataFrame(records, schema=get_payments_schema())
    payments_df = payments_df.withColumn("year_month", F.date_format(F.col("due_date"), "yyyy-MM"))

    (payments_df.write.mode("overwrite").partitionBy("year_month").parquet(str(out_dir)))

    month_count = payments_df.select("year_month").distinct().count()
    print(
        f"Dataset gerado com sucesso em {out_dir}. "
        f"Registros: {len(records)} | Escolas: {len(school_ids)} | Meses distintos: {month_count}."
    )

    spark.stop()


if __name__ == "__main__":
    main()
