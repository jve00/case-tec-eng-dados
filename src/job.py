"""Ponto de entrada do job Spark para cálculo de inadimplência rolling 3M."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession

from src.solution import compute_overdue_rolling_3m


def parse_args() -> argparse.Namespace:
    """Lê argumentos de linha de comando.

    Returns:
        Namespace com paths de entrada e saída.
    """
    parser = argparse.ArgumentParser(description="Executa cálculo rolling 3M de inadimplência.")
    parser.add_argument(
        "--input-path",
        default="data/payments",
        help="Diretório de entrada parquet com dataset payments.",
    )
    parser.add_argument(
        "--output-path",
        default="data/output",
        help="Diretório de saída parquet para resultado agregado.",
    )
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    """Cria SparkSession para execução local.

    Returns:
        SparkSession configurada.
    """
    spark = SparkSession.builder.appName("payments-overdue-rolling-3m").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def main() -> None:
    """Executa leitura, transformação e escrita do job."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    args = parse_args()
    spark = build_spark_session()

    logger.info("Lendo dados de entrada em %s", args.input_path)
    payments_df = spark.read.parquet(args.input_path)

    logger.info("Computando métricas mensais e rolling 3M")
    result_df = compute_overdue_rolling_3m(payments_df)

    logger.info("Escrevendo saída em %s", args.output_path)
    (result_df.write.mode("overwrite").partitionBy("year_month").parquet(args.output_path))

    logger.info("Job finalizado com sucesso")
    spark.stop()


if __name__ == "__main__":
    main()
