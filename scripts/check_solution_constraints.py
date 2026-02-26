"""Valida restrições de implementação do arquivo src/solution.py."""

from __future__ import annotations

import re
import sys
from pathlib import Path

TARGET_FILE = Path("src/solution.py")

FORBIDDEN_PATTERNS = {
    r"\bcollect\s*\(": "Uso de collect() é proibido na solução.",
    r"\btoPandas\s*\(": "Uso de toPandas() é proibido na solução.",
    r"\bforeach\s*\(": "Uso de foreach() é proibido na solução.",
    r"\bmapPartitions\s*\(": "Uso de mapPartitions() é proibido na solução.",
    r"\bpandas_udf\s*\(": "Uso de pandas_udf() é proibido na solução.",
    r"\budf\s*\(": "Uso de udf() é proibido na solução.",
}

FORBIDDEN_IMPORT_PATTERNS = {
    r"from\s+pyspark\.sql\.functions\s+import\s+.*\budf\b": (
        "Import de udf é proibido na solução."
    ),
    r"from\s+pyspark\.sql\.functions\s+import\s+.*\bpandas_udf\b": (
        "Import de pandas_udf é proibido na solução."
    ),
}


def scan_file(content: str) -> list[str]:
    """Varre conteúdo e retorna violações encontradas.

    Args:
        content: Conteúdo textual do arquivo.

    Returns:
        Lista de mensagens de erro.
    """
    violations: list[str] = []

    for pattern, message in FORBIDDEN_PATTERNS.items():
        for match in re.finditer(pattern, content):
            line_number = content[: match.start()].count("\n") + 1
            violations.append(f"Linha {line_number}: {message}")

    for pattern, message in FORBIDDEN_IMPORT_PATTERNS.items():
        for match in re.finditer(pattern, content):
            line_number = content[: match.start()].count("\n") + 1
            violations.append(f"Linha {line_number}: {message}")

    return violations


def main() -> int:
    """Executa validação de restrições e define exit code.

    Returns:
        Código de saída do processo.
    """
    if not TARGET_FILE.exists():
        print(f"Arquivo não encontrado: {TARGET_FILE}")
        return 1

    content = TARGET_FILE.read_text(encoding="utf-8")
    violations = scan_file(content)

    if violations:
        print("Falha na validação de restrições de solution.py:")
        for violation in violations:
            print(f"- {violation}")
        return 1

    print("Validação de restrições concluída sem violações.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
