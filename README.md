# 📊 Case Técnico — Engenharia de Dados

## 📌 Objetivo

Este projeto implementa o cálculo de métricas mensais de inadimplência por escola, incluindo o indicador rolling de 3 meses, utilizando **PySpark**.

O foco da solução é garantir:

- Correção estatística
- Uso adequado de Window Functions
- Código limpo e organizado
- Pipeline declarativo e performático

---

## 🧠 Estratégia da Solução

### 1️⃣ Agregação Mensal

Os dados são agregados por:

- `school_id`
- mês (derivado de `due_date`)

São calculadas as métricas:

- `total_due_amount_month`
- `total_overdue_amount_month`
- `overdue_ratio_month`

### 2️⃣ Cálculo do Rolling 3M

O indicador `overdue_ratio_rolling_3m` foi implementado utilizando **Window Functions**:

- `partitionBy("school_id")`
- `orderBy("month")`
- `rowsBetween(-2, 0)`

O cálculo é feito como **razão de somas** (`sum overdue / sum due`), evitando distorções que ocorreriam ao utilizar média de percentuais mensais. Também foi implementado tratamento para evitar divisão por zero.

---

## ⚙️ Como Executar

| Comando | Descrição |
|--------|-----------|
| `make run` | Executa a solução |
| `make test` | Executa os testes |
| `make lint` | Verifica lint e formatação |

---

## 🛠 Ambiente Utilizado

| Ferramenta | Versão |
|-----------|--------|
| Python | 3.11.9 |
| Java | OpenJDK 17.0.17 (Temurin) |
| Spark | 3.5.5 |

---

## 🏗 Decisões Técnicas

- Utilização exclusiva de transformações Spark (sem `collect()` ou conversões para Pandas)
- Uso consistente de Window Functions para cálculo acumulado
- Pipeline declarativo e organizado
- Validação básica do schema de entrada


