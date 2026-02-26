# Case Técnico - Engenheiro de Dados Pleno (Spark Declarativo + Window Functions)

## Objetivo
Este repositório contém um case técnico para avaliar competências em **PySpark declarativo** com foco em:
- modelagem de transformação com DataFrame API (sem UDF),
- agregações mensais,
- cálculo de **rolling 3 meses** com `Window`,
- legibilidade, testes e boas práticas de engenharia.

O dataset do case é único: `payments`.

## Regras e restrições
- Execução 100% local (sem Databricks).
- Execução principal via `spark-submit`.
- Python 3.10+.
- Proibido usar na solução (`src/solution.py`):
  - UDF,
  - `collect`,
  - `toPandas`,
  - `foreach`,
  - `mapPartitions`.
- Cálculo rolling deve usar **window functions** por escola e ordenação por mês calendário.
- Rolling 3M deve ser **razão de somas**, não média simples de percentuais mensais.

## Dataset do case (`payments`)
Schema:
- `school_id` (string)
- `payment_id` (string)
- `due_date` (date)
- `paid_date` (date, nullable)
- `amount` (decimal(18,2))
- `status` (string: `paid`, `late`, `open`)

## Regras de negócio
- Inadimplente no mês de `due_date`: `status in ('late', 'open')`.
- Adimplente no mês de `due_date`: `status == 'paid'`.
- Calcular por escola e mês (`month = trunc(due_date, 'month')`):
  - `total_due_amount_month = soma(amount)`
  - `total_overdue_amount_month = soma(amount onde status in ('late','open'))`
  - `overdue_ratio_month = total_overdue_amount_month / total_due_amount_month`
- Calcular rolling 3 meses por escola:
  - `overdue_ratio_rolling_3m = soma(total_overdue_amount_month nos 3 meses) / soma(total_due_amount_month nos 3 meses)`
  - incluir mês atual + 2 anteriores,
  - sem média simples dos percentuais.

## O que o candidato deve editar
- **Somente**: `src/solution.py`

Os demais arquivos (geração de dados, job, testes, scripts) são infraestrutura do case.

## Pré-requisitos
- Python 3.10+
- Java 11+ (ou 17+) com `JAVA_HOME` configurado

Sem Java, o Spark não inicializa (`JAVA_GATEWAY_EXITED`).

## Setup
### Linux/macOS
```bash
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -e ".[dev]"
```

### Windows (PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -e ".[dev]"
```

### Com Makefile
```bash
make setup
make check-java
```

## Passo a passo do candidato (fork -> solução -> PR)
### 1) Fazer fork e clonar
1. Acesse o repositório oficial e clique em **Fork**.
2. Clone o seu fork:

```bash
git clone git@github.com:<seu-usuario>/case-tecnico-eng-dados.git
cd case-tecnico-eng-dados
```

3. (Opcional) Adicione o upstream do repositório oficial:

```bash
git remote add upstream git@github.com:Educbank/case-tecnico-eng-dados.git
```

### 2) Criar branch de trabalho
```bash
git checkout -b feat/solution-rolling-3m
```

### 3) Instalar dependências e validar Java
```bash
make setup
make check-java
```

### 4) Gerar dados sintéticos
```bash
make generate-data
```

### 5) Implementar a solução
Edite **somente**:
- `src/solution.py`

Respeite as regras de negócio e as restrições do case descritas neste README.

### 6) Executar validações e testes locais
```bash
make run
make test
```

Se quiser validar estilo/código:
```bash
make lint
```

### 7) Commit e push no fork
```bash
git add src/solution.py
git commit -m "feat(solution): implement overdue rolling 3m by school"
git push -u origin feat/solution-rolling-3m
```

### 8) Abrir Pull Request
1. Abra um PR do seu fork para `Educbank/case-tecnico-eng-dados` (branch `main`).
2. No corpo do PR, inclua:
   - resumo da solução,
   - como calculou o rolling 3M,
   - comandos executados (`make run`, `make test`),
   - versão do Python e do Java.

### 9) Checklist antes de enviar
- Implementação em `src/solution.py`.
- Regras de negócio atendidas para cálculo mensal e rolling 3 meses.
- Sem UDF/collect/toPandas/foreach/mapPartitions.
- `make test` passando.

## Como gerar dados sintéticos
```bash
make generate-data
```

Com parâmetros customizados:
```bash
./.venv/bin/python src/generate_data.py --out-dir data/payments --seed 42 --months 12 --schools 30 --rows 12000 --start-month 2024-01
```

## Como rodar a solução (spark-submit)
```bash
make run
```

Ou diretamente:
```bash
./scripts/run_spark_submit.sh data/payments data/output
```

No Windows (PowerShell), se não usar shell script:
```powershell
$env:PYSPARK_DRIVER_PYTHON = ".\.venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = ".\.venv\Scripts\python.exe"
.\.venv\Scripts\spark-submit --master local[*] --conf spark.sql.shuffle.partitions=8 src/job.py --input-path data/payments --output-path data/output
```

## Como rodar os testes
```bash
make test
```

## Critérios de avaliação (rubrica 1 a 5)
1. **Correção**
   - métricas mensais corretas,
   - rolling 3M correto (razão de somas),
   - tratamento de meses faltantes e divisão por zero.
2. **Uso de janela**
   - uso apropriado de window functions de forma consistente com as regras do case.
3. **Legibilidade e organização**
   - código limpo, nomes claros, docstrings, validações.
4. **Performance básica**
   - evitar operações proibidas/anti-patterns,
   - pipeline declarativo sem ações desnecessárias.
5. **Explicação técnica**
   - clareza do raciocínio e justificativa de decisões.

## Entrega
Escolha uma opção:
- Fork deste repositório e abrir PR com sua implementação em `src/solution.py`.
- Publicar seu repositório e enviar o link.

No envio, inclua:
- versão do Python,
- comando usado para execução,
- breve explicação da solução.

## Estrutura
```text
.
├── README.md
├── Makefile
├── pyproject.toml
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── generate_data.py
│   ├── job.py
│   ├── solution.py
│   └── utils.py
├── scripts/
│   ├── run_spark_submit.sh
│   └── check_solution_constraints.py
├── tests/
│   └── test_solution.py
└── (sem gabarito no repositório público)
```
