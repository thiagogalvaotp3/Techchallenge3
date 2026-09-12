# Execução e Reprodução — Tech Challenge Fase 3

Passo a passo para reproduzir o pipeline completo a partir de um clone limpo deste repositório.
Os notebooks já vêm executados, com outputs salvos — isto é para quem quiser rodar do zero e
conferir os números por conta própria.

**Testado antes desta entrega:** clone limpo, venv novo, sem reaproveitar nada do ambiente local.
Os 5 notebooks executaram sem erro e reproduziram os mesmos 21 CSVs da camada Gold e os mesmos
15 gráficos já commitados no repositório, byte a byte, no seguinte ambiente:

| Componente | Versão |
|---|---|
| Python | 3.12.13 |
| PySpark | 3.5.1 |
| Java (JDK) | OpenJDK 17.0.20.1 (Homebrew) |
| pandas | 3.0.5 |
| matplotlib | 3.11.1 |
| SO | macOS (Darwin), Apple Silicon |

Reprodução byte a byte não é garantida fora desse conjunto de versões — variações de Java, Spark
ou glibc podem, em tese, alterar a ordem de agregação em paralelo ou a precisão de ponto flutuante.
Dentro do ambiente acima, o resultado é determinístico e foi conferido, não apenas assumido.

## 1. Pré-requisitos

- Python 3.10+
- Java: JDK 8, 11 ou 17 — o PySpark 3.5.1 não sobe em JDK 21+ (remove o Security Manager que o
  Spark 3.5 usa internamente). No macOS: `brew install openjdk@17`.

## 2. Setup do ambiente

```bash
git clone https://github.com/lgmricardo/Techchallenge3-G12.git
cd Techchallenge3-G12
./install_requirements.sh        # cria .venv/ e instala requirements.txt
source .venv/bin/activate
```

Aponte `JAVA_HOME` para o JDK 8/11/17 instalado (o caminho varia por sistema; no exemplo acima do
Homebrew em Apple Silicon seria `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`).

## 3. Executar o pipeline

Rodar os 5 notebooks em ordem — cada um é idempotente (Bronze é imutável, os jobs usam
`mode=overwrite`), então reexecutar não tem efeito colateral:

```bash
jupyter nbconvert --to notebook --execute --inplace src/notebooks/01_bronze_ingestion.ipynb
jupyter nbconvert --to notebook --execute --inplace src/notebooks/02_bronze_to_silver.ipynb
jupyter nbconvert --to notebook --execute --inplace src/notebooks/03_silver_to_gold.ipynb
jupyter nbconvert --to notebook --execute --inplace src/notebooks/04_athena_queries.ipynb
jupyter nbconvert --to notebook --execute --inplace src/notebooks/05_gold_analytics.ipynb
```

(Equivalente a abrir e "Run All" em cada `.ipynb` no Jupyter/VS Code, a partir de `src/notebooks/`.)

## 4. O que esperar como saída

- `datalake/gold/csv/` — as 21 tabelas analíticas da camada Gold
- `consumption/charts/` — os 15 gráficos executivos (PNG)
- Zero erros em qualquer célula

## 5. Conferir determinismo (opcional)

O pipeline é determinístico — mesma entrada, mesma saída, byte a byte:

```bash
md5sum datalake/gold/csv/*.csv consumption/charts/*.png > /tmp/hashes_depois.txt
diff /tmp/hashes_antes.txt /tmp/hashes_depois.txt   # deve vir vazio
```

## 6. AWS Academy Lab (execução real, não local)

Passo a passo completo em [`docs/aws_step_by_step_guide.md`](docs/aws_step_by_step_guide.md).
Resumo:

1. Criar bucket S3 privado, subir os 6 CSVs em `datalake/bronze/ano=YYYY/`.
2. Criar os 2 Glue Jobs colando `src/jobs/job_01_bronze_to_silver.py` e
   `src/jobs/job_02_silver_to_gold.py`, role `LabRole`, parâmetro `--BUCKET`.
3. Rodar Job 1 → Job 2; catalogar `datalake/silver` e `datalake/gold` via Crawler ou DDL
   (notebook 04).
4. Executar as 7 consultas do notebook 04 no Athena.

Evidências reais dessa execução (prints do console AWS) estão em
[`evidence/`](evidence/) (E01–E08).
