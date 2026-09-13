# Tech Challenge — Fase 3 · State of Data Brasil
**POSTECH DTAT · Grupo: Efraim Oliveira · Érica Tarsis · Ricardo Moraes · Rodrigo Bernardino · Thiago Galvão**

Repositório completo (código-fonte, histórico e evidências): https://github.com/lgmricardo/Techchallenge3-G12

Esta pasta segue exatamente as 3 entregas descritas no enunciado ("Expectativas de Entrega"):

## 1. Material executivo com DataViz e Storytelling → `1_material_executivo/`
`executive_deck.pptx` — indicadores, análises, insights e recomendações, com narrativa sobre
perfil profissional, tendências de mercado, tecnologias, remuneração, senioridade, modelos de
trabalho e oportunidades estratégicas. `executive_deck.html` é a versão-fonte (mesmo conteúdo,
formato web). Os PNGs soltos são os gráficos e o diagrama de arquitetura usados no material.

## 2. Diagrama da arquitetura da solução AWS → `2_diagrama_arquitetura/`
`aws_architecture.drawio` (editável, construído no Draw.io conforme pedido) e o PNG exportado.
O mesmo diagrama já está contido no material executivo (item 1), como exigido no enunciado.

## 3. Scripts e códigos utilizados → `3_scripts_e_codigos/`
- `notebooks/` — os 5 notebooks (01 a 05) executados, com outputs visíveis: ingestão, tratamento,
  transformação, catalogação (DDL/Crawler), consultas analíticas (Athena/Spark SQL) e geração
  dos gráficos. PySpark em todo o processamento (02–04); pandas/matplotlib só na etapa final
  de visualização (05), sobre os agregados já reduzidos da camada Gold.
- `jobs/` — os 2 Glue Jobs em PySpark (mesmo código roda localmente e no AWS Glue).
- `config/` — de-para de colunas entre as 6 edições (`column_mapping.json`) e premissas
  analíticas versionadas P1–P8 (`versioned_assumptions.md`).
- `dados_gold_csv/` — as 21 tabelas da camada Gold (saída dos scripts acima, consumida pelos
  gráficos e pelo relatório).
- `graficos/` — os 15 PNGs gerados pelo notebook 05 a partir da camada Gold.

**Nota sobre reexecução:** os notebooks já rodaram contra o datalake hospedado no S3 (AWS Academy
Lab) e trazem os outputs salvos — abrir o `.ipynb` já mostra o resultado, sem precisar reexecutar
nada. Os caminhos relativos do código pressupõem a estrutura completa do repositório (`datalake/`,
`consumption/`), por isso uma reexecução de ponta a ponta deve partir do clone do GitHub acima, e
não desta pasta isolada. Testamos isso antes de fechar o pacote: os 5 notebooks rodam sem erro a
partir de um clone limpo do repositório e reproduzem os mesmos 21 CSVs e 15 gráficos byte a byte.

## Material de apoio (além das 3 entregas exigidas)

- **`evidencias_execucao_aws/`** — prints da execução real no AWS Academy Lab (E01–E08: Lab
  ativo, bucket S3, Glue Jobs com status Succeeded, camadas Silver/Gold no S3, Glue Data
  Catalog, 7 queries no Athena). Não é um item numerado do enunciado, mas comprova o uso
  obrigatório da AWS Academy Lab.
- **`relatorio_tecnico/`** — documentação técnica completa (arquitetura, pipeline, premissas,
  resultados por pergunta de negócio, riscos e limitações, validação cruzada das interpretações).
  Não é um dos 3 entregáveis exigidos pelo enunciado; incluído como aprofundamento adicional.

---
*Fonte dos dados: State of Data Brasil — Data Hackers & Bain (Kaggle), edições 2019 a 2025/26.*
