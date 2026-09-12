# Evidências de execução no AWS Academy Lab

Prints reais da execução do pipeline no AWS Academy Lab (não simulados). O passo a passo usado
para coletá-los está documentado no repositório GitHub (`docs/aws_step_by_step_guide.md` e
`docs/guia_coleta_evidencias.md`) para quem quiser reproduzir a coleta.

| Arquivo | O que mostra | Requisito |
|---|---|---|
| E01a_vocareum_lab_ativo.png + E01b_console_us_east_1.png | Timer/budget do Lab + Console us-east-1 | R2 |
| E02_bucket_block_public_access.png | Block Public Access ativo no bucket | Segurança |
| E03_s3_bronze_6_edicoes.png | bronze/ano=YYYY com os 6 CSVs | R1, R3 |
| E04a_glue_job1_script.png + E04b_glue_job1_succeeded.png | Script + Run Succeeded (Job 1) | R4, R7 |
| E05a_glue_job2_script.png + E05b_glue_job2_succeeded.png | Script + Run Succeeded (Job 2) | R4, R7 |
| E06a_s3_silver.png + E06b_s3_gold.png | Camadas Silver e Gold no S3 | R5 |
| E07_glue_catalog_tabelas.png | Database stateofdata + tabelas | R4 |
| E08_athena_q1.png … E08_athena_q7.png | SQL + resultado de cada query | R6 |
