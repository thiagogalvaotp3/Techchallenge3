# -*- coding: utf-8 -*-
"""AWS GLUE JOB | Tech Challenge Fase 3 — State of Data Brasil
Job 2: Silver -> Gold (21 tabelas analiticas agregadas)

COMO IMPLANTAR NO AWS ACADEMY LAB:
  1. Console AWS -> Glue -> ETL Jobs -> Script editor -> colar este arquivo.
  2. IAM Role: LabRole | Glue version: 4.0 (Spark 3.3) | Workers: 2x G.1X.
  3. Job parameter obrigatorio:  --BUCKET = <nome-do-bucket-sem-s3://>
  4. O mesmo codigo roda localmente (fallback automatico sem awsglue).
"""
import sys

try:
    # ----- Ambiente AWS Glue -----------------------------------------
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    BASE = f"s3://{args['BUCKET']}/datalake"
    EH_GLUE = True
except ImportError:
    # ----- Fallback local (validacao/desenvolvimento) -----------------
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("job_silver_gold")
             .config("spark.driver.memory", "3g")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    BASE = "../../datalake"
    EH_GLUE = False
spark.sparkContext.setLogLevel("ERROR")

import os
from pyspark.sql import functions as F, Window
SILVER, GOLD = f"{BASE}/silver", f"{BASE}/gold"


core = spark.read.parquet(f"{SILVER}/silver_core")
serie = spark.read.parquet(f"{SILVER}/silver_serie_longa")

def grava(df, nome):
    """Grava parquet (consumo Athena) + CSV único (consumo notebooks/gráficos)."""
    df.write.mode("overwrite").parquet(f"{GOLD}/{nome}")
    (df.coalesce(1).write.mode("overwrite").option("header", True)
       .csv(f"{GOLD}/csv/{nome}"))
    print(f"[gold] {nome}: {df.count()} linhas")

def pct_sobre_ano(df, col_n="n"):
    w = Window.partitionBy("ano")
    return df.withColumn("pct", F.round(100 * F.col(col_n) / F.sum(col_n).over(w), 1))

# G01 — Respondentes por edição (6 anos) --------------------------------
grava(serie.groupBy("ano").count().withColumnRenamed("count", "n").orderBy("ano"),
      "gold_respondents")

# G02 — Cargos (grupo harmonizado, contribuidores individuais) ----------
cargos = (core.filter(F.col("cargo_grupo").isNotNull())
          .groupBy("ano", "cargo_grupo").count().withColumnRenamed("count", "n"))
grava(pct_sobre_ano(cargos).orderBy("ano", F.desc("n")), "gold_roles")

# G03 — Senioridade ------------------------------------------------------
sen = (core.filter(F.col("nivel").isNotNull())
       .groupBy("ano", "nivel").count().withColumnRenamed("count", "n"))
grava(pct_sobre_ano(sen).orderBy("ano", "nivel"), "gold_seniority")

# G04/G05 — Salário (ponto médio) por senioridade e por cargo ------------
sal_sen = (core.filter("salario_pm is not null and nivel is not null")
           .groupBy("ano", "nivel")
           .agg(F.count("*").alias("n"),
                F.round(F.avg("salario_pm"), 0).alias("salario_medio_pm"),
                F.round(F.expr("percentile(salario_pm, 0.5)"), 0).alias("salario_mediano_pm")))
grava(sal_sen.orderBy("ano", "nivel"), "gold_salary_by_seniority")

sal_cargo = (core.filter("salario_pm is not null and cargo_grupo is not null")
             .groupBy("ano", "cargo_grupo")
             .agg(F.count("*").alias("n"),
                  F.round(F.avg("salario_pm"), 0).alias("salario_medio_pm"),
                  F.round(F.expr("percentile(salario_pm, 0.5)"), 0).alias("salario_mediano_pm")))
grava(sal_cargo.orderBy("ano", F.desc("salario_mediano_pm")), "gold_salary_by_role")

# G05b — Salário por cargo, controlado por nível Sênior (Tabela 11 / Seção 6.3) --
# Materializa o recorte que a Seção 6.3/Tabela 11 do relatório usa para provar que
# o "prêmio de cargo" da G05 é real, e não apenas efeito de composição de senioridade.
cargo_total = (core.filter(F.col("cargo_grupo").isNotNull() & F.col("nivel").isNotNull())
               .groupBy("ano", "cargo_grupo").count().withColumnRenamed("count", "total_grupo"))
cargo_sen_mais = (core.filter(F.col("cargo_grupo").isNotNull()
                              & F.col("nivel").isin("Sênior", "Especialista/Staff+"))
                  .groupBy("ano", "cargo_grupo").count().withColumnRenamed("count", "sen_mais"))
cargo_sen = (core.filter((F.col("nivel") == "Sênior") & F.col("cargo_grupo").isNotNull()
                         & F.col("salario_pm").isNotNull())
             .groupBy("ano", "cargo_grupo")
             .agg(F.count("*").alias("n_senior"),
                  F.round(F.expr("percentile(salario_pm, 0.5)"), 0).alias("salario_mediano_pm_senior")))
cargo_ctrl = (cargo_total
              .join(cargo_sen_mais, ["ano", "cargo_grupo"], "left")
              .join(cargo_sen, ["ano", "cargo_grupo"], "left")
              .withColumn("pct_senior_mais", F.round(100 * F.coalesce(F.col("sen_mais"), F.lit(0))
                                                      / F.col("total_grupo"), 1))
              .select("ano", "cargo_grupo", "total_grupo", "pct_senior_mais",
                       "n_senior", "salario_mediano_pm_senior"))
grava(cargo_ctrl.orderBy("ano", F.desc("salario_mediano_pm_senior")), "gold_salary_by_role_seniority")

# G06 — Participação por gênero, série longa (6 anos) --------------------
gen = (serie.filter(F.col("genero").isNotNull())
       .groupBy("ano", "genero").count().withColumnRenamed("count", "n"))
grava(pct_sobre_ano(gen).orderBy("ano", "genero"), "gold_gender_participation")

# G07 — Gênero × senioridade × salário (controle de senioridade) ---------
gen_sal = (core.filter("salario_pm is not null and nivel is not null and genero in ('Masculino','Feminino')")
           .groupBy("ano", "genero", "nivel")
           .agg(F.count("*").alias("n"),
                F.round(F.avg("salario_pm"), 0).alias("salario_medio_pm"),
                F.round(F.expr("percentile(salario_pm, 0.5)"), 0).alias("salario_mediano_pm")))
grava(gen_sal.orderBy("ano", "nivel", "genero"), "gold_gender_seniority_salary")

# G08 — Gênero em posições de gestão -------------------------------------
gen_gestao = (core.filter("gestor is not null and genero in ('Masculino','Feminino')")
              .groupBy("ano", "genero")
              .agg(F.count("*").alias("n"),
                   F.sum("gestor").alias("gestores"),
                   F.round(100 * F.avg("gestor"), 1).alias("pct_gestores")))
grava(gen_gestao.orderBy("ano", "genero"), "gold_gender_leadership")

# G08b — Gênero × cargo, controlado por nível Sênior (Seção 7.4) ----------
# Materializa o recorte que a Seção 7.4 usa para decompor o gap de gênero em composição de
# cargo (Causa 1, via n/pct_do_genero) e em diferença residual dentro do mesmo cargo
# (Causa 2, via salario_mediano_pm) — antes desta tabela, os números de §7.4 não tinham
# origem reproduzível em nenhum artefato do pipeline.
gen_cargo_sen = (core.filter((F.col("nivel") == "Sênior") & F.col("cargo_grupo").isNotNull()
                             & F.col("genero").isin("Masculino", "Feminino"))
                  .groupBy("ano", "genero", "cargo_grupo")
                  .agg(F.count("*").alias("n"),
                       F.count(F.when(F.col("salario_pm").isNotNull(), 1)).alias("n_salario"),
                       F.round(F.expr("percentile(salario_pm, 0.5)"), 0).alias("salario_mediano_pm")))
w = Window.partitionBy("ano", "genero")
gen_cargo_sen = gen_cargo_sen.withColumn("pct_do_genero", F.round(100 * F.col("n") / F.sum("n").over(w), 1))
grava(gen_cargo_sen.orderBy("ano", "genero", F.desc("n")), "gold_gender_role_seniority")

# G09 — Tecnologias (linguagens/clouds 6 anos; BI 3 anos) -----------------
def tabela_tec(df, mapa, categoria):
    """Multirresposta: base = respondentes válidos da questão (Premissa P6/14.4)."""
    out = None
    for tec, col in mapa.items():
        t = (df.filter(F.col(col).isNotNull()).groupBy("ano")
             .agg(F.count("*").alias("base_valida"), F.sum(col).alias("usuarios"))
             .withColumn("tecnologia", F.lit(tec)).withColumn("categoria", F.lit(categoria)))
        out = t if out is None else out.unionByName(t)
    return out.withColumn("pct", F.round(100 * F.col("usuarios") / F.col("base_valida"), 1))

tec = tabela_tec(serie, {"SQL": "lang_sql", "Python": "lang_python", "R": "lang_r"}, "linguagem")
tec = tec.unionByName(tabela_tec(serie, {"AWS": "cloud_aws", "GCP": "cloud_gcp", "Azure": "cloud_azure"}, "cloud"))
tec = tec.unionByName(tabela_tec(core, {"Power BI": "bi_powerbi", "Tableau": "bi_tableau",
                                        "Looker Studio": "bi_looker_studio", "Metabase": "bi_metabase",
                                        "Qlik": "bi_qlik"}, "bi"))
grava(tec.select("ano", "categoria", "tecnologia", "usuarios", "base_valida", "pct")
      .orderBy("categoria", "tecnologia", "ano"), "gold_technologies")

# G10 — Prioridade de IA generativa (respondentes da questão: gestores) ---
ia = (core.filter(F.col("prioridade_ia_h").isNotNull())
      .groupBy("ano", "prioridade_ia_h").count().withColumnRenamed("count", "n"))
grava(pct_sobre_ano(ia).orderBy("ano", "prioridade_ia_h"), "gold_ai_priority")

# G11 — Uso individual de GenAI/Copilot ------------------------------------
MODALIDADES = {"Não usa GenAI": "genai_nao_uso", "Usa soluções gratuitas": "genai_gratuito",
               "Paga do próprio bolso": "genai_pago_proprio", "Empresa paga": "genai_pago_empresa",
               "Usa Copilot": "genai_copilot"}
genai = tabela_tec(core, MODALIDADES, "genai").withColumnRenamed("tecnologia", "modalidade")
grava(genai.select("ano", "modalidade", "usuarios", "base_valida", "pct").orderBy("modalidade", "ano"),
      "gold_genai_usage")

# G11b — Uso de GenAI, controlado por nível (Seção 9.2) --------------------
# Materializa o corte por senioridade que a Seção 9.2 usa para checar se "empresa paga"
# está concentrado em níveis mais seniores ou distribuído pela pirâmide inteira — antes
# desta tabela, a Seção 9 (IA) era a única pergunta de negócio sem controle de composição.
genai_nivel = None
for modalidade, col in MODALIDADES.items():
    t = (core.filter(F.col(col).isNotNull() & F.col("nivel").isNotNull())
         .groupBy("ano", "nivel")
         .agg(F.count("*").alias("base_valida"), F.sum(col).alias("usuarios"))
         .withColumn("modalidade", F.lit(modalidade)))
    genai_nivel = t if genai_nivel is None else genai_nivel.unionByName(t)
genai_nivel = genai_nivel.withColumn("pct", F.round(100 * F.col("usuarios") / F.col("base_valida"), 1))
grava(genai_nivel.select("ano", "nivel", "modalidade", "usuarios", "base_valida", "pct")
      .orderBy("ano", "modalidade", "nivel"), "gold_genai_usage_by_seniority")

# G12 — Regiões: distribuição e salário -----------------------------------
# "n" é a base de distribuição regional (denominador do "%" — todos com região válida);
# "n_salario" é a base real das colunas de salário, sempre <= n (Sec 5.1: 91,7% preenchida).
reg = (core.filter(F.col("regiao").isNotNull() & (F.col("regiao") != ""))
       .groupBy("ano", "regiao")
       .agg(F.count("*").alias("n"),
            F.count(F.when(F.col("salario_pm").isNotNull(), 1)).alias("n_salario"),
            # media do ponto medio: discrimina dentro da faixa, onde a mediana empata (Secao 10.1)
            F.round(F.avg("salario_pm"), 0).alias("salario_medio_pm"),
            F.round(F.expr("percentile(salario_pm, 0.5)"), 0).alias("salario_mediano_pm")))
grava(pct_sobre_ano(reg).orderBy("ano", F.desc("n")), "gold_regions")

# G12b — Regiões, controlado por nível Sênior (Seção 10.1) ----------------
# Materializa o recorte que a Seção 10.1 usa para mostrar que a diferença bruta de
# salário entre regiões (G12) é efeito de composição de senioridade, não de região.
reg_sen = (core.filter((F.col("nivel") == "Sênior") & F.col("regiao").isNotNull()
                       & (F.col("regiao") != "") & F.col("salario_pm").isNotNull())
           .groupBy("ano", "regiao")
           .agg(F.count("*").alias("n_senior"),
                F.round(F.avg("salario_pm"), 0).alias("salario_medio_pm_senior"),
                F.round(F.expr("percentile(salario_pm, 0.5)"), 0).alias("salario_mediano_pm_senior")))
grava(reg_sen.orderBy("ano", "salario_medio_pm_senior"), "gold_regions_by_seniority")

# G13 — Modelo de trabalho: atual vs ideal ---------------------------------
mod_a = (core.filter(F.col("modelo_atual_h").isNotNull())
         .groupBy("ano", "modelo_atual_h").count()
         .withColumnRenamed("modelo_atual_h", "modelo").withColumnRenamed("count", "n")
         .withColumn("tipo", F.lit("Atual")))
mod_i = (core.filter(F.col("modelo_ideal_h").isNotNull())
         .groupBy("ano", "modelo_ideal_h").count()
         .withColumnRenamed("modelo_ideal_h", "modelo").withColumnRenamed("count", "n")
         .withColumn("tipo", F.lit("Ideal")))
mod = mod_a.unionByName(mod_i)
w = Window.partitionBy("ano", "tipo")
mod = mod.withColumn("pct", F.round(100 * F.col("n") / F.sum("n").over(w), 1))
grava(mod.select("ano", "tipo", "modelo", "n", "pct").orderBy("ano", "tipo", "modelo"), "gold_work_model")

# G14 — Termômetro de mercado: satisfação, intenção de troca, layoff -------
mercado = (core.groupBy("ano")
           .agg(F.round(100 * F.avg("satisfeito"), 1).alias("pct_satisfeitos"),
                F.count("satisfeito").alias("n_satisfacao"),
                F.round(100 * F.avg("layoff_sim"), 1).alias("pct_layoff_sim"),
                F.count("layoff_sim").alias("n_layoff")))
grava(mercado.orderBy("ano"), "gold_market_pulse")

mudanca = (core.filter(F.col("mudar_emprego_6m").isNotNull())
           .groupBy("ano", "mudar_emprego_6m").count().withColumnRenamed("count", "n"))
grava(pct_sobre_ano(mudanca).orderBy("ano", F.desc("n")), "gold_job_change_intent")

# G15 — Critérios de escolha de emprego ------------------------------------
CRITERIOS = {"Remuneração/Salário": "crit_salario", "Benefícios": "crit_beneficios",
             "Propósito da empresa": "crit_proposito", "Flexibilidade remota": "crit_flex_remoto",
             "Ambiente de trabalho": "crit_ambiente", "Aprendizado/referências": "crit_aprendizado",
             "Plano de carreira": "crit_carreira", "Maturidade tech/dados": "crit_maturidade",
             "Qualidade dos gestores": "crit_gestores", "Reputação da empresa": "crit_reputacao"}
crit = tabela_tec(core, CRITERIOS, "criterio").withColumnRenamed("tecnologia", "criterio")
grava(crit.select("ano", "criterio", "usuarios", "base_valida", "pct").orderBy("ano", F.desc("pct")),
      "gold_job_criteria")

# G16 — Desafios dos gestores ----------------------------------------------
DESAFIOS = {"Contratar talentos": "des_contratar", "Reter talentos": "des_reter",
            "Aumentar investimentos": "des_investimentos", "Gestão remota": "des_remoto",
            "Projetos multidisciplinares": "des_multidisciplinar", "Qualidade dos dados": "des_qualidade",
            "Alto volume de dados": "des_volume", "Gerar valor p/ negócio": "des_valor",
            "ML em produção": "des_ml_prod", "Expectativa das áreas": "des_expectativas",
            "Manutenção em produção": "des_manutencao", "Levar inovação": "des_inovacao",
            "Garantir ROI": "des_roi", "Tempo técnico × gestão": "des_tempo"}
des = tabela_tec(core, DESAFIOS, "desafio").withColumnRenamed("tecnologia", "desafio")
grava(des.select("ano", "desafio", "usuarios", "base_valida", "pct").orderBy("ano", F.desc("pct")),
      "gold_manager_challenges")

print("\nOK — camada gold completa.")


if EH_GLUE:
    job.commit()  # finaliza o Glue Job com sucesso
