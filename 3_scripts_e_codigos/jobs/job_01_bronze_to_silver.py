# -*- coding: utf-8 -*-
"""AWS GLUE JOB | Tech Challenge Fase 3 — State of Data Brasil
Job 1: Bronze -> Silver (harmonizacao de schemas das 6 edicoes)

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
    spark = (SparkSession.builder.master("local[2]").appName("job_bronze_silver")
             .config("spark.driver.memory", "3g")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    BASE = "../../datalake"
    EH_GLUE = False
spark.sparkContext.setLogLevel("ERROR")

import json, re, os
from pyspark.sql import functions as F, types as T
BRONZE, SILVER = f"{BASE}/bronze", f"{BASE}/silver"
VOLUMETRIA_ESPERADA = {2019: 1765, 2021: 2645, 2022: 4271, 2023: 5293, 2024: 5217, 2025: 3495}


def ler_bronze(ano):
    df = (spark.read.option("header", True).option("escape", '"')
          .csv(f"{BRONZE}/ano={ano}/state_of_data_{ano}.csv"))
    n = df.count()
    assert n == VOLUMETRIA_ESPERADA[ano], f"{ano}: {n} != {VOLUMETRIA_ESPERADA[ano]}"
    print(f"[bronze] {ano}: {n} linhas x {len(df.columns)} colunas")
    return df

# ------------------------------------------------------------------
# 1. DE-PARA núcleo (2023 / 2024 / 2025)  destino -> origem
# ------------------------------------------------------------------
DEPARA_2025 = {
    "id": "0.a_token", "idade": "1.a_idade", "faixa_idade": "1.a.1_faixa_idade",
    "genero": "1.b_genero", "cor_raca": "1.c_cor/raca/etnia", "pcd": "1.d_pcd",
    "uf": "1.i.1_uf_onde_mora", "regiao": "1.i.2_regiao_onde_mora",
    "nivel_ensino": "1.l_nivel_de_ensino", "area_formacao": "1.m_área_de_formação",
    "situacao_trabalho": "2.a_situação_de_trabalho", "setor": "2.b_setor",
    "num_funcionarios": "2.c_numero_de_funcionarios", "gestor": "2.d_atua_como_gestor",
    "cargo_gestor": "2.e_cargo_como_gestor", "cargo": "2.f_cargo_atual",
    "nivel": "2.g_nivel", "faixa_salarial": "2.h_faixa_salarial",
    "tempo_exp_dados": "2.i_tempo_de_experiencia_em_dados",
    "satisfeito": "2.k_satisfeito_atualmente",
    "entrevistas_6m": "2.m_participou_de_entrevistas_ultimos_6m",
    "mudar_emprego_6m": "2.n_planos_de_mudar_de_emprego_6m",
    "layoff": "2.p_empresa_passou_por_layoff_em_2025",
    "modelo_atual": "2.q_modelo_de_trabalho_atual",
    "modelo_ideal": "2.r_modelo_de_trabalho_ideal",
    "atitude_presencial": "2.s_atitude_em_caso_de_retorno_presencial",
    "prioridade_ia": "3.e_ai_generativa_e_llm_é_uma_prioridade?",
    "atuacao": "4.a.1_atuacao_em_dados",
    "lang_sql": "4.c.1_SQL", "lang_r": "4.c.2_R", "lang_python": "4.c.3_Python",
    "cloud_aws": "4.e.1_Amazon Web Services (AWS)", "cloud_gcp": "4.e.2_Google Cloud (GCP)",
    "cloud_azure": "4.e.3_Azure (Microsoft)",
    "cloud_onprem": "4.e.6_Servidores On Premise/Não utilizamos Cloud",
    "bi_powerbi": "4.g.1_Microsoft PowerBI", "bi_qlik": "4.g.2_Qlik View/Qlik Sense",
    "bi_tableau": "4.g.3_Tableau", "bi_metabase": "4.g.4_Metabase",
    "bi_looker_studio": "4.g.8_Looker Studio(Google Data Studio)",
    "genai_nao_uso": "4.j.1 Não uso soluções de AI Generativa com foco em produtividade",
    "genai_gratuito": "4.j.2 Uso soluções gratuitas de AI Generativa com foco em produtividade",
    "genai_pago_proprio": "4.j.3 Uso e pago pelas soluções de AI Generativa com foco em produtividade",
    "genai_pago_empresa": "4.j.4 A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade",
    "genai_copilot": "4.j.5 Uso soluções do tipo Copilot",
    "crit_salario": "2.o.1_Remuneração/Salário", "crit_beneficios": "2.o.2_Benefícios",
    "crit_proposito": "2.o.3_Propósito do trabalho e da empresa",
    "crit_flex_remoto": "2.o.4_Flexibilidade de trabalho remoto",
    "crit_ambiente": "2.o.5_Ambiente e clima de trabalho",
    "crit_aprendizado": "2.o.6_Oportunidade de aprendizado e trabalhar com referências",
    "crit_carreira": "2.o.7_Plano de carreira e oportunidades de crescimento",
    "crit_maturidade": "2.o.8_Maturidade da empresa em termos de tecnologia e dados",
    "crit_gestores": "2.o.9_Qualidade dos gestores e líderes",
    "crit_reputacao": "2.o.10_Reputação que a empresa tem no mercado",
    "des_contratar": "3.d.1_Contratar talentos", "des_reter": "3.d.2_Reter talentos",
    "des_investimentos": "3.d.3_Convencer a empresa a aumentar investimentos",
    "des_remoto": "3.d.4_Gestão de equipes no ambiente remoto",
    "des_multidisciplinar": "3.d.5_Gestão de projetos envolvendo áreas multidisciplinares",
    "des_qualidade": "3.d.6_Organizar as informações com qualidade e confiabilidade",
    "des_volume": "3.d.7_Processar e armazenar um alto volume de dados",
    "des_valor": "3.d.8_Gerar valor para as áreas de negócios",
    "des_ml_prod": "3.d.9_Desenvolver e manter modelos Machine Learning em produção",
    "des_expectativas": "3.d.10_Gerenciar a expectativa das áreas",
    "des_manutencao": "3.d.11_Garantir a manutenção dos projetos e modelos em produção",
    "des_inovacao": "3.d.12_Conseguir levar inovação para a empresa",
    "des_roi": "3.d.13_Garantir (ROI) em projetos de dados",
    "des_tempo": "3.d.14_Dividir o tempo entre entregas técnicas e gestão",
}

# 2024: mesma notação de 2025, com deslocamentos pontuais de código
DEPARA_2024 = dict(DEPARA_2025)
DEPARA_2024.update({
    "layoff": "2.q_empresa_passou_por_layoff_em_2024",
    "modelo_atual": "2.r_modelo_de_trabalho_atual",
    "modelo_ideal": "2.s_modelo_de_trabalho_ideal",
    "atitude_presencial": "2.t_atitude_em_caso_de_retorno_presencial",
    "lang_sql": "4.d.1_SQL", "lang_r": "4.d.2_R", "lang_python": "4.d.3_Python",
    "cloud_aws": "4.h.1_Amazon Web Services (AWS)", "cloud_gcp": "4.h.2_Google Cloud (GCP)",
    "cloud_azure": "4.h.3_Azure (Microsoft)",
    "cloud_onprem": "4.h.6_Servidores On Premise/Não utilizamos Cloud",
    "bi_powerbi": "4.j.1_Microsoft PowerBI", "bi_qlik": "4.j.2_Qlik View/Qlik Sense",
    "bi_tableau": "4.j.3_Tableau", "bi_metabase": "4.j.4_Metabase",
    "bi_looker_studio": "4.j.8_Looker Studio(Google Data Studio)",
    "genai_nao_uso": "4.m.1 Não uso soluções de AI Generativa com foco em produtividade",
    "genai_gratuito": "4.m.2 Uso soluções gratuitas de AI Generativa com foco em produtividade",
    "genai_pago_proprio": "4.m.3 Uso e pago pelas soluções de AI Generativa com foco em produtividade",
    "genai_pago_empresa": "4.m.4 A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade",
    "genai_copilot": "4.m.5 Uso soluções do tipo Copilot",
})

DEPARA_2023 = {
    "id": "('P0', 'id')", "idade": "('P1_a ', 'Idade')", "faixa_idade": "('P1_a_1 ', 'Faixa idade')",
    "genero": "('P1_b ', 'Genero')", "cor_raca": "('P1_c ', 'Cor/raca/etnia')", "pcd": "('P1_d ', 'PCD')",
    "uf": "('P1_i_1 ', 'uf onde mora')", "regiao": "('P1_i_2 ', 'Regiao onde mora')",
    "nivel_ensino": "('P1_l ', 'Nivel de Ensino')", "area_formacao": "('P1_m ', 'Área de Formação')",
    "situacao_trabalho": "('P2_a ', 'Qual sua situação atual de trabalho?')", "setor": "('P2_b ', 'Setor')",
    "num_funcionarios": "('P2_c ', 'Numero de Funcionarios')", "gestor": "('P2_d ', 'Gestor?')",
    "cargo_gestor": "('P2_e ', 'Cargo como Gestor')", "cargo": "('P2_f ', 'Cargo Atual')",
    "nivel": "('P2_g ', 'Nivel')", "faixa_salarial": "('P2_h ', 'Faixa salarial')",
    "tempo_exp_dados": "('P2_i ', 'Quanto tempo de experiência na área de dados você tem?')",
    "satisfeito": "('P2_k ', 'Você está satisfeito na sua empresa atual?')",
    "entrevistas_6m": "('P2_m ', 'Você participou de entrevistas de emprego nos últimos 6 meses?')",
    "mudar_emprego_6m": "('P2_n ', 'Você pretende mudar de emprego nos próximos 6 meses?')",
    "layoff": "('P2_q ', 'Empresa que trabaha passou por layoff em 2023')",
    "modelo_atual": "('P2_r ', 'Atualmente qual a sua forma de trabalho?')",
    "modelo_ideal": "('P2_s ', 'Qual a forma de trabalho ideal para você?')",
    "atitude_presencial": "('P2_t ', 'Caso sua empresa decida pelo modelo 100% presencial qual será sua atitude?')",
    "prioridade_ia": "('P3_e ', 'AI Generativa é uma prioridade em sua empresa?')",
    "atuacao": "('P4_a_1 ', 'Atuacao')",
    "lang_sql": "('P4_d_1 ', 'SQL')", "lang_r": "('P4_d_2 ', 'R ')", "lang_python": "('P4_d_3 ', 'Python')",
    "cloud_aws": "('P4_h_2 ', 'Amazon Web Services (AWS)')", "cloud_gcp": "('P4_h_3 ', 'Google Cloud (GCP)')",
    "cloud_azure": "('P4_h_1 ', 'Azure (Microsoft)')",
    "cloud_onprem": "('P4_h_6 ', 'Servidores On Premise/Não utilizamos Cloud')",
    "bi_powerbi": "('P4_j_1 ', 'Microsoft PowerBI')", "bi_qlik": "('P4_j_2 ', 'Qlik View/Qlik Sense')",
    "bi_tableau": "('P4_j_3 ', 'Tableau')", "bi_metabase": "('P4_j_4 ', 'Metabase')",
    "bi_looker_studio": "('P4_j_8 ', 'Looker Studio(Google Data Studio)')",
    "genai_nao_uso": "('P4_m_1 ', 'Não uso soluções de AI Generativa com foco em produtividade')",
    "genai_gratuito": "('P4_m_2 ', 'Uso soluções gratuitas de AI Generativa com foco em produtividade')",
    "genai_pago_proprio": "('P4_m_3 ', 'Uso e pago pelas soluções de AI Generativa com foco em produtividade')",
    "genai_pago_empresa": "('P4_m_4 ', 'A empresa que trabalho paga pelas soluções de AI Generativa com foco em produtividade')",
    "genai_copilot": "('P4_m_5 ', 'Uso soluções do tipo Copilot')",
    "crit_salario": "('P2_o_1 ', 'Remuneração/Salário')", "crit_beneficios": "('P2_o_2 ', 'Benefícios')",
    "crit_proposito": "('P2_o_3 ', 'Propósito do trabalho e da empresa')",
    "crit_flex_remoto": "('P2_o_4 ', 'Flexibilidade de trabalho remoto')",
    "crit_ambiente": "('P2_o_5 ', 'Ambiente e clima de trabalho')",
    "crit_aprendizado": "('P2_o_6 ', 'Oportunidade de aprendizado e trabalhar com referências na área')",
    "crit_carreira": "('P2_o_7 ', 'Plano de carreira e oportunidades de crescimento profissional')",
    "crit_maturidade": "('P2_o_8 ', 'Maturidade da empresa em termos de tecnologia e dados')",
    "crit_gestores": "('P2_o_9 ', 'Qualidade dos gestores e líderes')",
    "crit_reputacao": "('P2_o_10 ', 'Reputação que a empresa tem no mercado')",
    "des_contratar": "('P3_d_1 ', 'a Contratar novos talentos.')",
    "des_reter": "('P3_d_2 ', 'b Reter talentos.')",
    "des_investimentos": "('P3_d_3 ', 'c Convencer a empresa a aumentar os investimentos na área de dados.')",
    "des_remoto": "('P3_d_4 ', 'd Gestão de equipes no ambiente remoto.')",
    "des_multidisciplinar": "('P3_d_5 ', 'e Gestão de projetos envolvendo áreas multidisciplinares da empresa.')",
    "des_qualidade": "('P3_d_6 ', 'f Organizar as informações e garantir a qualidade e confiabilidade.')",
    "des_volume": "('P3_d_7 ', 'g Conseguir processar e armazenar um alto volume de dados.')",
    "des_valor": "('P3_d_8 ', 'h Conseguir gerar valor para as áreas de negócios através de estudos e experimentos.')",
    "des_ml_prod": "('P3_d_9 ', 'i Desenvolver e manter modelos Machine Learning em produção.')",
    "des_expectativas": "('P3_d_10 ', 'j Gerenciar a expectativa das áreas de negócio em relação as entregas das equipes de dados.')",
    "des_manutencao": "('P3_d_11 ', 'k Garantir a manutenção dos projetos e modelos em produção, em meio ao crescimento da empresa.')",
    "des_inovacao": "('P3_d_12 ', 'Conseguir levar inovação para a empresa através dos dados.')",
    "des_roi": "('P3_d_13 ', 'Garantir retorno do investimento (ROI) em projetos de dados.')",
    "des_tempo": "('P3_d_14 ', 'Dividir o tempo entre entregas técnicas e gestão.')",
}

# ------------------------------------------------------------------
# 2. DE-PARA histórico (2019/2021/2022) — série longa mínima
# ------------------------------------------------------------------
DEPARA_HIST = {
    2019: {"genero": "('P2', 'gender')",
           "lang_sql": "('P21', 'sql_')", "lang_r": "('P21', 'r')", "lang_python": "('P21', 'python')",
           "cloud_aws": "('P25', 'aws')", "cloud_gcp": "('P25', 'gcp')", "cloud_azure": "('P25', 'azure')"},
    2021: {"genero": "('P1_b ', 'Genero')",
           "lang_sql": "('P4_d_a ', 'SQL')", "lang_r": "('P4_d_b ', 'R ')", "lang_python": "('P4_d_c ', 'Python')",
           "cloud_aws": "('P4_g_a ', 'Amazon Web Services (AWS)')",
           "cloud_gcp": "('P4_g_b ', 'Google Cloud (GCP)')", "cloud_azure": "('P4_g_c ', 'Azure (Microsoft)')"},
    2022: {"genero": "('P1_b ', 'Genero')",
           "lang_sql": "('P4_d_1 ', 'SQL')", "lang_r": "('P4_d_2 ', 'R ')", "lang_python": "('P4_d_3 ', 'Python')",
           "cloud_aws": "('P4_h_2 ', 'Amazon Web Services (AWS)')",
           "cloud_gcp": "('P4_h_3 ', 'Google Cloud (GCP)')", "cloud_azure": "('P4_h_1 ', 'Azure (Microsoft)')"},
}

# ------------------------------------------------------------------
# 3. Funções de harmonização (UDFs)
# ------------------------------------------------------------------
def parse_salario_pm(faixa):
    """Ponto médio da faixa salarial (Premissa P3).
    - 'de R$ X a R$ Y'  -> (X+Y)/2
    - 'Menos de R$ X'   -> X/2
    - 'Acima de R$ X'   -> X * 1.125
    - Correção de typo: se Y < X, multiplica Y por 10 (ex.: 'R$ 3000' -> 30.000)."""
    if faixa is None:
        return None
    nums = [float(x.replace(".", "")) for x in re.findall(r"\d[\d\.]*", faixa)]
    if not nums:
        return None
    low = nums[0]
    if "Menos" in faixa:
        return low / 2.0
    if "Acima" in faixa:
        return low * 1.125
    if len(nums) >= 2:
        high = nums[1]
        if high < low:          # typo conhecido na base 2025 ('a R$ 3000/mês')
            high = high * 10
        return (low + high) / 2.0
    return low

def harmoniza_cargo(cargo):
    """Agrupa nomenclaturas de cargo divergentes entre edições (Premissa P5)."""
    if cargo is None:
        return None
    c = cargo.lower()
    if "outra" in c: return "Outros"
    if "engenheiro de dados" in c or "data engineer" in c or "arquiteto" in c: return "Engenharia/Arquitetura de Dados"
    if "analista de dados" in c or "data analyst" in c: return "Análise de Dados"
    if "cientista de dados" in c or "data scientist" in c: return "Ciência de Dados"
    if "analista de bi" in c or "bi analyst" in c: return "Business Intelligence"
    if "analytics engineer" in c: return "Analytics Engineer"
    if "machine learning" in c or "ml engineer" in c or "ai engineer" in c: return "ML/AI Engineer"
    if "negócios" in c or "business analyst" in c: return "Análise de Negócios"
    if "desenvolvedor" in c or "software" in c or "sistemas" in c: return "Eng. de Software"
    if "product" in c or "produto" in c: return "Produto (DPM/PM)"
    if "dba" in c or "administrador de banco" in c: return "DBA"
    if "estatístico" in c or "economista" in c: return "Estatística/Economia"
    if "professor" in c or "pesquisador" in c: return "Professor/Pesquisador"
    if "suporte" in c or "analista de negócios" in c: return "Outros"
    return "Outros"

def harmoniza_modelo(m):
    if m is None: return None
    ml = m.lower()
    if "100% remoto" in ml: return "100% remoto"
    if "100% presencial" in ml: return "100% presencial"
    if "dias fixos" in ml: return "Híbrido (dias fixos)"
    if "flexível" in ml or "flexivel" in ml: return "Híbrido flexível"
    return "Outro"

def harmoniza_prioridade_ia(p):
    if p is None: return None
    if p.startswith("Sim, é nossa principal"): return "1. Principal prioridade da empresa"
    if p.startswith("Sim, está entre"): return "2. Entre as principais (2-4 anos)"
    if p.startswith("Mais ou menos"): return "3. Iniciativas isoladas, sem foco"
    if p.startswith("Não é uma iniciativa"): return "4. Não é prioridade"
    if p.startswith("Não sei"): return "5. Não sabe opinar"
    return "5. Não sabe opinar"

udf_salario = F.udf(parse_salario_pm, T.DoubleType())
udf_cargo = F.udf(harmoniza_cargo, T.StringType())
udf_modelo = F.udf(harmoniza_modelo, T.StringType())
udf_prio_ia = F.udf(harmoniza_prioridade_ia, T.StringType())

BINARIAS = [c for c in DEPARA_2025 if c.startswith(("lang_", "cloud_", "bi_", "genai_", "crit_", "des_"))] + ["gestor", "satisfeito"]

def normaliza_binaria(col):
    """'1'/'1.0'/'True' -> 1 ; '0'/'0.0'/'False' -> 0 ; demais -> null."""
    return (F.when(F.upper(F.col(col)).isin("1", "1.0", "TRUE"), F.lit(1))
             .when(F.upper(F.col(col)).isin("0", "0.0", "FALSE"), F.lit(0))
             .otherwise(F.lit(None).cast("int")))

def transforma_nucleo(df, depara, ano):
    # seleção + renomeação (backticks protegem nomes com pontos/aspas/parênteses)
    faltantes = [src for src in depara.values() if src not in df.columns]
    assert not faltantes, f"{ano}: colunas ausentes no CSV: {faltantes}"
    df = df.select([F.col(f"`{src}`").alias(dst) for dst, src in depara.items()])
    df = df.withColumn("ano", F.lit(ano))
    for c in BINARIAS:
        df = df.withColumn(c, normaliza_binaria(c))
    df = (df
          .withColumn("idade", F.col("idade").cast("int"))
          .withColumn("salario_pm", udf_salario("faixa_salarial"))
          .withColumn("cargo_grupo", udf_cargo("cargo"))
          .withColumn("modelo_atual_h", udf_modelo("modelo_atual"))
          .withColumn("modelo_ideal_h", udf_modelo("modelo_ideal"))
          .withColumn("prioridade_ia_h", udf_prio_ia("prioridade_ia"))
          .withColumn("layoff_sim", F.when(F.col("layoff").startswith("Sim"), 1)
                                     .when(F.col("layoff").startswith("Não"), 0)
                                     .otherwise(F.lit(None).cast("int"))))
    return df

def transforma_historico(df, depara, ano):
    faltantes = [src for src in depara.values() if src not in df.columns]
    assert not faltantes, f"{ano}: colunas ausentes: {faltantes}"
    df = df.select([F.col(f"`{src}`").alias(dst) for dst, src in depara.items()])
    df = df.withColumn("ano", F.lit(ano))
    for c in ["lang_sql", "lang_r", "lang_python", "cloud_aws", "cloud_gcp", "cloud_azure"]:
        df = df.withColumn(c, normaliza_binaria(c))
    return df

# ------------------------------------------------------------------
# 4. Execução
# ------------------------------------------------------------------
nucleo = None
for ano, depara in [(2023, DEPARA_2023), (2024, DEPARA_2024), (2025, DEPARA_2025)]:
    t = transforma_nucleo(ler_bronze(ano), depara, ano)
    nucleo = t if nucleo is None else nucleo.unionByName(t)

nucleo.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER}/silver_core")

# Série longa 6 anos: histórico + colunas equivalentes do núcleo
serie_cols = ["ano", "genero", "lang_sql", "lang_r", "lang_python", "cloud_aws", "cloud_gcp", "cloud_azure"]
serie = None
for ano, depara in DEPARA_HIST.items():
    t = transforma_historico(ler_bronze(ano), depara, ano)
    serie = t if serie is None else serie.unionByName(t)
serie = serie.select(serie_cols).unionByName(nucleo.select(serie_cols))
serie.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER}/silver_serie_longa")

# ------------------------------------------------------------------
# 5. Validação / reconciliação bronze x silver
# ------------------------------------------------------------------
sc = spark.read.parquet(f"{SILVER}/silver_core")
sl = spark.read.parquet(f"{SILVER}/silver_serie_longa")
print("\n== Reconciliação ==")
sc.groupBy("ano").count().orderBy("ano").show()
sl.groupBy("ano").count().orderBy("ano").show()
for ano in [2023, 2024, 2025]:
    n = sc.filter(F.col("ano") == ano).count()
    assert n == VOLUMETRIA_ESPERADA[ano], f"silver {ano} divergente"
print("Checagem salario_pm (2025):")
sc.filter("ano=2025").select("faixa_salarial", "salario_pm").distinct().orderBy("salario_pm").show(20, False)
print("Checagem prioridade IA:")
sc.groupBy("ano", "prioridade_ia_h").count().orderBy("ano", "prioridade_ia_h").show(30, False)

print("Job Bronze->Silver concluido.")

if EH_GLUE:
    job.commit()  # finaliza o Glue Job com sucesso
