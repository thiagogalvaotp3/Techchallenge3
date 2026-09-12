# Premissas Analíticas

| # | Premissa | Decisão | Onde é aplicada |
|---|---|---|---|
| P1 | Escopo das edições | Núcleo = 2023/2024/2025-26 (as 3 edições mais recentes, conforme enunciado); 2019/2021/2022 apenas em séries longas (gênero, linguagens, cloud) | Enunciado + decisão do grupo; `job_01_bronze_to_silver.py` / notebook 02 |
| P2 | Tratamento de dados sensíveis | Base pública anonimizada na origem; análise exclusivamente agregada; sem supressão adicional de células | Decisão do grupo; notebook 02 |
| P3 | Estimativa salarial | Ponto médio da faixa salarial: 'de X a Y' → (X + Y) ÷ 2; 'Menos de X' → X ÷ 2; 'Acima de X' → X × 1,125 | Notebook 02 · `job_01_bronze_to_silver.py` |
| P4 | Correção de typo (2025/26) | "de R$ 25.001/mês a R$ 3000/mês" → teto interpretado como R$ 30.000 (regra: teto<piso ⇒ teto×10); 1 registro afetado | Notebook 02 · `job_01_bronze_to_silver.py` (mesma função `parse_salario_pm` de P3) |
| P5 | Harmonização de cargos | Agrupamento em `cargo_grupo` por famílias funcionais; Engenharia de Dados e Arquitetura de Dados unificadas, seguindo o padrão de rótulo da própria edição 2023 | Notebook 02 |
| P6 | Multirresposta | Percentuais calculados sobre respondentes válidos da questão, nunca sobre o total da base | Notebook 03 · `job_02_silver_to_gold.py` (`base_valida`) |
| P7 | Booleanos heterogêneos | `1/0`, `True/False` e `TRUE/FALSE` (padrão 2024) normalizados para inteiro 1/0 | Bug corrigido no notebook 02 |
| P8 | Corte de exibição por tamanho de grupo | Recortes com **n < 30** não são exibidos em gráficos e tabelas — corte de **exibição**, não de processamento | Seção 2.6 do relatório · aplicado no G05 e no recorte regional |

## Nota sobre precisão da mediana

Todas as medianas salariais da camada Gold (P3) são calculadas com `percentile_approx` (Spark) —
uma função por amostragem, não um cálculo exato por ordenação completa. Na prática, para grupos
com dezenas ou mais de registros e valores discretos (ponto médio de faixa, poucas dezenas de
valores possíveis), a aproximação converge para o valor exato. Mas o método não garante isso
formalmente: já observamos, em investigação ad hoc no Athena, uma consulta com `APPROX_PERCENTILE`
divergir do valor exato em um recorte específico (mesmo `n`, mediana diferente) — corrigida trocando
para um cálculo exato por ranking. Se uma mediana citada em contexto de alta cobrança (banca,
auditoria) precisar de confirmação, o caminho seguro é recalculá-la por ranking exato, não repetir a
mesma função aproximada.

## P8 — por que 30, e por que isso não é comodismo

O corte não foi herdado de convenção nem escolhido para simplificar o gráfico: ele é a consequência
medida da baixa resolução do instrumento. A pesquisa coleta **faixas** salariais, não valores; a
mediana de um grupo pequeno, portanto, não varia de forma suave — ela **salta uma faixa inteira**
(até R$ 4.000) quando um único respondente muda de posição.

Reamostrando os próprios dados de 2025/26 (3.000 reamostragens com reposição), o efeito é medível:

| Grupo | n | Mediana | Intervalo de 95% sob reamostragem | Amplitude |
|---|---|---|---|---|
| Estatística/Economia | 9 | R$ 5.001 | R$ 1.501 – R$ 18.001 | **7 faixas** |
| Professor/Pesquisador | 16 | R$ 10.001 | R$ 5.001 – R$ 16.001 | 5 faixas |
| Produto (DPM/PM) | 33 | R$ 14.001 | R$ 10.001 – R$ 18.113 | 2 faixas |
| Análise de Dados | 599 | R$ 7.001 | R$ 7.001 – R$ 7.001 | estável |

*Reproduzível em `src/notebooks/02_bronze_to_silver.ipynb`, Seção 7 (semente fixa = 43).*

Exibir uma mediana apurada sobre 9 respondentes ao lado de outra apurada sobre 599, com a mesma
tipografia e no mesmo eixo, comunicaria uma precisão que o dado não possui — e é exatamente o tipo
de leitura que o relatório se propõe a evitar (Seção 2.6).

**O que o corte não faz:** não descarta registro algum do pipeline. Os grupos pequenos permanecem
íntegros na camada Gold (`gold_salary_by_role` traz os 12 grupos, inclusive os de n < 30), continuam
somando no total e são **nomeados no texto** sempre que ficam fora de um recorte. O critério é de
exibição.

**Impacto declarado:** afeta 2 dos 12 grupos de cargo de 2025/26 — Estatística/Economia (n = 9) e
Professor/Pesquisador (n = 16) —, o que representa 25 respondentes, ou **1,0%** da base com cargo
declarado; e, no recorte regional por senioridade, apenas a região Norte (n = 12 no nível Sênior).

**Quebras de série documentadas:** `Especialista/Staff+` só em 2025/26; cor/raça e PCD ausentes em 2019/2021; bloco de IA generativa nasce em 2023; `satisfeito` e `layoff_sim` presentes no `silver_core` apenas para 2023/2024/2025 — a pergunta de layoff existe no questionário desde 2022, mas 2022 está fora do núcleo (P1) e a série longa não inclui essas colunas; por isso Q7 retorna exatamente 3 linhas — uma por edição do núcleo (2023/2024/2025) — representando os 12.844 respondentes (91,7% do núcleo) que responderam satisfeito/layoff, o que é correto. Ao consultar via `COUNT(*)` em vez de `COUNT(satisfeito)`, o resultado passa a somar 14.005 (o núcleo inteiro, incluindo quem não respondeu essas duas perguntas) — use sempre `COUNT(satisfeito)`, como no notebook `04_athena_queries.ipynb` e no guia de evidências.
