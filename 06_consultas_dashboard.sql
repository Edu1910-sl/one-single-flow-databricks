-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Consultas para o dashboard (AI/BI Dashboard)
-- MAGIC
-- MAGIC Cada consulta abaixo vira um **dataset** do dashboard. No Free Edition: menu **Dashboards → Create dashboard →
-- MAGIC Data → Create from SQL** e cole uma consulta por vez. Troque `workspace.one_single_flow` se usou outro catálogo/schema.

-- COMMAND ----------

-- 1. Cartões de resumo (KPIs do período)
SELECT
  COUNT(*)                                                    AS produtos,
  ROUND(100 * AVG(CAST(first_pass AS INT)), 1)                AS fpy_pct,
  ROUND(100 * AVG(CAST(qtd_ciclos_reparo > 0 AS INT)), 1)     AS taxa_reparo_pct,
  ROUND(AVG(lead_time_horas), 1)                              AS lead_time_medio_h,
  SUM(CAST(status_atual = 'Expedido' AS INT))                 AS expedidos
FROM workspace.one_single_flow.gold_storyline_produto;

-- COMMAND ----------

-- 2. Evolução diária do First Pass Yield por linha (gráfico de linhas)
SELECT data_producao, linha, fpy_pct, taxa_reparo_pct, lead_time_medio_h
FROM workspace.one_single_flow.gold_kpi_diario
ORDER BY data_producao, linha;

-- COMMAND ----------

-- 3. Pareto dos desvios (gráfico de barras + linha de % acumulado)
SELECT nome_desvio, qtd, pct, pct_acumulado
FROM workspace.one_single_flow.gold_pareto_desvios
ORDER BY qtd DESC;

-- COMMAND ----------

-- 4. Onde estão os produtos agora (funil / barras)
SELECT status_atual, COUNT(*) AS produtos
FROM workspace.one_single_flow.gold_storyline_produto
GROUP BY status_atual
ORDER BY produtos DESC;

-- COMMAND ----------

-- 5. Busca da storyline de um produto (tabela com filtro por id no dashboard)
SELECT id, linha, status_atual, inicio_montagem_1, fim_montagem_2,
       qtd_desvios, codigos_desvio, qtd_ciclos_reparo, qtd_testes,
       status_teste_final, expedido_em, lead_time_horas
FROM workspace.one_single_flow.gold_storyline_produto
ORDER BY qtd_desvios DESC, qtd_ciclos_reparo DESC;

-- COMMAND ----------

-- 6. Saúde do pipeline: última execução das regras de qualidade
SELECT regra, valor, limite, status, executado_em
FROM workspace.one_single_flow.dq_resultados
WHERE executado_em = (SELECT MAX(executado_em) FROM workspace.one_single_flow.dq_resultados)
ORDER BY status DESC, regra;
