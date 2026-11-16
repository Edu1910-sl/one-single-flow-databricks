# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Gerar dados fictícios das fontes de origem
# MAGIC
# MAGIC Simula **6 fontes diferentes** de uma linha de produção, cada uma com seu formato e seus problemas reais de qualidade:
# MAGIC
# MAGIC | Fonte | Formato | Problemas simulados de propósito |
# MAGIC |---|---|---|
# MAGIC | `montagem_1` | CSV | IDs com espaço/prefixo, datas em 2 formatos, linhas duplicadas |
# MAGIC | `montagem_2` | CSV | Mesmo padrão; parte dos produtos ainda não chegou nesta etapa |
# MAGIC | `desvios_manuais` | JSON | Descrição digitada à mão com erros de digitação, IDs inválidos |
# MAGIC | `reparos` | CSV | Produto pode entrar no reparo mais de uma vez (loop) |
# MAGIC | `testes` | JSON | Reprovações que voltam para o reparo e são testadas de novo |
# MAGIC | `expedicao` | CSV | Datas de expedição ainda vazias para parte dos produtos |
# MAGIC
# MAGIC Todos os dados são **fictícios**, gerados com semente fixa (o resultado é sempre o mesmo).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "one_single_flow")
dbutils.widgets.text("raw_path", "")  # deixe vazio para usar o volume do Unity Catalog
dbutils.widgets.text("qtd_produtos", "1500")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW = dbutils.widgets.get("raw_path") or f"/Volumes/{CATALOG}/{SCHEMA}/raw"
QTD_PRODUTOS = int(dbutils.widgets.get("qtd_produtos"))

print("Gravando arquivos brutos em:", RAW)

# COMMAND ----------

import random
from datetime import datetime, timedelta

random.seed(42)

INICIO_PERIODO = datetime(2026, 10, 1, 6, 0, 0)

# Descrições "digitadas pelo operador", agrupadas pelo código correto.
# A camada Silver vai ter que descobrir o código a partir do texto livre.
DESCRICOES = {
    "VAZ_OLEO":  ["vazamento de oleo", "Vazamento de Óleo", "vazament de oleo", "VAZAMENTO OLEO", "vazamento  de óleo "],
    "TORQUE":    ["torque fora do especificado", "Torque fora", "torq fora espec", "TORQUE FORA DO ESPECIFICADO"],
    "PECA_FALT": ["falta de peca", "Falta de peça", "peça faltando", "falta peca"],
    "RISCO":     ["risco na carcaca", "Risco na carcaça", "riscado", "RISCO CARCACA"],
    "CONECTOR":  ["conector solto", "Conector mal encaixado", "conect solto", "CONECTOR SOLTO"],
}
DESCRICOES_SEM_PADRAO = ["barulho estranho", "verificar depois", "???"]


def fmt_data(dt):
    """95% no formato ISO, 5% no formato brasileiro (problema clássico de integração)."""
    if random.random() < 0.05:
        return dt.strftime("%d/%m/%Y %H:%M")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_id(id_num):
    """O mesmo produto escrito de jeitos diferentes em cada sistema."""
    r = random.random()
    if r < 0.85:
        return str(id_num)
    if r < 0.92:
        return f" {id_num} "
    if r < 0.97:
        return f"PRD-{id_num}"
    return f"prd{id_num}"


def id_invalido():
    return random.choice(["", "???", "SEM ID", "12345"])

# COMMAND ----------

montagem_1, montagem_2, desvios, reparos, testes, expedicao = [], [], [], [], [], []

for i in range(QTD_PRODUTOS):
    id_produto = 1_000_000 + i
    linha = random.choice(["L1", "L2"])
    inicio_m1 = INICIO_PERIODO + timedelta(days=random.randint(0, 29), minutes=random.randint(0, 16 * 60))
    fim_m1 = inicio_m1 + timedelta(minutes=random.randint(90, 240))
    montagem_1.append((fmt_id(id_produto), linha, f"M1-{random.randint(1, 8):02d}", fmt_data(inicio_m1), fmt_data(fim_m1), f"OP{random.randint(100, 160)}"))

    # Desvios encontrados na montagem (registro manual)
    tem_desvio = random.random() < 0.18
    if tem_desvio:
        for _ in range(random.choice([1, 1, 2])):
            if random.random() < 0.08:
                descricao = random.choice(DESCRICOES_SEM_PADRAO)
            else:
                descricao = random.choice(random.choice(list(DESCRICOES.values())))
            momento = fim_m1 - timedelta(minutes=random.randint(5, 60))
            id_registro = id_invalido() if random.random() < 0.03 else fmt_id(id_produto)
            desvios.append((id_registro, random.choice(["Montagem 1", "Montagem 2"]), descricao, fmt_data(momento), f"OP{random.randint(100, 160)}"))

    # 6% dos produtos ainda estão na montagem 1 (fluxo em andamento)
    if random.random() < 0.06:
        continue

    inicio_m2 = fim_m1 + timedelta(minutes=random.randint(10, 120))
    fim_m2 = inicio_m2 + timedelta(minutes=random.randint(60, 180))
    montagem_2.append((fmt_id(id_produto), linha, f"M2-{random.randint(1, 6):02d}", fmt_data(inicio_m2), fmt_data(fim_m2), f"OP{random.randint(100, 160)}"))

    # Produtos dos últimos 2 dias: metade ainda não chegou ao teste
    if inicio_m1 >= INICIO_PERIODO + timedelta(days=28) and random.random() < 0.5:
        continue

    # Loop de reparo: produto com desvio vai para o reparo (pode voltar mais de uma vez)
    momento = fim_m2
    ciclos = 0
    if tem_desvio and random.random() < 0.85:
        ciclos = random.choice([1, 1, 1, 2])
    for c in range(ciclos):
        entrada = momento + timedelta(minutes=random.randint(15, 90))
        saida = entrada + timedelta(minutes=random.randint(30, 300))
        resultado = "OK" if c == ciclos - 1 else "RETRABALHO"
        reparos.append((fmt_id(id_produto), "Montagem", fmt_data(entrada), fmt_data(saida), resultado))
        momento = saida

    # Teste de bancada (reprovação gera novo reparo + novo teste)
    aprovado = False
    for tentativa in range(1, 4):
        testado_em = momento + timedelta(minutes=random.randint(20, 120))
        chance_reprovar = 0.15 if tem_desvio else 0.04
        status = "REPROVADO" if (random.random() < chance_reprovar and tentativa < 3) else "APROVADO"
        testes.append((fmt_id(id_produto), fmt_data(testado_em), status, round(random.uniform(1850, 2000), 1), f"BANCO-{random.randint(1, 5):02d}"))
        momento = testado_em
        if status == "APROVADO":
            aprovado = True
            break
        entrada = momento + timedelta(minutes=random.randint(15, 60))
        saida = entrada + timedelta(minutes=random.randint(30, 240))
        reparos.append((fmt_id(id_produto), "Teste", fmt_data(entrada), fmt_data(saida), "OK"))
        momento = saida

    # Expedição: parte dos aprovados ainda aguarda envio
    if aprovado:
        pronto_em = momento + timedelta(hours=random.randint(1, 12))
        expedido_em = fmt_data(pronto_em + timedelta(hours=random.randint(4, 72))) if random.random() < 0.85 else None
        expedicao.append((fmt_id(id_produto), fmt_data(pronto_em), expedido_em))

# Duplicidades acidentais (reenvio de arquivo, clique duplo, etc.)
montagem_1 += random.sample(montagem_1, k=int(len(montagem_1) * 0.03))
testes += random.sample(testes, k=int(len(testes) * 0.02))

print({"montagem_1": len(montagem_1), "montagem_2": len(montagem_2), "desvios_manuais": len(desvios),
       "reparos": len(reparos), "testes": len(testes), "expedicao": len(expedicao)})

# COMMAND ----------

def gravar_csv(linhas, colunas, nome):
    df = spark.createDataFrame(linhas, colunas)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RAW}/{nome}")


def gravar_json(linhas, colunas, nome):
    df = spark.createDataFrame(linhas, colunas)
    df.coalesce(1).write.mode("overwrite").json(f"{RAW}/{nome}")


gravar_csv(montagem_1, "id_produto string, linha string, posto string, inicio string, fim string, operador string", "montagem_1")
gravar_csv(montagem_2, "id_produto string, linha string, posto string, inicio string, fim string, operador string", "montagem_2")
gravar_json(desvios, "id_produto string, etapa string, descricao string, registrado_em string, operador string", "desvios_manuais")
gravar_csv(reparos, "id_produto string, origem string, entrada string, saida string, resultado string", "reparos")
gravar_json(testes, "id_produto string, testado_em string, status string, torque_nm double, bancada string", "testes")
gravar_csv(expedicao, "id_produto string, pronto_em string, expedido_em string", "expedicao")

print("Arquivos brutos gravados.")
