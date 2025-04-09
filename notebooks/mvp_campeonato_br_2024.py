# Databricks notebook source
# MAGIC %md
# MAGIC # MVP - DATA ENGINERING
# MAGIC
# MAGIC - Aluno: Carlos Yuri Martins Braga Farias
# MAGIC - Sprint: Engenharia de Dados
# MAGIC
# MAGIC A proposta é realizar o tratamento e análise de dados relacionados ao desempenho de jogadores de futebol durante partidas, buscando extrair insights relevantes que possam contribuir para a compreensão de fatores que influenciam diretamente os resultados de times em competições.
# MAGIC
# MAGIC Este MVP terá como base um conjunto de dados estatísticos contendo atributos como gols, assistências, minutos jogados, passes, chutes, cartões, entre outros indicadores de performance individual por partida. A partir disso, pretende-se construir um modelo analítico estruturado em um modelo dimensional (Esquema Estrela) e implementar um pipeline completo de ingestão, transformação, carga e análise dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perguntas
# MAGIC
# MAGIC - Quais jogadores apresentam maior participação ofensiva (gols + assistências)?
# MAGIC - Existe correlação entre quantidade de passes certos e vitórias do time?
# MAGIC - Jogadores com mais minutos jogados têm melhor desempenho ofensivo?
# MAGIC - Quais são os jogadores mais eficientes (por exemplo, gols por minuto jogado)?
# MAGIC - Como a posição em campo influencia nas estatísticas de desempenho?
# MAGIC - Há diferenças de desempenho por nacionalidade?
# MAGIC - Jogadores com mais cartões têm menor desempenho técnico?
# MAGIC - É possível identificar padrões de desempenho que justifiquem convocações para seleções nacionais?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fonte de dados
# MAGIC
# MAGIC Fonte: Kaggle
# MAGIC
# MAGIC O conjunto de dados utilizado neste MVP consiste em estatísticas de desempenho de jogadores de futebol, coletadas em nível de partida. As informações incluem atributos individuais como gols, assistências, minutos jogados, passes, conduções, chutes, cartões, entre outros.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Modelagem
# MAGIC
# MAGIC A modelagem dos dados foi realizada no formato Esquema Estrela, com uma tabela fato central contendo as métricas de desempenho dos jogadores em cada partida, relacionada a tabelas de dimensão que descrevem entidades analíticas como jogadores, times e tempo.
# MAGIC
# MAGIC Tabela Fato: 
# MAGIC - fato_desempenho_jogador
# MAGIC
# MAGIC Dimensões: 
# MAGIC - dim_jogador: Infomações básicas dos jogadores
# MAGIC - dim_clube: Lista de clubes
# MAGIC - dim_posicao: Lista de posições
# MAGIC - dim_tempo: Tabela temporal
# MAGIC - dim_pais: Lista de países dos jogadores
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline de ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importando bibliotecas

# COMMAND ----------

# Importando bilbiotecas
import pandas as pd
import io
import requests
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import substring
from pyspark.sql.functions import max
from pyspark.sql.functions import create_map, lit
from itertools import chain
from pyspark.sql.functions import coalesce, lit
from pyspark.sql.functions import lit, create_map, col
from pyspark.sql.functions import split, explode, when
from pyspark.sql.functions import col, to_date, year, month, dayofweek, date_format
from pyspark.sql.types import DateType
from datetime import datetime, timedelta


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importando dados do github

# COMMAND ----------

# Importando dados

url = "https://raw.githubusercontent.com/cyurimartins/mvp-data-engineering/main/dados/camp_br_2024.csv"
csv = pd.read_csv(url)

df = spark.createDataFrame(csv)
df.display()


# COMMAND ----------

# Leitura da tabela bruta
df_br2024 = df

# Adiciona timestamp
df_br2024 = df_br2024.withColumn("ingestao_data", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01 - Camada Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando Banco de dados da camada bronze

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Gravando dados brutos na camada bronze

# COMMAND ----------

# Apagando a tabela se ela existir no BD Bronze
spark.sql("DROP TABLE IF EXISTS bronze.camp_br_2024")

# Criando a tabela bruta camp_br_2024
df_br2024.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.camp_br_2024")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verificando a tabela camp_brasileiro no banco de dados bronze

# COMMAND ----------

spark.sql("SELECT * FROM bronze.camp_br_2024").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02 - Camada Prata

# COMMAND ----------

# MAGIC %md
# MAGIC Criando o bando de dados Camada Prata

# COMMAND ----------

# Criando banco de dados prata (camada prata)
spark.sql("CREATE SCHEMA IF NOT EXISTS prata")


# COMMAND ----------

# Realizando a leitura da tabela bronze
df_bronze_camp_br_2024 = spark.read.table("bronze.camp_br_2024")


# COMMAND ----------

# Criando dicionário com novos nomes da coluna
colunas_renomeadas = {
    "Jogador": "jogador",
    "Time": "time",
    "#": "numero_camisa",
    "Nação": "pais",
    "Pos.": "posicao",
    "Idade": "idade",
    "Min.": "minutos",
    "Gols": "gols",
    "Assis.": "assistencias",
    "PB": "passes_basicos",
    "PT": "passes_totais",
    "TC": "toques_curtos",
    "CaG": "chutes_a_gol",
    "CrtsA": "cartoes_amarelos",
    "CrtV": "cartoes_vermelhos",
    "Contatos": "contatos",
    "Div": "divisoes",
    "Crts": "cruzamentos",
    "Bloqueios": "bloqueios",
    "xG": "xg",
    "npxG": "npxg",
    "xAG": "xag",
    "SCA": "sca",
    "GCA": "gca",
    "Cmp": "passes_completos",
    "Att": "passes_tentados",
    "Cmp%": "porcentagem_passes",
    "PrgP": "passes_prog",
    "ConduÃ§Ãµes": "conducoes",
    "PrgC": "conducoes_prog",
    "Tent": "dribles_tentados",
    "Suc": "dribles_sucesso",
    "Data": "data_partida",
    "ingestao_data": "ingestao_data"
}

# COMMAND ----------

# Aplicando os novos nomes das colunas
for original, novo in colunas_renomeadas.items():
    df_bronze_camp_br_2024 = df_bronze_camp_br_2024.withColumnRenamed(original, novo)

# COMMAND ----------

df_bronze_camp_br_2024 = df_bronze_camp_br_2024.withColumn("idade", substring("idade", 1, 2))

# COMMAND ----------

# 
spark.sql("drop table if exists prata.camp_br_2024")

# Gravando a tabela na camada prata
df_bronze_camp_br_2024.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("prata.camp_br_2024")

# COMMAND ----------

spark.sql("SELECT * FROM prata.camp_br_2024").display()


# COMMAND ----------

# Lê a tabela da camada Prata
df_prata_camp_br_2024 = spark.read.table("prata.camp_br_2024")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dimensão Clube
# MAGIC
# MAGIC tabela: dim_clube

# COMMAND ----------

# Seleciona os nomes dos times distintos
df_dim_clube = df_prata_camp_br_2024.select("time") \
                      .distinct() \
                      .withColumnRenamed("time", "clube")

# COMMAND ----------

# Janela para ordenação dos nomes (opcional, só pra manter estabilidade)
janela = Window.orderBy("clube")

# Adiciona a coluna ID sequencial a partir de 1
df_dim_clube = df_dim_clube.withColumn("id_clube", row_number().over(janela))

# COMMAND ----------

df_dim_clube = df_dim_clube.select("id_clube", "clube")

# COMMAND ----------

df_dim_clube.display()

# COMMAND ----------

spark.sql("drop table if exists prata.dim_clube")

# Gravando a dimensao clube na camada prata
df_dim_clube.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("prata.dim_clube")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_clube

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dimensão Jogador
# MAGIC
# MAGIC Tabela: dim_jogador

# COMMAND ----------

# Janela por jogador, ordenando pela data mais recente
janela = Window.partitionBy("jogador").orderBy(df_prata_camp_br_2024.data_partida.desc())

# Adiciona a linha com o número da ordem
df_jogador = df_prata_camp_br_2024.withColumn("linha", row_number().over(janela))

# Filtra a linha mais recente por jogador
df_jogador_recente = df_jogador.filter("linha = 1").select("jogador", "time", "idade", "numero_camisa", "posicao")

# COMMAND ----------

# Seleciona os jogadores
df_jogador_recente = df_jogador_recente.select("jogador", "time", "idade", "numero_camisa", "posicao").distinct()

# COMMAND ----------

# Agrupa por jogador, time e numero_camisa e pega a idade máxima
df_jogador_recente = df_jogador_recente.groupBy("jogador", "time", "numero_camisa").agg(max("idade").alias("idade"))

# COMMAND ----------

df_filtro = df_jogador_recente.filter(df_jogador_recente.time == "Flamengo")

# COMMAND ----------

df_filtro.orderBy("jogador").display()

# COMMAND ----------

# Renomeia as colunas
df_jogador = df_jogador_recente.withColumnRenamed("jogador", "nome").withColumnRenamed("time", "clube").withColumn("idade", substring("idade", 1, 2))

df_jogador

# COMMAND ----------

# Cria janela para gerar o ID
janela = Window.orderBy("nome", "clube")

# Adiciona o ID
df_jogador = df_jogador.withColumn("id_jogador", row_number().over(janela))

# COMMAND ----------

# Reorganizar colunas
df_jogador = df_jogador.select("id_jogador", "nome", "clube","idade")

# COMMAND ----------

spark.sql("drop table if exists prata.dim_jogador")

# Gravando a tabela jogador na camada prata
df_jogador.write.format("delta").mode("overwrite").saveAsTable("prata.dim_jogador")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_jogador

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dimensão País
# MAGIC
# MAGIC Tabela: dim_pais

# COMMAND ----------

# Selecionando os países da fonte de dados
df_dim_pais = df_prata_camp_br_2024.select("pais").distinct()

# COMMAND ----------

df_dim_pais = df_dim_pais.withColumnRenamed("pais", "sigla_pais")

# COMMAND ----------

# Dicionário de códigos para nomes completos
codigo_pais_to_nome = {
    "BRA": "Brasil",
    "FRA": "França",
    "COD": "República Democrática do Congo",
    "ITA": "Itália",
    "PAR": "Paraguai",
    "VEN": "Venezuela",
    "POR": "Portugal",
    "DEN": "Dinamarca",
    "URU": "Uruguai",
    "ANG": "Angola",
    "PER": "Peru",
    "CRC": "Costa Rica",
    "COL": "Colômbia",
    "ARG": "Argentina",
    "ESP": "Espanha",
    "ECU": "Equador",
    "CHI": "Chile",
    "NED": "Países Baixos",
    "SUI": "Suíça",
    "BUL": "Bulgária",
    "NIR": "Irlanda do Norte",
    "NGA": "Nigéria",
    "NCA": "Nicarágua",
}

# COMMAND ----------

# Cria o mapa para o Spark
map_expr = create_map([lit(x) for x in chain(*codigo_pais_to_nome.items())])

# Adiciona a coluna de nome completo
df_dim_pais = df_dim_pais.withColumn("pais", map_expr.getItem(col("sigla_pais")))

# COMMAND ----------

# MAGIC %md
# MAGIC Enriquecimento da dimensão país

# COMMAND ----------



# Dicionário de países e continentes
continente_map = {
    "BRA": "América do Sul",
    "ARG": "América do Sul",
    "URU": "América do Sul",
    "PAR": "América do Sul",
    "CHI": "América do Sul",
    "VEN": "América do Sul",
    "COL": "América do Sul",
    "ECU": "América do Sul",
    "PER": "América do Sul",
    "FRA": "Europa",
    "ITA": "Europa",
    "ESP": "Europa",
    "POR": "Europa",
    "NED": "Europa",
    "SUI": "Europa",
    "BUL": "Europa",
    "NIR": "Europa",
    "DEN": "Europa",
    "COD": "África",
    "ANG": "África",
    "NGA": "África",
    "CRC": "América Central",
    "NCA": "América Central"
}

# Transformar dicionário em expressão do PySpark
map_expr = create_map([lit(x) for x in chain(*continente_map.items())])

# Adicionar coluna 'continente'
df_dim_pais = df_dim_pais.withColumn("continente", map_expr.getItem(col("sigla_pais")))


# COMMAND ----------

# Tratando nulos
df_dim_pais = df_dim_pais \
    .withColumn("pais",coalesce(col("pais"), lit("Não Informado"))) \
    .withColumn("sigla_pais",coalesce(col("sigla_pais"), lit("N/I")))\
    .withColumn("continente",coalesce(col("continente"), lit("Não Informado")))

# COMMAND ----------

# Cria janela para gerar o ID
janela = Window.orderBy("sigla_pais")

# Adiciona o ID
df_dim_pais = df_dim_pais.withColumn("id_pais", row_number().over(janela))

# COMMAND ----------

# Reorganizar colunas
df_dim_pais = df_dim_pais.select("id_pais", "sigla_pais", "pais","continente")

# COMMAND ----------

df_dim_pais.display()

# COMMAND ----------

spark.sql("drop table if exists prata.dim_pais")

# Gravando a dimensao clube na camada prata
df_dim_pais.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("prata.dim_pais")

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from prata.camp_br_2024

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dimensão Posição
# MAGIC
# MAGIC Tabela: dim_posicao

# COMMAND ----------

# Selecionando os países da fonte de dados
df_dim_posicao = df_prata_camp_br_2024.select("posicao").distinct()

# COMMAND ----------

# Mudar para 3FN

# 1. Dividir por vírgula
df_split = df_dim_posicao.withColumn("posicao", split(col("posicao"), ","))

# 2. Explodir em várias linhas
df_dim_posicao_normalizado = df_split.select(explode(col("posicao")).alias("posicao")).distinct()



# COMMAND ----------

# Criando 
posicoes_dict = {
    "AM": "Meia Ofensivo",
    "LM": "Meia Esquerda",
    "FW": "Atacante",
    "LB": "Lateral Esquerdo",
    "LW": "Ponta Esquerda",
    "GK": "Goleiro",
    "RB": "Lateral Direito",
    "WB": "Ala (Wing Back)",
    "CM": "Meia Central",
    "RW": "Ponta Direita",
    "DM": "Volante",
    "CB": "Zagueiro",
    "RM": "Meia Direita"
}

# COMMAND ----------

# Transformar dicionário em expressão do PySpark
map_expr = create_map([lit(x) for x in chain(*posicoes_dict.items())])

# Adicionar coluna 'continente'
df_dim_posicao = df_dim_posicao_normalizado.withColumn("posicao_completa", map_expr.getItem(col("posicao")))

# COMMAND ----------

# Cria janela para gerar o ID
janela = Window.orderBy("posicao_completa")

# Adiciona o ID
df_dim_posicao = df_dim_posicao.withColumn("id_posicao", row_number().over(janela))

# Reorganizar colunas
df_dim_posicao = df_dim_posicao.select("id_posicao", "posicao", "posicao_completa")

# COMMAND ----------

df_dim_posicao = df_dim_posicao.withColumn(
    "tipo_posicao",
    when(col("posicao").isin("FW", "LW", "RW", "AM"), "Ataque")
    .when(col("posicao").isin("CM", "DM", "RM", "LM"), "Meio-Campo")
    .when(col("posicao").isin("CB", "LB", "RB", "WB", "GK"), "Defesa")
    .otherwise("Desconhecido")
)

# COMMAND ----------

descricao_tatica_dict = {
    "AM": "Responsável por criar jogadas ofensivas e conectar o meio com o ataque.",
    "LM": "Atua pela faixa esquerda do meio-campo, apoiando tanto a defesa quanto o ataque.",
    "FW": "Atacante central, com foco em finalizações e presença na área.",
    "LB": "Defensor lateral esquerdo, com funções defensivas e apoio nas subidas ao ataque.",
    "LW": "Extremo esquerdo ofensivo, busca abrir o campo e criar chances de gol.",
    "GK": "Último defensor da equipe, protege o gol e inicia jogadas com os pés ou mãos.",
    "RB": "Defensor lateral direito, participa da construção ofensiva e protege o lado direito.",
    "WB": "Ala com liberdade para atacar e defender pelos lados do campo.",
    "CM": "Meio-campista central com equilíbrio entre defesa e criação de jogadas.",
    "RW": "Extremo direito, busca amplitude ofensiva e cruzamentos para a área.",
    "DM": "Volante responsável por marcar e proteger a linha de defesa.",
    "CB": "Zagueiro central com função de interceptar ataques e organizar a defesa.",
    "RM": "Meia pela direita, faz transição entre defesa e ataque pelo lado do campo."
}

# COMMAND ----------

# Transformar dicionário em expressão do PySpark
map_expr = create_map([lit(x) for x in chain(*descricao_tatica_dict.items())])

# Adicionar coluna 'continente'
df_dim_posicao = df_dim_posicao.withColumn("descricao_tatica", map_expr.getItem(col("posicao")))

# COMMAND ----------

spark.sql("drop table if exists prata.dim_posicao")

# Gravando a dimensao clube na camada prata
df_dim_posicao.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("prata.dim_posicao")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dimensão Tempo
# MAGIC
# MAGIC Tabela: dim_tempo

# COMMAND ----------

# Definindo intervalo de datas
datainicio = datetime(2018, 1, 1)
datafim = datetime(2030, 12, 31)

# COMMAND ----------

# Criando lista de datas
data_lista = [(datainicio + timedelta(days=x)) for x in range(0, (datafim - datainicio).days + 1)]

# Criando o DataFrame
df_dim_tempo = spark.createDataFrame([(d,) for d in data_lista], ["data"])

# COMMAND ----------

# Adicionando colunas
df_dim_tempo = df_dim_tempo.withColumn("ano", year("data")) \
                   .withColumn("mes", month("data")) \
                   .withColumn("dia", date_format("data", "dd")) \
                   .withColumn("dia_semana", date_format("data", "EEEE")) \
                   .withColumn("mes", date_format("data", "MMMM")) \
                   .withColumn("trimestre", ((month("data") - 1) / 3 + 1).cast("int"))

# COMMAND ----------

# Traduzindo os dias da semana
df_dim_tempo = df_dim_tempo.withColumn(
    "dia_semana", when(col("dia_semana") == "Monday", "Segunda-feira")
                 .when(col("dia_semana") == "Tuesday", "Terça-feira")
                 .when(col("dia_semana") == "Wednesday", "Quarta-feira")
                 .when(col("dia_semana") == "Thursday", "Quinta-feira")
                 .when(col("dia_semana") == "Friday", "Sexta-feira")
                 .when(col("dia_semana") == "Saturday", "Sábado")
                 .when(col("dia_semana") == "Sunday", "Domingo")
)

# COMMAND ----------

# Traduzindo os dias da semana

df_dim_tempo = df_dim_tempo.withColumn(
          "mes",when(col("mes") == "January", "Janeiro")
               .when(col("mes") == "February", "Fevereiro")
               .when(col("mes") == "March", "Março")
               .when(col("mes") == "April", "Abril")
               .when(col("mes") == "May", "Maio")
               .when(col("mes") == "June", "Junho")
               .when(col("mes") == "July", "Julho")
               .when(col("mes") == "August", "Agosto")
               .when(col("mes") == "September", "Setembro")
               .when(col("mes") == "October", "Outubro")
               .when(col("mes") == "November", "Novembro")
               .when(col("mes") == "December", "Dezembro")
)

# COMMAND ----------

df_dim_tempo = df_dim_tempo.withColumn("data", to_date("data"))


# COMMAND ----------

spark.sql("drop table if exists prata.dim_tempo")

# Gravando a dimensao clube na camada prata
df_dim_tempo.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("prata.dim_tempo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_tempo
# MAGIC where ano = '2025'

# COMMAND ----------

# MAGIC %sql
# MAGIC select jogador, posicao, time,  round(sum(minutos) / 60, 4) as horas from prata.camp_br_2024
# MAGIC group by jogador, posicao, time
# MAGIC order by 4 desc

# COMMAND ----------

df_prata_camp_br_2024.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando tabela de estatisticas consolidada

# COMMAND ----------

# MAGIC %md
# MAGIC ### 03 - Camada OURO

# COMMAND ----------

# Criando banco de dados ouro (camada ouro)
spark.sql("CREATE SCHEMA IF NOT EXISTS ouro")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fato Desempenho Jogador

# COMMAND ----------

# MAGIC %md
# MAGIC Criando tabela fato na camada ouro, no modelo estrela, utilizando SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ouro.fato_desempenho_jogador AS
# MAGIC SELECT
# MAGIC     j.id_jogador,
# MAGIC     c.id_clube,
# MAGIC     p.id_pais,
# MAGIC     pos.id_posicao,
# MAGIC     t.data,
# MAGIC
# MAGIC     e.minutos,
# MAGIC     e.gols,
# MAGIC     e.assistencias,
# MAGIC     e.passes_basicos,
# MAGIC     e.passes_totais,
# MAGIC     e.toques_curtos,
# MAGIC     e.chutes_a_gol,
# MAGIC     e.cartoes_amarelos,
# MAGIC     e.cartoes_vermelhos,
# MAGIC     e.contatos,
# MAGIC     e.divisoes,
# MAGIC     e.cruzamentos,
# MAGIC     e.bloqueios,
# MAGIC     e.xg,
# MAGIC     e.npxg,
# MAGIC     e.xag,
# MAGIC     e.sca,
# MAGIC     e.gca,
# MAGIC     e.passes_completos,
# MAGIC     e.passes_tentados,
# MAGIC     e.porcentagem_passes,
# MAGIC     e.passes_prog,
# MAGIC     --e.conducoes,
# MAGIC     e.conducoes_prog,
# MAGIC     e.dribles_tentados,
# MAGIC     e.dribles_sucesso
# MAGIC
# MAGIC FROM prata.camp_br_2024 e
# MAGIC JOIN prata.dim_jogador j    ON e.jogador = j.nome
# MAGIC JOIN prata.dim_clube c      ON e.time = c.clube
# MAGIC JOIN prata.dim_pais p       ON e.pais = p.sigla_pais
# MAGIC JOIN prata.dim_posicao pos  ON e.posicao = pos.posicao
# MAGIC JOIN prata.dim_tempo t      ON TO_DATE(e.data_partida) = t.data

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando a criação da tabela fato

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ouro.fato_desempenho_jogador

# COMMAND ----------

# MAGIC %md
# MAGIC Verificando a quantidade de registros da tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from ouro.fato_desempenho_jogador

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela fato possui 10480 registros. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### AMBIENTE DE TESTES

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.camp_br_2024

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_clube

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_jogador

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_pais

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_posicao

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prata.dim_tempo

# COMMAND ----------

# MAGIC %md
# MAGIC Qualidade dos Dados
# MAGIC
# MAGIC - Checagem de valores nulos e inconsistências.
# MAGIC - Garantia de unicidade nas dimensões.
# MAGIC - Conversão e formatação de datas.
# MAGIC - Verificação de consistência de nomes entre a tabela bruta e dimensões.
# MAGIC
# MAGIC