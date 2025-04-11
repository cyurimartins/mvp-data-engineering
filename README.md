# 📊 MVP ENGENHARIA DE DADOS
Pipeline Campeonato Brasileiro 2024

Este projeto é parte do trabalho de pós-graduação e tem como objetivo construir um pipeline completo de dados na nuvem utilizando a plataforma **Databricks Community Edition**. O conjunto de dados analisado refere-se ao desempenho de jogadores no **Campeonato Brasileiro de 2024**.

> Projeto desenvolvido como parte da pós-graduação em Engenharia de Dados.

---

## 🎯 Objetivo

O objetivo principal é analisar dados de performance de jogadores para responder perguntas como:

- Quais jogadores mais marcaram gols?
- Quais são os mais eficientes (gols/minutos)?
- Quais clubes possuem os elencos mais produtivos?
- Existe correlação entre idade e desempenho?
- Como o desempenho varia ao longo do tempo (dimensão tempo)?
- Quais países mais contribuem com jogadores de destaque?

---

## 🛠️ Tecnologias Utilizadas

- **Databricks Community Edition**
- **PySpark** para ETL e transformação
- **Delta Lake** para versionamento e performance
- **SQL** para consultas e análise exploratória
- **Modelo Estrela** com Data Warehouse
- **GitHub** para versionamento

---

## 🗃️ Arquitetura do Pipeline

O pipeline foi dividido em **três camadas principais**:

### 🟠 Bronze (Raw)
- Dados carregados diretamente da origem, sem transformação
- Armazenamento inicial do CSV com ingestão controlada

### 🟡 Prata (Curated)
- Transformações aplicadas: limpeza, formatação de datas, joins
- Criação das tabelas dimensão:
  - `dim_jogador`
  - `dim_clube`
  - `dim_pais`
  - `dim_posicao`
  - `dim_tempo`

### 🟢 Ouro (Gold)
- Criação da tabela fato: `fato_desempenho_jogador`
- Métricas e indicadores consolidados para análises

---

## 🧠 Modelo Dimensional

O modelo estrela foi utilizado, com a seguinte estrutura:

- 🎯 **Fato:**
  - `fato_desempenho_jogador`
- 🌟 **Dimensões:**
  - `dim_jogador`
  - `dim_clube`
  - `dim_pais`
  - `dim_posicao`
  - `dim_tempo`

https://github.com/cyurimartins/mvp-data-engineering/blob/main/img/modelagem_mvp.png?raw=true

---

## 📈 Análises Realizadas

Exemplos de perguntas que podem ser respondidas:

- Top 10 jogadores com mais gols
- Ranking de assistências por clube
- Comparativo de desempenho por posição
- Evolução mensal dos gols marcados
- Jogadores mais eficientes (gols por minuto)
- Distribuição de cartões amarelos e vermelhos

---

## 🧪 Qualidade de Dados

Foi feita análise de:
- Valores nulos
- Dados inconsistentes (ex: datas com timezone)
- Colunas redundantes ou irrelevantes

A dimensão tempo foi ajustada para formato `yyyy-MM-dd` e os dias da semana foram traduzidos para português.

---

## 📂 Estrutura do Projeto


