# 📊 MVP 2025 - ENGENHARIA DE DADOS
Pipeline Campeonato Brasileiro 2024

> Projeto desenvolvido como parte da pós-graduação em Engenharia de Dados.

---

## 🎯 Objetivo

O objetivo deste projeto é construir um pipeline completo de dados para análise do desempenho de jogadores no Campeonato Brasileiro de 2024, utilizando ferramentas de Engenharia de Dados modernas e escaláveis.

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

### 🟠 Bronze (Dados Brutos)
- Dados carregados diretamente da origem, sem transformação
- Armazenamento inicial do CSV com ingestão controlada

### 🟡 Prata (Transformações dos dados)
- Transformações aplicadas: limpeza, formatação de datas, joins
- Criação das tabelas dimensão:
  - `dim_jogador`
  - `dim_clube`
  - `dim_pais`
  - `dim_posicao`
  - `dim_tempo`

### 🟢 Ouro (Métricas)
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

##### https://github.com/cyurimartins/mvp-data-engineering/blob/main/img/modelagem_mvp.png

<p align="center">
  <img src="img/modelagem_mvp.png" alt="Modelo Dimensional" width="600"/>
</p>
---

## 📈 Análises Realizadas

Exemplos de perguntas que podem ser respondidas:

- Jogador com maior participação em gols
- Jogadores com mais tempo de jogo
- Top 10 com mais minutos e seu desempenho ofensivo
- Quantidade de jogadores por país
- Clubes com mais estrangeiros
- Jogadores com mais cartões
- Total de gols e ranking de artilheiros/assistências
- Evolução dos gols por mês (gráfico de barras com total por mês)

---

## 🧪 Qualidade de Dados

Foi feita análise de:
- Valores nulos
- Padronização de datas
- Normalização de nomes
- Tradução da dimensão tempo
- Dados inconsistentes (ex: datas com timezone)
- Colunas redundantes ou irrelevantes

A dimensão tempo foi ajustada para formato `yyyy-MM-dd` e os dias da semana foram traduzidos para português.

---

## 📚 Dicionário de Dados
O dicionário de dados completo, com a descrição de todas as tabelas, colunas e seus respectivos tipos, está documentado diretamente no notebook principal:

📄 notebooks/mvp_campeonato_br_2024.ipynb

---

## 📂 Estrutura do Projeto

```
mvp-data-engineering/
├── dados/                        # Arquivos de dados brutos
│   └── camp_br_2024.csv
│
├── img/                          # Imagens utilizadas na documentação
│   └── modelagem_mvp.png
│
├── notebooks/                    # Notebook principal do projeto
│   └── mvp_campeonato_br_2024.ipynb
│
├── .gitignore                    # Arquivos ignorados pelo Git
├── LICENSE                       # Licença do projeto
└── README.md                     # Documentação principal
```





## ▶️ Como Executar

1 - Clone o repositório:

git clone https://github.com/cyurimartins/mvp-data-engineering.git

2 - Importe o notebook no Databricks Community Edition

3 - Execute as células conforme a sequência:

- Ingestão
- Limpeza
- Criação de dimensões e fato
- Visualizações

## 📄 Licença
Este projeto está licenciado sob a Licença MIT. Consulte o arquivo LICENSE para mais informações.

### 🔗 Repositório oficial: cyurimartins/mvp-data-engineering
### 🔗 Publicação Databricks: [cyurimartins/mvp-data-engineering](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2901160332205419/2815362119945264/6510660432633837/latest.html)

