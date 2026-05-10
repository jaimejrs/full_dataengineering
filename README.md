# Full Data Engineering

> Pipeline ETL completo para análise de vendas com suporte a Pessoa Física e Jurídica, geolocalização por estado/cidade, orquestração via Airflow, Data Lake distribuído em HDFS e dashboard interativo.

**Projeto:** DEM-BI-2024-014-EXT | Digital College  
**Aluno:** Jaime Teixeira

---

## 📸 Screenshots

### Arquitetura do Pipeline
![Arquitetura Infografico](docs/screenshots/00_arquitetura_infografico.jpg)

### Dashboard Interativo (Dash/Plotly)
![Dash Dashboard](docs/screenshots/06_dash_dashboard.png)

### Apache Airflow — DAG com todas as tasks em sucesso
![Airflow DAG Success](docs/screenshots/03_airflow_dag.png)

### AWS Redshift — Query analítica de faturamento por estado
![Redshift Query](docs/screenshots/05_redshift_query.png)

### Hadoop HDFS — Data Lake particionado por ano/mês
![HDFS Data Lake](docs/screenshots/02_hadoop_hdfs.png)

### pgAdmin — Data Mart com tabela vendas_localidade_jaime
![Data Mart pgAdmin](docs/screenshots/01_datamart_pgadmin.png)

### Docker Desktop — Todos os containers em execução
![Docker Containers](docs/screenshots/04_docker_containers.png)

---

## 🛠️ Stack Tecnológica

| Camada | Tecnologia | Descrição |
|--------|-----------|-----------|
| **Orquestração** | ![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white) | Agendamento e monitoramento das 5 tasks do pipeline |
| **Data Warehouse** | ![AWS](https://img.shields.io/badge/AWS%20Redshift-232F3E?style=flat&logo=amazon-aws&logoColor=white) | Star Schema com 5 dimensões + fato (~342K linhas) em Redshift Serverless |
| **Data Lake** | ![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=flat&logo=apache&logoColor=black) | Armazenamento Parquet particionado por `ano/mês` no HDFS |
| **Data Mart** | ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat&logo=postgresql&logoColor=white) | Tabelas agregadas para consumo do dashboard |
| **Dashboard** | ![Dash](https://img.shields.io/badge/Dash%20Plotly-00A3E0?style=flat&logo=plotly&logoColor=white) | KPIs, gráficos de faturamento, comparativo de metas |
| **Containers** | ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white) | Airflow, Hadoop e Postgres orquestrados via docker-compose |
| **Banco Fonte** | ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat&logo=postgresql&logoColor=white) | Banco transacional com esquemas `geral`, `vendas` e `produto` |
| **Linguagem** | ![Python](https://img.shields.io/badge/Python%203.12-3776AB?style=flat&logo=python&logoColor=white) | Pipeline, transformações e dashboard |
| **Formato Lake** | ![Parquet](https://img.shields.io/badge/Apache%20Parquet-50ABF1?style=flat) | Compressão Snappy, leitura colunnar eficiente |

---

## Modelo Dimensional (Star Schema)

### Dimensões — Redshift

| Tabela | Tipo | Descrição |
|--------|------|-----------|
| `dim_tempo` | Dimensão | Calendário completo 2015–2026 (data, ano, mês, trimestre, dia da semana) |
| `dim_produto` | Dimensão | Produtos com categoria, preço de tabela e margem |
| `dim_cliente` | Dimensão | Clientes **PF e PJ** — nome, `documento` (CPF/CNPJ), `tipo_pessoa` |
| `dim_vendedor` | Dimensão | Vendedores com dados pessoais |
| `dim_localidade` | Dimensão | Geolocalização completa (bairro, cidade, estado, CEP) |
| `fato_venda` | Fato | Grão: 1 item de nota fiscal — ~342 mil linhas |

> A `dim_cliente` foi refatorada para suportar tanto **Pessoa Física** (CPF) quanto **Pessoa Jurídica** (CNPJ), unificando via `COALESCE` nas tabelas `geral.pessoa_fisica` e `geral.pessoa_juridica`.

### Data Mart — PostgreSQL (Hostinger)

| Tabela | Descrição |
|--------|-----------|
| `vendas_ano_mes_jaime` | Faturamento consolidado por ano/mês com quantidade e meta |
| `vendas_localidade_jaime` | Vendas por estado/cidade com `valor_total_esperado`, `pct_atingimento` e `data_atualizacao` (auditoria) |

---

## 🗂️ Estrutura de Pastas

```
projeto/
├── analise.ipynb            # Notebook com exploração e prototipagem
├── app.py                   # Dashboard Dash/Plotly (6 gráficos + 5 KPIs)
├── script_redshift.sql      # DDL completo do modelo estrela no Redshift
├── requirements.txt         # Dependências Python do projeto
├── Dockerfile               # Container do dashboard
├── docker-compose.yml       # Orquestração de todos os serviços
├── .env.example             # Template de variáveis sem credenciais
├── .gitignore
├── docs/
│   └── screenshots/         # Evidências do pipeline em execução
│       ├── 00_arquitetura_infografico.jpg
│       ├── 01_datamart_pgadmin.png
│       ├── 02_hadoop_hdfs.png
│       ├── 03_airflow_dag.png
│       ├── 04_docker_containers.png
│       ├── 05_redshift_query.png
│       └── 06_dash_dashboard.png
├── dags/
│   ├── pipeline_vendas.py   # DAG principal com 5 tasks (psycopg2 puro)
│   └── etl_vendas_dag.py    # DAG base auxiliar
└── lake/                    # Data Lake local (ignorado pelo Git)
    └── fato_venda/
        └── ano=YYYY/mes=MM/data_historico_atualizado.parquet
```

---

## Configuração e Execução

### 1. Variáveis de ambiente

Copie o template e preencha com suas credenciais:

```bash
cp .env.example .env
```

```env
# Banco fonte (PostgreSQL)
SOURCE_DATABASE_URL=postgresql://usuario:senha@host:5432/datadt_digital_corporativo

# Data Warehouse (AWS Redshift Serverless)
REDSHIFT_DATABASE_URL=redshift+psycopg2://usuario:senha@host:5439/vendas_dw

# Data Mart (PostgreSQL - Dashboard)
DASHBOARD_DATABASE_URL=postgresql://usuario:senha@host:5433/aula

# Data Lake (HDFS)
HDFS_URL=http://hadoop:9870
HDFS_USER=root
HDFS_DEST_PATH=/vendas/fato_vendas/
```

### 2. Executar o pipeline manualmente

1. Acesse o Airflow em http://localhost:8080
2. Ative a DAG `pipeline_vendas_digital_corporativo`
3. Clique em **Trigger DAG**
4. Acompanhe as tasks na visão de grafo

---

## Dashboard

O painel analítico contém:

- **5 KPIs:** Receita Real, Meta Esperada, Qtde Vendida, % Atingimento, Melhor Mês
- **Gráfico 1:** Receita Real vs Esperada por Mês (barras agrupadas)
- **Gráfico 2:** % Atingimento da Meta por Mês (linha com área)
- **Gráfico 3:** Quantidade Vendida por Mês (área)
- **Gráfico 4:** Desvio Real − Esperado por Mês (waterfall)
- **Gráfico 5:** Receita Real por Estado — Top 10 (barras horizontais)
- **Gráfico 6:** Receita Real vs. Meta — Top 20 Cidades (barras agrupadas)

Filtro de **ano** atualiza todos os KPIs e gráficos simultaneamente.

---

## Design do Pipeline (DAG)

### Tasks e responsabilidades

| Task | Descrição |
|------|-----------|
| `extrair_banco_fonte` | Lê PostgreSQL fonte via `psycopg2` puro, monta todas as dimensões (incluindo PF/PJ e Localidade) e carrega no Redshift via `TRUNCATE + INSERT` com bulk insert |
| `carregar_redshift` | Carga incremental da `fato_venda`: `DELETE` do dia de execução + `INSERT` dos novos registros |
| `gravar_data_lake` | Exporta `fato_venda` do dia para Parquet (Snappy), grava local e faz upload para HDFS |
| `popular_data_mart` | Recalcula `vendas_ano_mes_jaime` e `vendas_localidade_jaime` com `DELETE + INSERT` e atualiza `data_atualizacao` |
| `validar_pipeline` | Gera relatório de contagens, nulos e integridade referencial nos logs do Airflow |

### Padrão de conexão

Todas as conexões usam **`psycopg2` explícito** (sem SQLAlchemy Engine), garantindo compatibilidade com Airflow 3.x + Pandas 3.0:

```python
def get_conn(url: str):
    p = urlparse(url)
    return psycopg2.connect(
        dbname=p.path[1:], user=p.username,
        password=p.password, host=p.hostname, port=p.port
    )
```

---

## Validações Realizadas

- Contagem de linhas em cada dimensão e tabela fato
- Nulos em surrogate keys zerados após backfill de PJ
- Conferência de totais entre Data Mart e Redshift
- Idempotência: re-execuções não duplicam dados
- Dashboard sem erros de callback com filtros
- Pipeline Airflow com todas as tasks `success`
- HDFS com arquivos `data_historico_atualizado.parquet` datados de 2026-05-10
- dim_cliente suportando PF e PJ com coluna `tipo_pessoa`

---

## Segurança

- `.env` nunca commitado (protegido por `.gitignore`)
- `.env.example` disponível sem credenciais reais
- `.dockerignore` exclui `.env` e dados locais da imagem
- Nenhuma credencial hardcoded no código-fonte

---

## Licença

Projeto acadêmico desenvolvido para o curso de Engenharia de Dados — Digital College.
