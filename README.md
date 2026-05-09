# TechVendas — Pipeline de Dados com Geolocalização

Pipeline ETL completo para análise de vendas com extensão geográfica, orquestração Airflow, Data Lake em Parquet/HDFS e dashboard interativo.

**Projeto:** DEM-BI-2024-014-EXT | Digital College  
**Aluno:** Jaime

---

## Arquitetura do Pipeline

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  PostgreSQL  │────▸│   Redshift   │────▸│  Data Lake   │────▸│  Data Mart   │────▸│  Dashboard   │
│  (Fonte)     │     │  (DW Star)   │     │ Parquet/HDFS │     │  PostgreSQL  │     │  Dash/Plotly │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                    │                    │
    Extração           5 dimensões         Particionado         Agregações           KPIs + 6
    SQLAlchemy        + fato_venda         ano/mês/snappy       por período          gráficos
                                                                e localidade
                              Orquestrado pelo Apache Airflow (DAG: pipeline_vendas)
```

## Modelo Dimensional (Star Schema)

| Tabela | Tipo | Descrição |
|--------|------|-----------|
| `dim_tempo` | Dimensão | Calendário 2015–2026 (data, ano, mês, trimestre, dia da semana) |
| `dim_produto` | Dimensão | Produtos com categoria e preço de tabela |
| `dim_cliente` | Dimensão | Clientes pessoa física (nome, CPF) |
| `dim_vendedor` | Dimensão | Vendedores pessoa física |
| `dim_localidade` | Dimensão | **NOVO** — Geolocalização (bairro, cidade, estado, CEP) |
| `fato_venda` | Fato | Grão: 1 item de nota fiscal (~342K linhas) |

### Data Mart (PostgreSQL)

| Tabela | Descrição |
|--------|-----------|
| `vendas_ano_mes_jaime` | Vendas agregadas por ano/mês |
| `vendas_localidade_jaime` | **NOVO** — Vendas agregadas por estado/cidade com % atingimento |

## Tecnologias

| Componente | Tecnologia |
|------------|------------|
| Linguagem | Python 3.12 |
| Banco Fonte | PostgreSQL (alwaysdata) |
| Data Warehouse | Amazon Redshift Serverless |
| Data Lake | Parquet local + Apache HDFS |
| Data Mart | PostgreSQL (Hostinger) |
| Dashboard | Dash 4.1 + Plotly 6.7 |
| Orquestração | Apache Airflow |
| Container | Docker + docker-compose |
| Bibliotecas | pandas, SQLAlchemy, pyarrow, python-dotenv |

## Estrutura de Pastas

```
projeto/
├── analise.ipynb           # Notebook com desenvolvimento do pipeline
├── app.py                  # Dashboard Dash/Plotly (6 gráficos + 5 KPIs)
├── script_redshift.sql     # DDL do modelo estrela no Redshift
├── requirements.txt        # Dependências Python
├── Dockerfile              # Container do dashboard
├── docker-compose.yml      # Orquestração Docker
├── .env.example            # Template de variáveis de ambiente
├── .gitignore              # Arquivos ignorados pelo Git
├── .dockerignore           # Arquivos ignorados pelo Docker
├── dags/
│   ├── pipeline_vendas.py  # DAG principal (5 tasks)
│   └── etl_vendas_dag.py   # DAG auxiliar (versão base)
└── lake/                   # Data Lake local (ignorado pelo Git)
    └── fato_venda/
        └── ano=YYYY/mes=MM/*.parquet
```

## Configuração

### 1. Clonar o repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd projeto
```

### 2. Criar ambiente virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configurar variáveis de ambiente

Copie o template e preencha com suas credenciais:

```bash
cp .env.example .env
```

Edite o `.env` com os valores reais:

```env
SOURCE_DATABASE_URL=postgresql://usuario:senha@host:5432/datadt_digital_corporativo
REDSHIFT_DATABASE_URL=redshift+psycopg2://usuario:senha@host:5439/vendas_dw
DASHBOARD_DATABASE_URL=postgresql://usuario:senha@host:5433/aula
HDFS_URL=http://hadoop:9870
HDFS_USER=root
```

> ⚠️ **NUNCA** commite o arquivo `.env`. Ele contém credenciais sensíveis.

## Execução

### Notebook

```bash
jupyter notebook analise.ipynb
```

Execute as células em ordem para:
1. Extrair dados do banco fonte
2. Criar dimensões e fato no Redshift
3. Exportar para Parquet/HDFS
4. Agregar e carregar no Data Mart

### Dashboard Local

```bash
python app.py
```

Acesse: [http://localhost:8050](http://localhost:8050)

### Dashboard via Docker

```bash
docker compose up --build
```

Acesse: [http://localhost:8050](http://localhost:8050)

### DAG no Airflow

1. Copie `dags/pipeline_vendas.py` para a pasta de DAGs do Airflow
2. Acesse a interface do Airflow
3. Ative a DAG `pipeline_vendas_digital_corporativo`
4. Execute com `Trigger DAG`
5. Verifique os logs de cada task

## Dashboard

O painel contém:

- **5 KPIs:** Receita Real, Meta Esperada, Qtde Vendida, % Atingimento, Melhor Mês
- **Gráfico 1:** Receita Real vs Esperada por Mês
- **Gráfico 2:** % Atingimento da Meta por Mês
- **Gráfico 3:** Quantidade Vendida por Mês (área)
- **Gráfico 4:** Desvio Real − Esperado por Mês
- **Gráfico 5:** Receita Real por Estado — Top 10 (barras horizontais)
- **Gráfico 6:** Receita Real vs. Meta — Top 20 Cidades (barras agrupadas)

Filtro de ano atualiza todos os elementos simultaneamente.

## Validações Realizadas

- Contagem de linhas em cada dimensão e fato
- Nulos em surrogate keys documentados
- Conferência de totais entre Data Mart e fato
- Idempotência via `TRUNCATE + INSERT` em todas as cargas
- Dashboard sem erros de callback
- Pipeline Airflow executável com logs de contagem

## Limitações Conhecidas

1. **`sk_cliente` nulo (~41K linhas):** Clientes pessoa jurídica (PJ) não possuem registro em `pessoa_fisica`, gerando nulos no JOIN. Esses registros mantêm `sk_cliente = NULL` na fato
2. **`sk_localidade` nulo:** Clientes sem endereço cadastrado na tabela `geral.endereco` terão localidade nula
3. **HDFS opcional:** O pipeline funciona sem HDFS, usando o Data Lake local em Parquet

## Segurança

- `.env` nunca é commitado (protegido por `.gitignore`)
- `.env.example` disponível sem credenciais reais
- `.dockerignore` exclui `.env` e dados locais da imagem Docker
- Nenhuma credencial hardcoded em código-fonte
