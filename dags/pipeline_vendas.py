"""
DAG: pipeline_vendas_digital_corporativo
Pipeline ETL de Vendas com Geolocalização — Airflow 3.x compatível

Fluxo:
    PostgreSQL (fonte) → Redshift (DW) → Parquet/HDFS (Lake) → PostgreSQL (Data Mart)

Estratégia de carga:
    - Dimensões : FULL (TRUNCATE + INSERT)
    - Fato       : INCREMENTAL por data de execução (DELETE dia + INSERT dia)
    - Data Lake  : INCREMENTAL — grava apenas a partição do dia (overwrite=True)
    - Data Mart  : FULL a partir do Redshift, com coluna data_atualizacao
"""

import os
import logging
from datetime import datetime, timedelta
import warnings

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from urllib.parse import urlparse
from dotenv import load_dotenv

# Ocultar o UserWarning do Pandas sobre DBAPI2
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")

# ── Configuração ──────────────────────────────────────────────────────────────
env_paths = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'),        # Na própria pasta dags/
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'), # Na pasta pai
    '/home/jota/digital_college/projeto_python14/projeto/.env'              # Fallback hardcoded
]

for p in env_paths:
    if os.path.exists(p):
        load_dotenv(dotenv_path=p)
        break

log = logging.getLogger(__name__)


# ── Helpers Psycopg2 (Bypass SQLAlchemy/Pandas conflict) ─────────────────────
def get_source_url():
    url = os.environ.get("SOURCE_DATABASE_URL")
    if not url:
        raise ValueError("Variável SOURCE_DATABASE_URL não encontrada! Verifique o .env")
    return url

def get_dw_url():
    url = os.environ.get("REDSHIFT_DATABASE_URL")
    if not url:
        raise ValueError("Variável REDSHIFT_DATABASE_URL não encontrada! Verifique o .env")
    if url.startswith("redshift+psycopg2://"):
        url = url.replace("redshift+psycopg2://", "postgresql://")
    return url

def get_dm_url():
    url = os.environ.get("DASHBOARD_DATABASE_URL")
    if not url:
        raise ValueError("Variável DASHBOARD_DATABASE_URL não encontrada! Verifique o .env")
    return url

def _get_pg_conn(url):
    p = urlparse(url)
    return psycopg2.connect(
        dbname=p.path[1:],
        user=p.username,
        password=p.password,
        host=p.hostname,
        port=p.port
    )

def execute_query(url, query):
    """Executa uma query DML/DDL simples."""
    conn = _get_pg_conn(url)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def read_sql_from_url(query, url):
    """Lê um DataFrame via DBAPI puro, contornando a Engine do Pandas."""
    conn = _get_pg_conn(url)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def _truncate_insert(url_str, df, table, schema="public", chunksize=2000):
    """TRUNCATE + INSERT bulk via psycopg2."""
    execute_query(url_str, f"TRUNCATE TABLE {schema}.{table}")
    append_to_table(url_str, df, table, schema, chunksize)

def append_to_table(url_str, df, table, schema="public", chunksize=2000):
    """INSERT bulk via psycopg2."""
    if df.empty: return
    # Tratar NaNs para NULL no banco
    df = df.where(pd.notnull(df), None)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    
    conn = _get_pg_conn(url_str)
    cur = conn.cursor()
    query = f"INSERT INTO {schema}.{table} ({cols}) VALUES %s"
    execute_values(cur, query, tuples, page_size=chunksize)
    conn.commit()
    cur.close()
    conn.close()

def add_sk(df, col_name="sk"):
    df = df.copy().reset_index(drop=True)
    df.insert(0, col_name, range(1, len(df) + 1))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1 — extrair_banco_fonte
# ══════════════════════════════════════════════════════════════════════════════
def extrair_banco_fonte(**context):
    url_src = get_source_url()
    url_rs  = get_dw_url()

    # ── DIM TEMPO ─────────────────────────────────────────────────────────────
    datas = pd.date_range("2015-01-01", "2026-12-31")
    dim_tempo = pd.DataFrame({
        "data":          datas,
        "ano":           datas.year,
        "mes":           datas.month,
        "trimestre":     datas.quarter,
        "nome_mes":      datas.month_name(),
        "dia_semana":    datas.day_name(),
        "is_fim_semana": datas.weekday >= 5,
    })
    add_sk(dim_tempo, "sk_tempo").pipe(
        lambda df: _truncate_insert(url_rs, df, "dim_tempo")
    )
    log.info(f"dim_tempo: {len(dim_tempo)} linhas")

    # ── DIM PRODUTO ────────────────────────────────────────────────────────────
    df_produto = read_sql_from_url("""
        SELECT p.id AS id_produto, p.nome AS nome,
               cat.descricao AS categoria, p.valor_venda AS preco_tabela
        FROM vendas.produto p
        JOIN vendas.categoria cat ON cat.id = p.id_categoria
    """, url_src)
    add_sk(df_produto, "sk_produto").pipe(
        lambda df: _truncate_insert(url_rs, df, "dim_produto")
    )
    log.info(f"dim_produto: {len(df_produto)} linhas")

    # ── DIM CLIENTE (PF + PJ) ─────────────────────────────────────────────────
    df_cliente = read_sql_from_url("""
        SELECT DISTINCT
            p.id                                        AS id_pessoa,
            COALESCE(pf.nome, pj.razao_social)          AS nome,
            COALESCE(pf.cpf,  pj.cnpj)                  AS cpf
        FROM geral.pessoa p
        LEFT JOIN geral.pessoa_fisica   pf ON pf.id = p.id
        LEFT JOIN geral.pessoa_juridica pj ON pj.id = p.id
        WHERE p.id IN (SELECT id_cliente FROM vendas.nota_fiscal)
          AND (pf.id IS NOT NULL OR pj.id IS NOT NULL)
    """, url_src)
    add_sk(df_cliente, "sk_cliente").pipe(
        lambda df: _truncate_insert(url_rs, df, "dim_cliente")
    )
    log.info(f"dim_cliente: {len(df_cliente)} linhas (PF+PJ)")

    # ── DIM VENDEDOR ───────────────────────────────────────────────────────────
    df_vendedor = read_sql_from_url("""
        SELECT DISTINCT pf.id AS id_pessoa, pf.nome AS nome
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_vendedor
    """, url_src)
    add_sk(df_vendedor, "sk_vendedor").pipe(
        lambda df: _truncate_insert(url_rs, df, "dim_vendedor")
    )
    log.info(f"dim_vendedor: {len(df_vendedor)} linhas")

    # ── DIM LOCALIDADE ─────────────────────────────────────────────────────────
    df_localidade = read_sql_from_url("""
        SELECT DISTINCT
            p.id          AS id_pessoa,
            b.descricao   AS bairro,
            c.descricao   AS cidade,
            est.sigla     AS sigla_estado,
            est.descricao AS estado,
            e.cep
        FROM geral.pessoa p
        LEFT JOIN geral.endereco e   ON e.id_pessoa = p.id
        LEFT JOIN geral.bairro   b   ON b.id = e.id_bairro
        LEFT JOIN geral.cidade   c   ON c.id = b.id_cidade
        LEFT JOIN geral.estado   est ON est.id = c.id_estado
        WHERE e.id IS NOT NULL
    """, url_src)
    add_sk(df_localidade, "sk_localidade").pipe(
        lambda df: _truncate_insert(url_rs, df, "dim_localidade")
    )
    log.info(f"dim_localidade: {len(df_localidade)} linhas")

    context['ti'].xcom_push(key='contagens_dim', value={
        'dim_tempo':       len(dim_tempo),
        'dim_produto':     len(df_produto),
        'dim_cliente':     len(df_cliente),
        'dim_vendedor':    len(df_vendedor),
        'dim_localidade':  len(df_localidade),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2 — carregar_redshift
# ══════════════════════════════════════════════════════════════════════════════
def carregar_redshift(**context):
    execution_date = context['ds']
    url_src = get_source_url()
    url_rs  = get_dw_url()

    log.info(f"Carregando fato para data de execução: {execution_date}")

    df_raw = read_sql_from_url(f"""
        SELECT
            nf.data_venda::date  AS data_venda,
            nf.id_cliente,
            nf.id_vendedor,
            inf.id_produto,
            nf.id                AS id_nota,
            inf.quantidade,
            inf.valor_unitario,
            inf.valor_venda_real,
            p.valor_venda
        FROM vendas.nota_fiscal nf
        JOIN vendas.item_nota_fiscal inf ON inf.id_nota_fiscal = nf.id
        JOIN vendas.produto p            ON p.id = inf.id_produto
        WHERE nf.data_venda::date = '{execution_date}'
    """, url_src)

    if df_raw.empty:
        log.info(f"Nenhuma venda encontrada para {execution_date}. Encerrando task.")
        context['ti'].xcom_push(key='fato_count', value=0)
        return

    df_fato = df_raw.copy()
    df_fato["data_venda"] = pd.to_datetime(df_fato["data_venda"]).dt.date

    dim_tempo      = read_sql_from_url("SELECT sk_tempo, data FROM public.dim_tempo", url_rs)
    dim_produto    = read_sql_from_url("SELECT sk_produto, id_produto FROM public.dim_produto", url_rs)
    dim_cliente    = read_sql_from_url("SELECT sk_cliente, id_pessoa FROM public.dim_cliente", url_rs)
    dim_vendedor   = read_sql_from_url("SELECT sk_vendedor, id_pessoa FROM public.dim_vendedor", url_rs)
    dim_localidade = read_sql_from_url("SELECT sk_localidade, id_pessoa FROM public.dim_localidade", url_rs)

    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"]).dt.date

    df_fato = (df_fato
        .merge(dim_tempo.rename(columns={"data": "data_venda"}), on="data_venda", how="left")
        .merge(dim_produto, on="id_produto", how="left")
        .merge(dim_cliente.rename(columns={"id_pessoa": "id_cliente"}), on="id_cliente", how="left")
        .merge(dim_vendedor.rename(columns={"id_pessoa": "id_vendedor"}), on="id_vendedor", how="left")
        .merge(dim_localidade.rename(columns={"id_pessoa": "id_cliente"}), on="id_cliente", how="left")
    )

    df_fato["desconto"]     = (df_fato["valor_venda"] - df_fato["valor_unitario"]).round(2)
    df_fato["pct_desconto"] = (df_fato["desconto"] / df_fato["valor_unitario"] * 100).round(2)

    cols = ["sk_tempo", "sk_produto", "sk_cliente", "sk_vendedor", "sk_localidade",
            "id_nota", "quantidade", "valor_unitario", "valor_venda_real",
            "desconto", "pct_desconto"]
    df_fato = df_fato[cols]

    nulos = df_fato[["sk_cliente", "sk_localidade"]].isnull().sum()
    log.info(f"Nulos nas SKs:\n{nulos}")

    # DELETE do dia + INSERT (idempotente)
    execute_query(url_rs, f"""
        DELETE FROM public.fato_venda
        WHERE sk_tempo IN (
            SELECT sk_tempo FROM public.dim_tempo
            WHERE data = '{execution_date}'
        )
    """)
    append_to_table(url_rs, df_fato, "fato_venda", schema="public", chunksize=5000)

    log.info(f"fato_venda [{execution_date}]: {len(df_fato)} linhas inseridas")
    context['ti'].xcom_push(key='fato_count', value=len(df_fato))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3 — gravar_data_lake
# ══════════════════════════════════════════════════════════════════════════════
def gravar_data_lake(**context):
    import pyarrow as pa
    import pyarrow.parquet as pq

    execution_date = context['ds']
    exec_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    ano, mes = exec_dt.year, exec_dt.month

    url_rs = get_dw_url()

    df = read_sql_from_url(f"""
        SELECT dt.ano, dt.mes, dt.data,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real,
               dl.sigla_estado, dl.cidade
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
        LEFT JOIN public.dim_localidade dl ON dl.sk_localidade = fv.sk_localidade
        WHERE dt.ano = {ano} AND dt.mes = {mes}
    """, url_rs)

    if df.empty:
        log.info("Nenhum dado para gravar no lake.")
        context['ti'].xcom_push(key='parquet_count', value=0)
        return

    base = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        '..', os.environ.get("LOCAL_PARQUET_FOLDER", "lake/fato_venda"))
    particao_local = os.path.join(base, f"ano={ano}", f"mes={mes}")
    os.makedirs(particao_local, exist_ok=True)

    arquivo = os.path.join(particao_local, f"data_{execution_date}.parquet")
    df_sem_part = df.drop(columns=["ano", "mes"])
    table = pa.Table.from_pandas(df_sem_part, preserve_index=False)
    pq.write_table(table, arquivo, compression="snappy")
    log.info(f"Parquet local: {len(df)} linhas → {arquivo}")

    # Upload HDFS
    hdfs_url       = os.environ.get("HDFS_URL", "http://hadoop:9870")
    hdfs_user      = os.environ.get("HDFS_USER", "root")
    hdfs_dest_path = os.environ.get("HDFS_DEST_PATH", "/vendas/fato_vendas/")

    try:
        from hdfs import InsecureClient
        client = InsecureClient(hdfs_url, user=hdfs_user)
        dest_hdfs = f"{hdfs_dest_path.rstrip('/')}/ano={ano}/mes={mes}"
        if not client.status(dest_hdfs, strict=False):
            client.makedirs(dest_hdfs)
        with open(arquivo, "rb") as fdata:
            client.write(f"{dest_hdfs}/data_{execution_date}.parquet",
                         fdata, overwrite=True)
        log.info(f"Upload HDFS: {dest_hdfs}")
    except Exception as e:
        log.warning(f"HDFS indisponível — mantendo lake local: {e}")

    context['ti'].xcom_push(key='parquet_count', value=len(df))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 4 — popular_data_mart
# ══════════════════════════════════════════════════════════════════════════════
def popular_data_mart(**context):
    url_rs = get_dw_url()
    url_dm = get_dm_url()
    agora  = pd.Timestamp.now().floor("s")

    df_fato = read_sql_from_url("""
        SELECT dt.ano, dt.mes, fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
    """, url_rs)

    vam = (
        df_fato.groupby(["ano", "mes"], as_index=False)
        .agg(qtde_vendida=("quantidade", "sum"),
             valor_total_real=("valor_venda_real", "sum"),
             valor_total_esperado=("valor_unitario", "sum"))
    )
    vam["qtde_vendida"]          = vam["qtde_vendida"].astype(int)
    vam["valor_total_real"]      = vam["valor_total_real"].round(2)
    vam["valor_total_esperado"]  = vam["valor_total_esperado"].round(2)
    vam["data_atualizacao"]      = agora

    execute_query(url_dm, """
        CREATE TABLE IF NOT EXISTS public.vendas_ano_mes_jaime (
            id                   SERIAL PRIMARY KEY,
            ano                  INTEGER,
            mes                  INTEGER,
            qtde_vendida         INTEGER,
            valor_total_real     NUMERIC(18,2),
            valor_total_esperado NUMERIC(18,2),
            data_atualizacao     TIMESTAMP
        )
    """)
    _truncate_insert(url_dm, vam, "vendas_ano_mes_jaime", schema="public", chunksize=1000)
    log.info(f"vendas_ano_mes_jaime: {len(vam)} linhas | atualizado em {agora}")

    df_loc = read_sql_from_url("""
        SELECT dt.ano, dl.sigla_estado, dl.estado, dl.cidade,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt            ON dt.sk_tempo = fv.sk_tempo
        LEFT JOIN public.dim_localidade dl  ON dl.sk_localidade = fv.sk_localidade
        WHERE dl.sk_localidade IS NOT NULL
    """, url_rs)

    vloc = (
        df_loc.groupby(["ano", "sigla_estado", "estado", "cidade"], as_index=False)
        .agg(qtde_vendida=("quantidade", "sum"),
             valor_total_real=("valor_venda_real", "sum"),
             valor_total_esperado=("valor_unitario", "sum"))
    )
    vloc["qtde_vendida"]         = vloc["qtde_vendida"].astype(int)
    vloc["valor_total_real"]     = vloc["valor_total_real"].round(2)
    vloc["valor_total_esperado"] = vloc["valor_total_esperado"].round(2)
    vloc["pct_atingimento"]      = (
        vloc["valor_total_real"] / vloc["valor_total_esperado"] * 100
    ).round(2)
    vloc["data_atualizacao"]     = agora

    execute_query(url_dm, """
        CREATE TABLE IF NOT EXISTS public.vendas_localidade_jaime (
            id                   SERIAL PRIMARY KEY,
            ano                  INTEGER,
            sigla_estado         VARCHAR(2),
            estado               VARCHAR(100),
            cidade               VARCHAR(200),
            qtde_vendida         INTEGER,
            valor_total_real     NUMERIC(18,2),
            valor_total_esperado NUMERIC(18,2),
            pct_atingimento      NUMERIC(8,2),
            data_atualizacao     TIMESTAMP
        )
    """)
    _truncate_insert(url_dm, vloc, "vendas_localidade_jaime", schema="public", chunksize=1000)
    log.info(f"vendas_localidade_jaime: {len(vloc)} linhas | atualizado em {agora}")

    context['ti'].xcom_push(key='mart_counts', value={
        'vendas_ano_mes':    len(vam),
        'vendas_localidade': len(vloc),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TASK 5 — validar_pipeline
# ══════════════════════════════════════════════════════════════════════════════
def validar_pipeline(**context):
    ti = context['ti']

    contagens_dim = ti.xcom_pull(task_ids='extrair_banco_fonte', key='contagens_dim') or {}
    fato_count    = ti.xcom_pull(task_ids='carregar_redshift',   key='fato_count')   or 0
    parquet_count = ti.xcom_pull(task_ids='gravar_data_lake',    key='parquet_count') or 0
    mart_counts   = ti.xcom_pull(task_ids='popular_data_mart',   key='mart_counts')  or {}

    log.info("=" * 60)
    log.info("RELATÓRIO DE VALIDAÇÃO DO PIPELINE")
    log.info(f"  Execução: {context['ds']}")
    log.info("-" * 60)
    log.info("  DIMENSÕES (Redshift):")
    for dim, cnt in contagens_dim.items():
        log.info(f"    {dim}: {cnt} linhas")
    log.info(f"  fato_venda [{context['ds']}]: {fato_count} linhas")
    log.info(f"  Data Lake  [{context['ds']}]: {parquet_count} linhas")
    log.info("  DATA MART:")
    for mart, cnt in mart_counts.items():
        log.info(f"    {mart}: {cnt} linhas")
    log.info("=" * 60)

    assert contagens_dim.get('dim_cliente', 0) > 0,    "dim_cliente está vazia!"
    assert contagens_dim.get('dim_localidade', 0) > 0, "dim_localidade está vazia!"
    assert mart_counts.get('vendas_ano_mes', 0) > 0,   "vendas_ano_mes está vazia!"
    assert mart_counts.get('vendas_localidade', 0) > 0,"vendas_localidade está vazia!"

    log.info("PIPELINE VALIDADO COM SUCESSO ✓")


# ══════════════════════════════════════════════════════════════════════════════
# DAG
# ══════════════════════════════════════════════════════════════════════════════
default_args = {
    'owner':            'airflow',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_vendas_digital_corporativo',
    default_args=default_args,
    description=(
        'Pipeline ETL incremental de Vendas com Geolocalização | '
        'PostgreSQL → Redshift → Parquet/HDFS → PostgreSQL Data Mart'
    ),
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['vendas', 'etl', 'incremental', 'geolocalizacao'],
) as dag:

    t1 = PythonOperator(task_id='extrair_banco_fonte', python_callable=extrair_banco_fonte)
    t2 = PythonOperator(task_id='carregar_redshift',   python_callable=carregar_redshift)
    t3 = PythonOperator(task_id='gravar_data_lake',    python_callable=gravar_data_lake)
    t4 = PythonOperator(task_id='popular_data_mart',   python_callable=popular_data_mart)
    t5 = PythonOperator(task_id='validar_pipeline',    python_callable=validar_pipeline)

    t1 >> t2 >> t3 >> t4 >> t5
