"""
DAG: pipeline_vendas_digital_corporativo
Pipeline ETL de Vendas com Geolocalização
PostgreSQL → Redshift → Parquet/HDFS → PostgreSQL Data Mart
"""

import os
import shutil
import logging
from io import BytesIO
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
from sqlalchemy import create_engine, text
from hdfs import InsecureClient
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# ── Configuração ──────────────────────────────────────────────────────────────
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path)

log = logging.getLogger(__name__)


def get_source_engine():
    return create_engine(os.environ.get("SOURCE_DATABASE_URL"))

def get_dw_engine():
    return create_engine(os.environ.get("REDSHIFT_DATABASE_URL"))

def get_dm_engine():
    return create_engine(os.environ.get("DASHBOARD_DATABASE_URL"))

def add_sk(df, col_name="sk"):
    df.insert(0, col_name, range(1, len(df) + 1))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1: extrair_banco_fonte
# ══════════════════════════════════════════════════════════════════════════════
def extrair_banco_fonte(**context):
    """Extrai dimensões do banco fonte PostgreSQL e carrega no Redshift."""
    engine_source = get_source_engine()
    engine_rs = get_dw_engine()

    # --- DIM TEMPO ---
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
    add_sk(dim_tempo, "sk_tempo")
    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_tempo"))
    dim_tempo.to_sql("dim_tempo", engine_rs, schema="public",
                     if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"dim_tempo: {len(dim_tempo)} linhas")

    # --- DIM PRODUTO ---
    df_produto = pd.read_sql("""
        SELECT p.id AS id_produto, p.nome AS nome,
               cat.descricao AS categoria, p.valor_venda AS preco_tabela
        FROM vendas.produto p
        JOIN vendas.categoria cat ON cat.id = p.id_categoria
    """, engine_source)
    dim_produto = add_sk(df_produto.copy(), "sk_produto")
    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_produto"))
    dim_produto.to_sql("dim_produto", engine_rs, schema="public",
                       if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"dim_produto: {len(dim_produto)} linhas")

    # --- DIM CLIENTE ---
    df_cliente = pd.read_sql("""
        SELECT DISTINCT pf.id AS id_pessoa, pf.nome AS nome, pf.cpf
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_cliente
    """, engine_source)
    dim_cliente = add_sk(df_cliente.copy(), "sk_cliente")
    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_cliente"))
    dim_cliente.to_sql("dim_cliente", engine_rs, schema="public",
                       if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"dim_cliente: {len(dim_cliente)} linhas")

    # --- DIM VENDEDOR ---
    df_vendedor = pd.read_sql("""
        SELECT DISTINCT pf.id AS id_pessoa, pf.nome AS nome
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_vendedor
    """, engine_source)
    dim_vendedor = add_sk(df_vendedor.copy(), "sk_vendedor")
    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_vendedor"))
    dim_vendedor.to_sql("dim_vendedor", engine_rs, schema="public",
                        if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"dim_vendedor: {len(dim_vendedor)} linhas")

    # --- DIM LOCALIDADE ---
    df_localidade = pd.read_sql("""
        SELECT DISTINCT
            p.id          AS id_pessoa,
            b.descricao   AS bairro,
            c.descricao   AS cidade,
            est.sigla     AS sigla_estado,
            est.descricao AS estado,
            e.cep
        FROM geral.pessoa p
        LEFT JOIN geral.endereco e   ON e.id_pessoa = p.id
        LEFT JOIN geral.bairro b     ON b.id = e.id_bairro
        LEFT JOIN geral.cidade c     ON c.id = b.id_cidade
        LEFT JOIN geral.estado est   ON est.id = c.id_estado
        WHERE e.id IS NOT NULL
    """, engine_source)
    dim_localidade = add_sk(df_localidade.copy(), "sk_localidade")
    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_localidade"))
    dim_localidade.to_sql("dim_localidade", engine_rs, schema="public",
                          if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"dim_localidade: {len(dim_localidade)} linhas")

    # Passar contagens via XCom
    context['ti'].xcom_push(key='contagens_dim', value={
        'dim_tempo': len(dim_tempo),
        'dim_produto': len(dim_produto),
        'dim_cliente': len(dim_cliente),
        'dim_vendedor': len(dim_vendedor),
        'dim_localidade': len(dim_localidade),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2: carregar_redshift
# ══════════════════════════════════════════════════════════════════════════════
def carregar_redshift(**context):
    """Extrai itens de nota fiscal e carrega fato_venda no Redshift."""
    engine_source = get_source_engine()
    engine_rs = get_dw_engine()

    df_fato_raw = pd.read_sql("""
        SELECT
            nf.data_venda::date AS data_venda,
            nf.id_cliente, nf.id_vendedor,
            inf.id_produto,
            nf.id AS id_nota,
            inf.quantidade, inf.valor_unitario, inf.valor_venda_real,
            p.valor_venda
        FROM vendas.nota_fiscal nf
        JOIN vendas.item_nota_fiscal inf ON inf.id_nota_fiscal = nf.id
        JOIN vendas.produto p            ON p.id = inf.id_produto
    """, engine_source)

    df_fato = df_fato_raw.copy()
    df_fato["data_venda"] = pd.to_datetime(df_fato["data_venda"]).dt.date

    # Ler dimensões do DW para pegar SKs
    dim_tempo = pd.read_sql("SELECT sk_tempo, data FROM public.dim_tempo", engine_rs)
    dim_produto = pd.read_sql("SELECT sk_produto, id_produto FROM public.dim_produto", engine_rs)
    dim_cliente = pd.read_sql("SELECT sk_cliente, id_pessoa FROM public.dim_cliente", engine_rs)
    dim_vendedor = pd.read_sql("SELECT sk_vendedor, id_pessoa FROM public.dim_vendedor", engine_rs)
    dim_localidade = pd.read_sql("SELECT sk_localidade, id_pessoa FROM public.dim_localidade", engine_rs)

    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"]).dt.date

    # Merges
    df_fato = df_fato.merge(dim_tempo.rename(columns={"data": "data_venda"}), on="data_venda", how="left")
    df_fato = df_fato.merge(dim_produto, on="id_produto", how="left")
    df_fato = df_fato.merge(dim_cliente.rename(columns={"id_pessoa": "id_cliente"}), on="id_cliente", how="left")
    df_fato = df_fato.merge(dim_vendedor.rename(columns={"id_pessoa": "id_vendedor"}), on="id_vendedor", how="left")
    df_fato = df_fato.merge(dim_localidade.rename(columns={"id_pessoa": "id_cliente"}), on="id_cliente", how="left")

    df_fato["desconto"] = (df_fato["valor_venda"] - df_fato["valor_unitario"]).round(2)
    df_fato["pct_desconto"] = (df_fato["desconto"] / df_fato["valor_unitario"] * 100).round(2)

    colunas_fato = [
        "sk_tempo", "sk_produto", "sk_cliente", "sk_vendedor", "sk_localidade",
        "id_nota", "quantidade", "valor_unitario", "valor_venda_real",
        "desconto", "pct_desconto",
    ]
    df_fato = df_fato[colunas_fato]

    # Registrar nulos
    nulos = df_fato[["sk_tempo", "sk_produto", "sk_cliente", "sk_vendedor", "sk_localidade"]].isnull().sum()
    log.info(f"Nulos nas SKs:\n{nulos}")

    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.fato_venda"))
    df_fato.to_sql("fato_venda", engine_rs, schema="public",
                   if_exists="append", index=False, method="multi", chunksize=10000)
    log.info(f"fato_venda: {len(df_fato)} linhas")

    context['ti'].xcom_push(key='fato_count', value=len(df_fato))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3: gravar_data_lake
# ══════════════════════════════════════════════════════════════════════════════
def gravar_data_lake(**context):
    """Exporta fato para Parquet particionado e faz upload para HDFS."""
    engine_rs = get_dw_engine()
    local_parquet_folder = os.environ.get("LOCAL_PARQUET_FOLDER", "lake/fato_venda")
    lake_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', local_parquet_folder)

    query = """
    SELECT ano, mes, fv.quantidade, fv.valor_unitario, fv.valor_venda_real,
           dl.sigla_estado, dl.cidade
    FROM public.fato_venda fv
    JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
    LEFT JOIN public.dim_localidade dl ON dl.sk_localidade = fv.sk_localidade
    """
    df_fato = pd.read_sql(query, engine_rs)

    if os.path.exists(lake_path):
        shutil.rmtree(lake_path)

    table = pa.Table.from_pandas(df_fato, preserve_index=False)
    pq.write_to_dataset(table, root_path=lake_path,
                        partition_cols=["ano", "mes"], compression="snappy")
    log.info(f"Parquet local: {len(df_fato)} linhas em {lake_path}")

    # Upload HDFS
    hdfs_url = os.environ.get("HDFS_URL", "http://hadoop:9870")
    hdfs_user = os.environ.get("HDFS_USER", "root")
    hdfs_dest_path = os.environ.get("HDFS_DEST_PATH", "/vendas/fato_vendas/")

    try:
        client = InsecureClient(hdfs_url, user=hdfs_user)
        for root, _, files in os.walk(lake_path):
            parquet_files = [f for f in files if f.endswith(".parquet")]
            if not parquet_files:
                continue
            relative_path = os.path.relpath(root, lake_path).replace("\\", "/")
            dest = hdfs_dest_path.rstrip("/")
            if relative_path != ".":
                dest = f"{dest}/{relative_path}"
            if not client.status(dest, strict=False):
                client.makedirs(dest)
            for pf in parquet_files:
                with open(os.path.join(root, pf), "rb") as fdata:
                    client.write(f"{dest}/{pf}", fdata, overwrite=True)
        log.info(f"Upload HDFS concluído: {hdfs_dest_path}")
    except Exception as e:
        log.warning(f"HDFS indisponível, continuando com lake local: {e}")

    context['ti'].xcom_push(key='parquet_count', value=len(df_fato))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 4: popular_data_mart
# ══════════════════════════════════════════════════════════════════════════════
def popular_data_mart(**context):
    """Agrega vendas e carrega vendas_ano_mes + vendas_localidade no Data Mart."""
    engine_rs = get_dw_engine()
    engine_dm = get_dm_engine()

    # --- vendas_ano_mes ---
    df_fato = pd.read_sql("""
        SELECT dt.ano, dt.mes, fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
    """, engine_rs)

    vendas_ano_mes = (
        df_fato.groupby(["ano", "mes"], as_index=False)
        .agg(qtde_vendida=("quantidade", "sum"),
             valor_total_real=("valor_venda_real", "sum"),
             valor_total_esperado=("valor_unitario", "sum"))
    )
    vendas_ano_mes["qtde_vendida"] = vendas_ano_mes["qtde_vendida"].astype(int)
    vendas_ano_mes["valor_total_real"] = vendas_ano_mes["valor_total_real"].round(2)
    vendas_ano_mes["valor_total_esperado"] = vendas_ano_mes["valor_total_esperado"].round(2)

    with engine_dm.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.vendas_ano_mes_jaime"))
    vendas_ano_mes.to_sql("vendas_ano_mes_jaime", engine_dm, schema="public",
                          if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"vendas_ano_mes_jaime: {len(vendas_ano_mes)} linhas")

    # --- vendas_localidade ---
    df_loc = pd.read_sql("""
        SELECT dt.ano, dl.sigla_estado, dl.estado, dl.cidade,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt       ON dt.sk_tempo = fv.sk_tempo
        LEFT JOIN public.dim_localidade dl ON dl.sk_localidade = fv.sk_localidade
        WHERE dl.sk_localidade IS NOT NULL
    """, engine_rs)

    vendas_loc = (
        df_loc.groupby(["ano", "sigla_estado", "estado", "cidade"], as_index=False)
        .agg(qtde_vendida=("quantidade", "sum"),
             valor_total_real=("valor_venda_real", "sum"),
             valor_total_esperado=("valor_unitario", "sum"))
    )
    vendas_loc["qtde_vendida"] = vendas_loc["qtde_vendida"].astype(int)
    vendas_loc["valor_total_real"] = vendas_loc["valor_total_real"].round(2)
    vendas_loc["valor_total_esperado"] = vendas_loc["valor_total_esperado"].round(2)
    vendas_loc["pct_atingimento"] = (
        vendas_loc["valor_total_real"] / vendas_loc["valor_total_esperado"] * 100
    ).round(2)

    with engine_dm.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.vendas_localidade_jaime (
                id SERIAL PRIMARY KEY,
                ano INTEGER,
                sigla_estado VARCHAR(2),
                estado VARCHAR(100),
                cidade VARCHAR(200),
                qtde_vendida INTEGER,
                valor_total_real NUMERIC(18,2),
                valor_total_esperado NUMERIC(18,2),
                pct_atingimento NUMERIC(8,2)
            )
        """))
        conn.execute(text("TRUNCATE TABLE public.vendas_localidade_jaime"))
    vendas_loc.to_sql("vendas_localidade_jaime", engine_dm, schema="public",
                      if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"vendas_localidade_jaime: {len(vendas_loc)} linhas")

    context['ti'].xcom_push(key='mart_counts', value={
        'vendas_ano_mes': len(vendas_ano_mes),
        'vendas_localidade': len(vendas_loc),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TASK 5: validar_pipeline
# ══════════════════════════════════════════════════════════════════════════════
def validar_pipeline(**context):
    """Valida contagens e integridade do pipeline."""
    ti = context['ti']

    contagens_dim = ti.xcom_pull(task_ids='extrair_banco_fonte', key='contagens_dim')
    fato_count = ti.xcom_pull(task_ids='carregar_redshift', key='fato_count')
    parquet_count = ti.xcom_pull(task_ids='gravar_data_lake', key='parquet_count')
    mart_counts = ti.xcom_pull(task_ids='popular_data_mart', key='mart_counts')

    log.info("=" * 60)
    log.info("VALIDAÇÃO DO PIPELINE")
    log.info("=" * 60)

    if contagens_dim:
        for dim, count in contagens_dim.items():
            log.info(f"  {dim}: {count} linhas")

    log.info(f"  fato_venda: {fato_count} linhas")
    log.info(f"  parquet exportado: {parquet_count} linhas")

    if mart_counts:
        for mart, count in mart_counts.items():
            log.info(f"  {mart}: {count} linhas")

    # Validações básicas
    assert fato_count and fato_count > 0, "fato_venda está vazia!"
    assert mart_counts and mart_counts.get('vendas_ano_mes', 0) > 0, "vendas_ano_mes está vazia!"
    assert mart_counts and mart_counts.get('vendas_localidade', 0) > 0, "vendas_localidade está vazia!"

    log.info("=" * 60)
    log.info("PIPELINE VALIDADO COM SUCESSO")
    log.info("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_vendas_digital_corporativo',
    default_args=default_args,
    description='Pipeline ETL de Vendas com Geolocalização '
                '(PostgreSQL → Redshift → Parquet/HDFS → PostgreSQL Data Mart)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['vendas', 'etl', 'idempotente', 'geolocalizacao'],
) as dag:

    t1 = PythonOperator(
        task_id='extrair_banco_fonte',
        python_callable=extrair_banco_fonte,
    )

    t2 = PythonOperator(
        task_id='carregar_redshift',
        python_callable=carregar_redshift,
    )

    t3 = PythonOperator(
        task_id='gravar_data_lake',
        python_callable=gravar_data_lake,
    )

    t4 = PythonOperator(
        task_id='popular_data_mart',
        python_callable=popular_data_mart,
    )

    t5 = PythonOperator(
        task_id='validar_pipeline',
        python_callable=validar_pipeline,
    )

    t1 >> t2 >> t3 >> t4 >> t5
