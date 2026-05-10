"""
DAG: pipeline_vendas_digital_corporativo
Pipeline ETL de Vendas com Geolocalização — Airflow 3.x compatível

Fluxo:
    PostgreSQL (fonte) → Redshift (DW) → Parquet/HDFS (Lake) → PostgreSQL (Data Mart)

Estratégia de carga:
    - Dimensões : FULL (TRUNCATE + INSERT) — tabelas pequenas e estáveis
    - Fato       : INCREMENTAL por data de execução (DELETE dia + INSERT dia)
    - Data Lake  : INCREMENTAL — grava apenas a partição do dia (overwrite=True)
    - Data Mart  : FULL a partir do Redshift, com coluna data_atualizacao
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_source_engine():
    url = os.environ.get("SOURCE_DATABASE_URL")
    if not url:
        raise ValueError("Variável SOURCE_DATABASE_URL não encontrada! Verifique se o arquivo .env foi carregado.")
    return create_engine(url)

def get_dw_engine():
    url = os.environ.get("REDSHIFT_DATABASE_URL")
    if not url:
        raise ValueError("Variável REDSHIFT_DATABASE_URL não encontrada! Verifique se o arquivo .env foi carregado.")
    
    # Airflow muitas vezes não tem o plugin 'sqlalchemy-redshift' instalado por padrão.
    # Como o Redshift é baseado no PostgreSQL, podemos usar o driver nativo como fallback.
    if url.startswith("redshift+psycopg2://"):
        url = url.replace("redshift+psycopg2://", "postgresql://")
        
    return create_engine(url)

def get_dm_engine():
    url = os.environ.get("DASHBOARD_DATABASE_URL")
    if not url:
        raise ValueError("Variável DASHBOARD_DATABASE_URL não encontrada! Verifique se o arquivo .env foi carregado.")
    return create_engine(url)

def add_sk(df, col_name="sk"):
    """Adiciona surrogate key sequencial como primeira coluna."""
    df = df.copy().reset_index(drop=True)
    df.insert(0, col_name, range(1, len(df) + 1))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1 — extrair_banco_fonte
# Carrega TODAS as dimensões no Redshift (carga FULL, tabelas pequenas)
# Inclui PF e PJ na dim_cliente
# ══════════════════════════════════════════════════════════════════════════════
def extrair_banco_fonte(**context):
    """Extrai e recarrega todas as dimensões do banco fonte no Redshift."""
    engine_src = get_source_engine()
    engine_rs  = get_dw_engine()

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
        lambda df: _truncate_insert(engine_rs, df, "dim_tempo")
    )
    log.info(f"dim_tempo: {len(dim_tempo)} linhas")

    # ── DIM PRODUTO ────────────────────────────────────────────────────────────
    df_produto = pd.read_sql("""
        SELECT p.id AS id_produto, p.nome AS nome,
               cat.descricao AS categoria, p.valor_venda AS preco_tabela
        FROM vendas.produto p
        JOIN vendas.categoria cat ON cat.id = p.id_categoria
    """, engine_src)
    add_sk(df_produto, "sk_produto").pipe(
        lambda df: _truncate_insert(engine_rs, df, "dim_produto")
    )
    log.info(f"dim_produto: {len(df_produto)} linhas")

    # ── DIM CLIENTE (PF + PJ) ─────────────────────────────────────────────────
    # FIX: inclui Pessoa Jurídica via UNION com COALESCE para identificador único
    df_cliente = pd.read_sql("""
        SELECT DISTINCT
            p.id                                        AS id_pessoa,
            COALESCE(pf.nome, pj.razao_social)          AS nome,
            COALESCE(pf.cpf,  pj.cnpj)                  AS documento,
            CASE WHEN pf.id IS NOT NULL THEN 'PF' ELSE 'PJ' END AS tipo_pessoa
        FROM geral.pessoa p
        LEFT JOIN geral.pessoa_fisica   pf ON pf.id = p.id
        LEFT JOIN geral.pessoa_juridica pj ON pj.id = p.id
        WHERE p.id IN (SELECT id_cliente FROM vendas.nota_fiscal)
          AND (pf.id IS NOT NULL OR pj.id IS NOT NULL)
    """, engine_src)
    add_sk(df_cliente, "sk_cliente").pipe(
        lambda df: _truncate_insert(engine_rs, df, "dim_cliente")
    )
    log.info(f"dim_cliente: {len(df_cliente)} linhas (PF+PJ)")

    # ── DIM VENDEDOR ───────────────────────────────────────────────────────────
    df_vendedor = pd.read_sql("""
        SELECT DISTINCT pf.id AS id_pessoa, pf.nome AS nome
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_vendedor
    """, engine_src)
    add_sk(df_vendedor, "sk_vendedor").pipe(
        lambda df: _truncate_insert(engine_rs, df, "dim_vendedor")
    )
    log.info(f"dim_vendedor: {len(df_vendedor)} linhas")

    # ── DIM LOCALIDADE ─────────────────────────────────────────────────────────
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
        LEFT JOIN geral.bairro   b   ON b.id = e.id_bairro
        LEFT JOIN geral.cidade   c   ON c.id = b.id_cidade
        LEFT JOIN geral.estado   est ON est.id = c.id_estado
        WHERE e.id IS NOT NULL
    """, engine_src)
    add_sk(df_localidade, "sk_localidade").pipe(
        lambda df: _truncate_insert(engine_rs, df, "dim_localidade")
    )
    log.info(f"dim_localidade: {len(df_localidade)} linhas")

    context['ti'].xcom_push(key='contagens_dim', value={
        'dim_tempo':       len(dim_tempo),
        'dim_produto':     len(df_produto),
        'dim_cliente':     len(df_cliente),
        'dim_vendedor':    len(df_vendedor),
        'dim_localidade':  len(df_localidade),
    })


def _truncate_insert(engine, df, table, schema="public", chunksize=2000):
    """TRUNCATE + INSERT idempotente numa única transação."""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
    df.to_sql(table, engine, schema=schema,
              if_exists="append", index=False, method="multi", chunksize=chunksize)


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2 — carregar_redshift
# INCREMENTAL: apaga somente o dia de execução e reinsere (idempotente)
# ══════════════════════════════════════════════════════════════════════════════
def carregar_redshift(**context):
    """
    Carga incremental da fato_venda no Redshift.
    Apaga apenas os registros do dia de execução e reinsere,
    garantindo idempotência sem precisar fazer TRUNCATE total.
    """
    execution_date = context['ds']          # formato 'YYYY-MM-DD'
    engine_src = get_source_engine()
    engine_rs  = get_dw_engine()

    log.info(f"Carregando fato para data de execução: {execution_date}")

    # Extrai apenas vendas do dia de execução
    df_raw = pd.read_sql(f"""
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
    """, engine_src)

    if df_raw.empty:
        log.info(f"Nenhuma venda encontrada para {execution_date}. Encerrando task.")
        context['ti'].xcom_push(key='fato_count', value=0)
        return

    df_fato = df_raw.copy()
    df_fato["data_venda"] = pd.to_datetime(df_fato["data_venda"]).dt.date

    # Lê SKs das dimensões
    dim_tempo      = pd.read_sql("SELECT sk_tempo, data FROM public.dim_tempo", engine_rs)
    dim_produto    = pd.read_sql("SELECT sk_produto, id_produto FROM public.dim_produto", engine_rs)
    dim_cliente    = pd.read_sql("SELECT sk_cliente, id_pessoa FROM public.dim_cliente", engine_rs)
    dim_vendedor   = pd.read_sql("SELECT sk_vendedor, id_pessoa FROM public.dim_vendedor", engine_rs)
    dim_localidade = pd.read_sql("SELECT sk_localidade, id_pessoa FROM public.dim_localidade", engine_rs)

    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"]).dt.date

    # Merges
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
    with engine_rs.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM public.fato_venda
            WHERE sk_tempo IN (
                SELECT sk_tempo FROM public.dim_tempo
                WHERE data = '{execution_date}'
            )
        """))
    df_fato.to_sql("fato_venda", engine_rs, schema="public",
                   if_exists="append", index=False, method="multi", chunksize=5000)

    log.info(f"fato_venda [{execution_date}]: {len(df_fato)} linhas inseridas")
    context['ti'].xcom_push(key='fato_count', value=len(df_fato))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3 — gravar_data_lake
# INCREMENTAL: grava apenas a partição ano/mês do dia de execução
# ══════════════════════════════════════════════════════════════════════════════
def gravar_data_lake(**context):
    """
    Exporta os dados do dia de execução para Parquet particionado (incremental).
    Sobrescreve apenas o arquivo da partição correspondente ao dia.
    Tenta upload para HDFS; se indisponível, mantém lake local.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    execution_date = context['ds']
    exec_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    ano, mes = exec_dt.year, exec_dt.month

    engine_rs = get_dw_engine()

    df = pd.read_sql(f"""
        SELECT dt.ano, dt.mes, dt.data,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real,
               dl.sigla_estado, dl.cidade
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
        LEFT JOIN public.dim_localidade dl ON dl.sk_localidade = fv.sk_localidade
        WHERE dt.ano = {ano} AND dt.mes = {mes}
    """, engine_rs)

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

    # Upload HDFS (opcional)
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
# FULL: recalcula agregações completas + coluna data_atualizacao
# ══════════════════════════════════════════════════════════════════════════════
def popular_data_mart(**context):
    """
    Recalcula as agregações completas e atualiza o Data Mart.
    FIX: adiciona coluna data_atualizacao em ambas as tabelas.
    """
    engine_rs = get_dw_engine()
    engine_dm = get_dm_engine()
    agora     = pd.Timestamp.now().floor("s")   # timestamp do momento do ETL

    # ── vendas_ano_mes ─────────────────────────────────────────────────────────
    df_fato = pd.read_sql("""
        SELECT dt.ano, dt.mes, fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
    """, engine_rs)

    vam = (
        df_fato.groupby(["ano", "mes"], as_index=False)
        .agg(qtde_vendida=("quantidade", "sum"),
             valor_total_real=("valor_venda_real", "sum"),
             valor_total_esperado=("valor_unitario", "sum"))
    )
    vam["qtde_vendida"]          = vam["qtde_vendida"].astype(int)
    vam["valor_total_real"]      = vam["valor_total_real"].round(2)
    vam["valor_total_esperado"]  = vam["valor_total_esperado"].round(2)
    vam["data_atualizacao"]      = agora          # FIX: registro do momento

    with engine_dm.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.vendas_ano_mes_jaime (
                id                   SERIAL PRIMARY KEY,
                ano                  INTEGER,
                mes                  INTEGER,
                qtde_vendida         INTEGER,
                valor_total_real     NUMERIC(18,2),
                valor_total_esperado NUMERIC(18,2),
                data_atualizacao     TIMESTAMP
            )
        """))
        conn.execute(text("TRUNCATE TABLE public.vendas_ano_mes_jaime"))
    vam.to_sql("vendas_ano_mes_jaime", engine_dm, schema="public",
               if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"vendas_ano_mes_jaime: {len(vam)} linhas | atualizado em {agora}")

    # ── vendas_localidade ──────────────────────────────────────────────────────
    df_loc = pd.read_sql("""
        SELECT dt.ano, dl.sigla_estado, dl.estado, dl.cidade,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt            ON dt.sk_tempo = fv.sk_tempo
        LEFT JOIN public.dim_localidade dl  ON dl.sk_localidade = fv.sk_localidade
        WHERE dl.sk_localidade IS NOT NULL
    """, engine_rs)

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
    vloc["data_atualizacao"]     = agora          # FIX: registro do momento

    with engine_dm.begin() as conn:
        conn.execute(text("""
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
        """))
        conn.execute(text("TRUNCATE TABLE public.vendas_localidade_jaime"))
    vloc.to_sql("vendas_localidade_jaime", engine_dm, schema="public",
                if_exists="append", index=False, method="multi", chunksize=1000)
    log.info(f"vendas_localidade_jaime: {len(vloc)} linhas | atualizado em {agora}")

    context['ti'].xcom_push(key='mart_counts', value={
        'vendas_ano_mes':    len(vam),
        'vendas_localidade': len(vloc),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TASK 5 — validar_pipeline
# ══════════════════════════════════════════════════════════════════════════════
def validar_pipeline(**context):
    """Valida contagens via XCom e assegura que nenhuma tabela ficou vazia."""
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

    # Validações
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
