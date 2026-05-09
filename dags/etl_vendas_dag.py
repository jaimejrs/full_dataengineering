import os
import shutil
from io import BytesIO
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine, text
from hdfs import InsecureClient
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Configuração de variáveis de ambiente
# A DAG estará em `dags/`, então o `.env` estará na pasta pai `../.env`
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path)

def get_source_engine():
    return create_engine(os.environ.get("SOURCE_DATABASE_URL"))

def get_dw_engine():
    return create_engine(os.environ.get("REDSHIFT_DATABASE_URL"))

def get_dm_engine():
    return create_engine(os.environ.get("DASHBOARD_DATABASE_URL"))

def add_sk(df, col_name="sk"):
    df.insert(0, col_name, range(1, len(df) + 1))
    return df

def extract_load_dimensions():
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
    dim_tempo.to_sql("dim_tempo", engine_rs, schema="public", if_exists="append", index=False, method="multi", chunksize=1000)

    # --- DIM PRODUTO ---
    df_produto = pd.read_sql("""
        SELECT
            p.id          AS id_produto,
            p.nome        AS nome,
            cat.descricao AS categoria,
            p.valor_venda AS preco_tabela
        FROM vendas.produto p
        JOIN vendas.categoria cat ON cat.id = p.id_categoria
    """, engine_source)
    dim_produto = add_sk(df_produto.copy(), "sk_produto")

    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_produto"))
    dim_produto.to_sql("dim_produto", engine_rs, schema="public", if_exists="append", index=False, method="multi", chunksize=1000)

    # --- DIM CLIENTE ---
    df_cliente = pd.read_sql("""
        SELECT DISTINCT
            pf.id       AS id_pessoa,
            pf.nome     AS nome,
            pf.cpf
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_cliente
    """, engine_source)
    dim_cliente = add_sk(df_cliente.copy(), "sk_cliente")

    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_cliente"))
    dim_cliente.to_sql("dim_cliente", engine_rs, schema="public", if_exists="append", index=False, method="multi", chunksize=1000)

    # --- DIM VENDEDOR ---
    df_vendedor = pd.read_sql("""
        SELECT DISTINCT
            pf.id   AS id_pessoa,
            pf.nome AS nome
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_vendedor
    """, engine_source)
    dim_vendedor = add_sk(df_vendedor.copy(), "sk_vendedor")

    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.dim_vendedor"))
    dim_vendedor.to_sql("dim_vendedor", engine_rs, schema="public", if_exists="append", index=False, method="multi", chunksize=1000)

def extract_load_fact():
    engine_source = get_source_engine()
    engine_rs = get_dw_engine()
    
    df_fato_raw = pd.read_sql("""
        SELECT
            nf.data_venda::date        AS data_venda,
            nf.id_cliente,
            nf.id_vendedor,
            inf.id_produto,
            nf.id                      AS id_nota,
            inf.quantidade,
            inf.valor_unitario,
            inf.valor_venda_real,
            p.valor_venda                  
        FROM vendas.nota_fiscal nf
        JOIN vendas.item_nota_fiscal inf ON inf.id_nota_fiscal = nf.id
        JOIN vendas.produto p            ON p.id = inf.id_produto
    """, engine_source)

    df_fato = df_fato_raw.copy()
    df_fato["data_venda"] = pd.to_datetime(df_fato["data_venda"]).dt.date

    # Lendo dimensões do DW para pegar as SKs
    dim_tempo = pd.read_sql("SELECT sk_tempo, data FROM public.dim_tempo", engine_rs)
    dim_produto = pd.read_sql("SELECT sk_produto, id_produto FROM public.dim_produto", engine_rs)
    dim_cliente = pd.read_sql("SELECT sk_cliente, id_pessoa FROM public.dim_cliente", engine_rs)
    dim_vendedor = pd.read_sql("SELECT sk_vendedor, id_pessoa FROM public.dim_vendedor", engine_rs)
    
    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"]).dt.date

    df_fato = df_fato.merge(dim_tempo.rename(columns={"data": "data_venda"}), on="data_venda", how="left")
    df_fato = df_fato.merge(dim_produto, on="id_produto", how="left")
    df_fato = df_fato.merge(dim_cliente.rename(columns={"id_pessoa": "id_cliente"}), on="id_cliente", how="left")
    df_fato = df_fato.merge(dim_vendedor.rename(columns={"id_pessoa": "id_vendedor"}), on="id_vendedor", how="left")

    df_fato["desconto"] = (df_fato["valor_venda"] - df_fato["valor_unitario"]).round(2)
    df_fato["pct_desconto"] = (df_fato["desconto"] / df_fato["valor_unitario"] * 100).round(2)

    colunas_fato = [
        "sk_tempo", "sk_produto", "sk_cliente", "sk_vendedor",
        "id_nota",  "quantidade", "valor_unitario", "valor_venda_real",
        "desconto", "pct_desconto",
    ]
    df_fato = df_fato[colunas_fato]

    with engine_rs.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.fato_venda"))

    df_fato.to_sql("fato_venda", engine_rs, schema="public", if_exists="append", index=False, method="multi", chunksize=10000)

def export_to_parquet():
    engine_rs = get_dw_engine()
    local_parquet_folder = os.environ.get("LOCAL_PARQUET_FOLDER", "lake/fato_venda")
    
    # O caminho do lake local deve ser relativo à raiz do projeto
    lake_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', local_parquet_folder)
    
    query = """
    SELECT ano, mes, fv.quantidade, fv.valor_unitario, fv.valor_venda_real
    FROM public.fato_venda fv
    JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
    """
    
    df_fato = pd.read_sql(query, engine_rs)
    
    if os.path.exists(lake_path):
        shutil.rmtree(lake_path)
        
    table = pa.Table.from_pandas(df_fato, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=lake_path,
        partition_cols=["ano", "mes"],
        compression="snappy"
    )

def upload_to_hdfs_helper(local_folder, hdfs_url, hdfs_user, hdfs_dest_path):
    client = InsecureClient(hdfs_url, user=hdfs_user)
    if not client.status(hdfs_dest_path, strict=False):
        client.makedirs(hdfs_dest_path)
    
    for file_name in os.listdir(local_folder):
        file_path = os.path.join(local_folder, file_name)
        if os.path.isfile(file_path):
            hdfs_file_path = f"{hdfs_dest_path}/{file_name}"
            with open(file_path, "rb") as file_data:
                client.write(hdfs_file_path, file_data, overwrite=True)

def upload_to_hdfs():
    local_parquet_folder = os.environ.get("LOCAL_PARQUET_FOLDER", "lake/fato_venda")
    lake_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', local_parquet_folder)
    
    hdfs_url = os.environ.get("HDFS_URL", "http://hadoop:9870")
    hdfs_user = os.environ.get("HDFS_USER", "root")
    hdfs_dest_path = os.environ.get("HDFS_DEST_PATH", "/vendas/fato_vendas/")
    
    for root, _, files in os.walk(lake_path):
        parquet_files = [file for file in files if file.endswith(".parquet")]
        if not parquet_files:
            continue
            
        relative_path = os.path.relpath(root, lake_path).replace("\\", "/")
        hdfs_partition_path = hdfs_dest_path.rstrip("/")
        if relative_path != ".":
            hdfs_partition_path = f"{hdfs_partition_path}/{relative_path}"
            
        upload_to_hdfs_helper(
            local_folder=root,
            hdfs_url=hdfs_url,
            hdfs_user=hdfs_user,
            hdfs_dest_path=hdfs_partition_path
        )

def aggregate_and_load_dashboard():
    engine_dm = get_dm_engine()
    
    hdfs_url = os.environ.get("HDFS_URL", "http://hadoop:9870")
    hdfs_user = os.environ.get("HDFS_USER", "root")
    hdfs_dest_path = os.environ.get("HDFS_DEST_PATH", "/vendas/fato_vendas/")
    anos_str = os.environ.get("AGGREGATION_YEARS", "2024,2025,2026")
    anos = [int(ano.strip()) for ano in anos_str.split(",")]
    
    client = InsecureClient(hdfs_url, user=hdfs_user)
    dfs = []
    
    for ano in anos:
        ano_path = f"{hdfs_dest_path.rstrip('/')}/ano={ano}"
        if not client.status(ano_path, strict=False):
            continue
            
        meses = client.list(ano_path, status=True)
        for mes_nome, mes_status in meses:
            if mes_status["type"] != "DIRECTORY" or not mes_nome.startswith("mes="):
                continue
                
            mes_path = f"{ano_path}/{mes_nome}"
            arquivos = client.list(mes_path, status=True)
            
            for arquivo_nome, arquivo_status in arquivos:
                if arquivo_status["type"] != "FILE" or not arquivo_nome.endswith(".parquet"):
                    continue
                    
                parquet_path = f"{mes_path}/{arquivo_nome}"
                with client.read(parquet_path) as reader:
                    parquet_bytes = BytesIO(reader.read())
                    
                df_part = pq.read_table(parquet_bytes).to_pandas()
                df_part["ano"] = ano
                df_part["mes"] = int(mes_nome.split("=")[1])
                dfs.append(df_part)
                
    if not dfs:
        raise ValueError("Nenhum arquivo Parquet encontrado no HDFS para os anos informados.")
        
    df_vendas_hdfs = pd.concat(dfs, ignore_index=True)
    vendas_ano_mes = (
        df_vendas_hdfs
        .groupby(["ano", "mes"], as_index=False)
        .agg(
            qtde_vendida=("quantidade", "sum"),
            valor_total_real=("valor_venda_real", "sum"),
            valor_total_esperado=("valor_unitario", "sum"),
        )
    )
    
    vendas_ano_mes["qtde_vendida"] = vendas_ano_mes["qtde_vendida"].astype(int)
    vendas_ano_mes["valor_total_real"] = vendas_ano_mes["valor_total_real"].round(2)
    vendas_ano_mes["valor_total_esperado"] = vendas_ano_mes["valor_total_esperado"].round(2)
    
    with engine_dm.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.vendas_ano_mes_jaime"))
        
    vendas_ano_mes.to_sql(
        "vendas_ano_mes_jaime",
        engine_dm,
        schema="public",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )


# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_vendas_dag',
    default_args=default_args,
    description='Pipeline ETL de Vendas (PostgreSQL -> Redshift -> Parquet/HDFS -> PostgreSQL Painel)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['vendas', 'etl', 'idempotente'],
) as dag:

    t1_extract_load_dimensions = PythonOperator(
        task_id='extract_load_dimensions',
        python_callable=extract_load_dimensions,
    )

    t2_extract_load_fact = PythonOperator(
        task_id='extract_load_fact',
        python_callable=extract_load_fact,
    )

    t3_export_to_parquet = PythonOperator(
        task_id='export_to_parquet',
        python_callable=export_to_parquet,
    )

    t4_upload_to_hdfs = PythonOperator(
        task_id='upload_to_hdfs',
        python_callable=upload_to_hdfs,
    )

    t5_aggregate_and_load_dashboard = PythonOperator(
        task_id='aggregate_and_load_dashboard',
        python_callable=aggregate_and_load_dashboard,
    )

    # Definindo a ordem de execução
    t1_extract_load_dimensions >> t2_extract_load_fact >> t3_export_to_parquet >> t4_upload_to_hdfs >> t5_aggregate_and_load_dashboard
