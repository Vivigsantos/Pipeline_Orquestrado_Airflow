# ==============================================================================
# PROJETO 2: Orquestracao de Data Pipeline com Apache Airflow
# DAG: pipeline_medallion_orquestrada
# DESCRICAO: DAG que gerencia o fluxo em camadas (Bronze -> Silver -> Gold)
# ==============================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Configurações padrão da DAG
default_args = {
    'owner': 'Viviane Santos',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ------------------------------------------------------------------------------
# Funções Python que representam a execução de cada etapa da Pipeline Medallion
# ------------------------------------------------------------------------------

def executar_ingestao_bronze():
    """
    Task 1: Camada Bronze - Ingestão dos dados brutos com metadados de auditoria.
    """
    print("=" * 60)
    print("🚀 INICIANDO CAMADA BRONZE (RAW INGESTION)...")
    print("-> Lendo fonte de dados brutos (CSV/JSON)")
    print("-> Adicionando metadados: ingestion_timestamp e source_file")
    print("-> Gravando tabela Delta Lake: bronze_ai_usage")
    print("✅ CAMADA BRONZE CONCLUÍDA COM SUCESSO!")
    print("=" * 60)


def executar_transformacao_silver():
    """
    Task 2: Camada Silver - Limpeza, deduplicação e padronização dos dados.
    """
    print("=" * 60)
    print("🧹 INICIANDO CAMADA SILVER (CLEANSING & DEDUPLICATION)...")
    print("-> Lendo tabela Delta: bronze_ai_usage")
    print("-> Executando Window Function para remover duplicatas por user_id (300 -> 288 registros)")
    print("-> Limpando sufixos e caracteres monetários ('16 yrs' -> 16, '$1,884' -> 1884.0)")
    print("-> Padronizando nomes de colunas em snake_case")
    print("-> Gravando tabela Delta Lake: silver_ai_usage")
    print("✅ CAMADA SILVER CONCLUÍDA COM SUCESSO!")
    print("=" * 60)


def executar_agregacao_gold():
    """
    Task 3: Camada Gold - Agregação de dados e cálculo de KPIs de negócio.
    """
    print("=" * 60)
    print("🥇 INICIANDO CAMADA GOLD (BUSINESS AGGREGATIONS & KPIS)...")
    print("-> Lendo tabela Delta: silver_ai_usage")
    print("-> Calculando métricas de impacto de IA por ferramenta (horas salvas, satisfação, recomendação)")
    print("-> Gerando tabela comparativa Estudantes vs Profissionais")
    print("-> Gravando tabelas Delta Lake: gold_ai_usage_by_tool e gold_metrics_by_user_type")
    print("✅ CAMADA GOLD CONCLUÍDA COM SUCESSO!")
    print("=" * 60)


def notificar_sucesso_pipeline():
    """
    Task 4: Notificação / Finalização da Pipeline.
    """
    print("=" * 60)
    print("🎉 PIPELINE MEDALLION FINALIZADA COM SUCESSO PELO AIRFLOW!")
    print("-> Status: SUCCESS")
    print("-> Alerta enviado para o time de Engenharia de Dados.")
    print("=" * 60)


# ------------------------------------------------------------------------------
# Definição da DAG
# ------------------------------------------------------------------------------

with DAG(
    dag_id='pipeline_medallion_orquestrada',
    default_args=default_args,
    description='Orquestração do Pipeline Medallion (Bronze -> Silver -> Gold)',
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'databricks', 'pyspark', 'portfolio', 'engenharia_de_dados'],
) as dag:

    # Task inicial (Dummy / Start)
    inicio = EmptyOperator(task_id='inicio_pipeline')

    # Task 1: Ingestão Bronze
    task_bronze = PythonOperator(
        task_id='extracao_bronze',
        python_callable=executar_ingestao_bronze,
    )

    # Task 2: Transformação Silver
    task_silver = PythonOperator(
        task_id='transformacao_silver',
        python_callable=executar_transformacao_silver,
    )

    # Task 3: Agregação Gold
    task_gold = PythonOperator(
        task_id='agregacao_gold',
        python_callable=executar_agregacao_gold,
    )

    # Task 4: Notificação Final
    task_notificacao = PythonOperator(
        task_id='notificacao_sucesso',
        python_callable=notificar_sucesso_pipeline,
    )

    # Definindo a Ordem e Dependência das Tarefas (DAG Flow)
    inicio >> task_bronze >> task_silver >> task_gold >> task_notificacao
