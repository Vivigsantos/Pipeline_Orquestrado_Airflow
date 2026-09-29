# ==============================================================================
# PROJETO 2: Orquestração de Pipeline de Vendas & E-Commerce com Apache Airflow
# DAG: dag_orquestracao_vendas
# AUTORA: Viviane Santos
# DESCRICAO: Pipeline ETL diário de vendas, validação de clientes e métricas de faturamento
# ==============================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ------------------------------------------------------------------------------
# Configurações padrão da DAG
# ------------------------------------------------------------------------------
default_args = {
    'owner': 'Viviane Santos',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


# ------------------------------------------------------------------------------
# Funções Python das Tarefas da Pipeline (Tasks)
# ------------------------------------------------------------------------------

def extracao_pedidos_ecommerce():
    """
    Task 1: Simula a ingestão e extração de vendas/pedidos do dia a partir do sistema de E-commerce.
    """
    print("=" * 70)
    print("📦 TASK 1: EXTRAÇÃO DE PEDIDOS DO E-COMMERCE")
    print("-> Conectando ao banco relacional de vendas do E-commerce...")
    print("-> Extraindo pedidos da data atual...")
    
    # Amostra simulada dos dados brutos extraídos
    pedidos_extraidos = [
        {"order_id": "ORD-1001", "cliente": " Ana Silva ", "email": "ana.silva@email.com", "valor": "R$ 250,50", "produto": "Notebook Bag"},
        {"order_id": "ORD-1002", "cliente": "CARLOS SOUZA", "email": "carlos_email_invalido", "valor": "R$ 1.200,00", "produto": "Monitor 27in"},
        {"order_id": "ORD-1003", "cliente": "Mariana Lima", "email": "mariana@empresa.com", "valor": "R$ 89,90", "produto": "Mouse Sem Fio"},
        {"order_id": "ORD-1004", "cliente": "João Pedro", "email": "joao.pedro@gmail.com", "valor": "R$ 450,00", "produto": "Teclado Mecânico"}
    ]
    
    print(f"-> Total de pedidos extraídos: {len(pedidos_extraidos)}")
    print("✅ Extração finalizada com sucesso!")
    print("=" * 70)
    return pedidos_extraidos


def validacao_e_limpeza_clientes():
    """
    Task 2: Valida e-mails de clientes, limpa formatações de valores e remove inconsistências.
    """
    print("=" * 70)
    print("🧹 TASK 2: VALIDAÇÃO E LIMPEZA DE DADOS DE CLIENTES E PREÇOS")
    print("-> Padronizando nomes de clientes para Title Case...")
    print("-> Removendo símbolos de moeda ('R$') e convertendo valores para números float...")
    print("-> Validando sintaxe de e-mails...")
    
    # Simulação da regra de limpeza
    print("   [CLEANED] ORD-1001: ' Ana Silva ' -> 'Ana Silva' | R$ 250,50 -> 250.50 | Email Válido")
    print("   [INVALID] ORD-1002: Email 'carlos_email_invalido' sinalizado para quarentena!")
    print("   [CLEANED] ORD-1003: 'Mariana Lima' -> 'Mariana Lima' | R$ 89,90 -> 89.90 | Email Válido")
    print("   [CLEANED] ORD-1004: 'João Pedro' -> 'João Pedro' | R$ 450,00 -> 450.00 | Email Válido")
    
    print("✅ Validação e limpeza concluídas! 3 pedidos válidos prontos para consolidação.")
    print("=" * 70)


def calculo_faturamento_diario():
    """
    Task 3: Consolida o faturamento diário, calcula o ticket médio e os produtos mais vendidos.
    """
    print("=" * 70)
    print("📊 TASK 3: CÁLCULO DE FATURAMENTO E KPIS DIÁRIOS")
    print("-> Calculando faturamento total do dia...")
    faturamento_total = 250.50 + 89.90 + 450.00
    ticket_medio = faturamento_total / 3
    
    print(f"📈 FATURAMENTO TOTAL DO DIA: R$ {faturamento_total:.2f}")
    print(f"🎯 TICKET MÉDIO POR PEDIDO:  R$ {ticket_medio:.2f}")
    print("🏆 PRODUTO MAIS VENDIDO (VALOR): Teclado Mecânico (R$ 450,00)")
    print("✅ KPIs de vendas consolidados com sucesso!")
    print("=" * 70)


def envio_relatorio_diretoria():
    """
    Task 4: Envia relatório/alerta de notificação da conclusão do pipeline para a diretoria/time de dados.
    """
    print("=" * 70)
    print("📧 TASK 4: ENVIO DE RELATÓRIO E ALERTA DE SUCESSO")
    print("-> Gerando resumo executivo em HTML/Text...")
    print("-> Disparando notificação de conclusão para o Slack/Email da Diretoria...")
    print("   ------------------------------------------------------------")
    print("   [ASSUNTO]: Relatório Diário de Vendas E-Commerce - Carga Concluída")
    print("   [CORPO]: A pipeline de vendas foi executada com sucesso pelo Airflow.")
    print("   [STATUS]: 100% SUCCESS")
    print("   ------------------------------------------------------------")
    print("✅ Relatório enviado com sucesso!")
    print("=" * 70)


# ------------------------------------------------------------------------------
# Definição do Grafo (DAG)
# ------------------------------------------------------------------------------

with DAG(
    dag_id='dag_orquestracao_vendas',
    default_args=default_args,
    description='Pipeline ETL de Vendas E-Commerce: Extração, Limpeza, KPIs e Relatório',
    schedule_interval='@daily',
    catchup=False,
    tags=['e-commerce', 'vendas', 'airflow', 'etl', 'portfolio'],
) as dag:

    # Ponto de entrada (Start)
    inicio = EmptyOperator(task_id='inicio_pipeline')

    # Tarefa 1: Extração
    t1_extracao = PythonOperator(
        task_id='extracao_pedidos_ecommerce',
        python_callable=extracao_pedidos_ecommerce,
    )

    # Tarefa 2: Limpeza e Validação
    t2_limpeza = PythonOperator(
        task_id='validacao_e_limpeza_clientes',
        python_callable=validacao_e_limpeza_clientes,
    )

    # Tarefa 3: KPIs e Faturamento
    t3_kpis = PythonOperator(
        task_id='calculo_faturamento_diario',
        python_callable=calculo_faturamento_diario,
    )

    # Tarefa 4: Notificação / Relatório
    t4_relatorio = PythonOperator(
        task_id='envio_relatorio_diretoria',
        python_callable=envio_relatorio_diretoria,
    )

    # Ordem e Dependência de Execução (Directed Acyclic Graph)
    inicio >> t1_extracao >> t2_limpeza >> t3_kpis >> t4_relatorio
