"""
DAG para configuração inicial do banco de dados de candidatos
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from database_setup import (
    create_database_if_not_exists,
    create_airflow_connection,
    execute_schema,
    test_connection,
    setup_complete_database
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def setup_database_callable(**kwargs):
    """
    Executa a configuração completa do banco de dados
    """
    try:
        setup_complete_database()
        return "Database setup completed successfully"
    except Exception as e:
        raise Exception(f"Database setup failed: {e}")

def test_database_connection(**kwargs):
    """
    Testa a conexão com o banco de dados
    """
    if test_connection():
        return "Database connection test successful"
    else:
        raise Exception("Database connection test failed")

with DAG(
    dag_id='setup_database',
    default_args=default_args,
    description='Configura o banco de dados PostgreSQL para dados de candidatos',
    schedule_interval=None,  # Executar manualmente
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['setup', 'database', 'admin'],
) as dag:

    # Task 1: Configurar banco completo
    setup_database_task = PythonOperator(
        task_id='setup_database',
        python_callable=setup_database_callable,
    )

    # Task 2: Testar conexão
    test_connection_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_database_connection,
    )

    # Definir dependências
    setup_database_task >> test_connection_task 