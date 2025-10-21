"""
Módulo para configuração do banco de dados PostgreSQL
Contém funções para criar conexões, executar schema e testar conectividade
"""

import os
import logging
from sqlalchemy import create_engine, text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Connection
from airflow import settings

logger = logging.getLogger(__name__)

def get_postgres_connection_string():
    """
    Retorna a string de conexão do PostgreSQL baseada nas variáveis de ambiente
    """
    host = os.getenv('POSTGRES_HOST', 'postgres_candidatos')
    port = os.getenv('POSTGRES_PORT', '5434')
    database = os.getenv('POSTGRES_DB', 'candidatos_sc')
    user = os.getenv('POSTGRES_USER', 'airflow')
    password = os.getenv('POSTGRES_PASSWORD', 'airflow')
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

def get_postgres_hook():
    """
    Retorna um PostgresHook configurado para o banco de candidatos
    """
    try:
        return PostgresHook(
            postgres_conn_id='postgres_candidatos',
            schema='candidatos_sc'
        )
    except Exception as e:
        logger.warning(f"Erro ao criar PostgresHook: {e}")

        # Para uso direto sem hook do Airflow


def create_database_if_not_exists():
    """
    Cria o banco de dados se ele não existir
    """
    try:
        host = os.getenv('POSTGRES_HOST', 'postgres_candidatos')
        port = os.getenv('POSTGRES_PORT', '5434')
        database = os.getenv('POSTGRES_DB', 'candidatos_sc')
        user = os.getenv('POSTGRES_USER', 'airflow')
        password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        
        # Conectar ao banco postgres para criar o banco de dados
        admin_conn_string = f"postgresql://{user}:{password}@{host}:{port}/postgres"
        admin_engine = create_engine(admin_conn_string)
        
        with admin_engine.connect() as conn:
            # Verificar se o banco existe
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{database}'"))
            if not result.fetchone():
                # Criar o banco
                conn.execute(text("COMMIT"))  # Finalizar transação atual
                conn.execute(text(f"CREATE DATABASE {database}"))
                logger.info(f"Banco de dados {database} criado com sucesso")
            else:
                logger.info(f"Banco de dados {database} já existe")
                
        admin_engine.dispose()
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        return False

def execute_schema():
    """
    Executa o schema SQL para criar as tabelas
    """
    try:
        connection_string = get_postgres_connection_string()
        engine = create_engine(connection_string)
        
        # Ler o arquivo de schema
        schema_path = '/opt/airflow/dags/../database/scripts/01-schema.sql'
        if not os.path.exists(schema_path):
            # Tentar caminho alternativo
            schema_path = '/docker-entrypoint-initdb.d/01-schema.sql'
        
        if not os.path.exists(schema_path):
            logger.error(f"Arquivo de schema não encontrado: {schema_path}")
            return False
            
        with open(schema_path, 'r', encoding='utf-8') as file:
            schema_sql = file.read()
        
        with engine.connect() as conn:
            # Executar o schema SQL
            conn.execute(text(schema_sql))
            conn.commit()
            logger.info("Schema executado com sucesso")
            
        engine.dispose()
        return True
        
    except Exception as e:
        logger.error(f"Erro ao executar schema: {e}")
        return False

def reset_database():
    """
    Reseta completamente o banco de dados, removendo todas as tabelas e recriando o schema
    ATENÇÃO: Esta função remove TODOS os dados existentes!
    """
    try:
        connection_string = get_postgres_connection_string()
        engine = create_engine(connection_string)
        
        # Ler o arquivo de reset
        reset_path = '/opt/airflow/dags/../database/scripts/reset_database.sql'
        if not os.path.exists(reset_path):
            # Tentar caminho alternativo
            reset_path = '/docker-entrypoint-initdb.d/reset_database.sql'
        
        if not os.path.exists(reset_path):
            logger.error(f"Arquivo de reset não encontrado: {reset_path}")
            return False
            
        with open(reset_path, 'r', encoding='utf-8') as file:
            reset_sql = file.read()
        
        with engine.connect() as conn:
            # Executar o script de reset
            # Dividir em comandos individuais para executar um por vez
            statements = [stmt.strip() for stmt in reset_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    conn.execute(text(statement))
            
            conn.commit()
            logger.info("Banco de dados resetado com sucesso")
            
        engine.dispose()
        return True
        
    except Exception as e:
        logger.error(f"Erro ao resetar banco de dados: {e}")
        return False

def test_connection():
    """
    Testa a conexão com o banco de dados
    """
    try:
        connection_string = get_postgres_connection_string()
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            test_value = result.fetchone()[0]
            
            if test_value == 1:
                logger.info("Teste de conexão realizado com sucesso")
                engine.dispose()
                return True
            else:
                logger.error("Teste de conexão falhou")
                engine.dispose()
                return False
                
    except Exception as e:
        logger.error(f"Erro no teste de conexão: {e}")
        return False

def create_airflow_connection():
    """
    Cria a conexão do Airflow para o banco de candidatos
    """
    try:
        session = settings.Session()
        
        # Verificar se a conexão já existe
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'postgres_candidatos'
        ).first()
        
        if existing_conn:
            logger.info("Conexão postgres_candidatos já existe")
            session.close()
            return True
        
        # Criar nova conexão
        new_conn = Connection(
            conn_id='postgres_candidatos',
            conn_type='postgres',
            host=os.getenv('POSTGRES_HOST', 'postgres_candidatos'),
            port=int(os.getenv('POSTGRES_PORT', '5434')),
            schema=os.getenv('POSTGRES_DB', 'candidatos_sc'),
            login=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow')
        )
        
        session.add(new_conn)
        session.commit()
        session.close()
        
        logger.info("Conexão postgres_candidatos criada com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar conexão do Airflow: {e}")
        return False

def drop_all_tables():
    """
    Remove todas as tabelas do banco (usar com cuidado!)
    """
    try:
        connection_string = get_postgres_connection_string()
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Listar todas as tabelas
            result = conn.execute(text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
            """))
            
            tables = [row[0] for row in result.fetchall()]
            
            if tables:
                # Remover todas as tabelas
                tables_str = ', '.join(tables)
                conn.execute(text(f"DROP TABLE {tables_str} CASCADE"))
                conn.commit()
                logger.info(f"Tabelas removidas: {tables_str}")
            else:
                logger.info("Nenhuma tabela encontrada para remover")
                
        engine.dispose()
        return True
        
    except Exception as e:
        logger.error(f"Erro ao remover tabelas: {e}")
        return False

def setup_complete_database():
    """
    Executa a configuração completa do banco de dados
    """
    try:
        logger.info("Iniciando configuração completa do banco de dados")
        
        # 1. Criar banco se não existir
        if not create_database_if_not_exists():
            raise Exception("Falha ao criar banco de dados")
        
        # 2. Criar conexão no Airflow
        if not create_airflow_connection():
            logger.warning("Falha ao criar conexão do Airflow (não crítico)")
        
        # 3. Executar schema - Comentado pois o schema é executado via init scripts
        # if not execute_schema():
        #     raise Exception("Falha ao executar schema")
        
        # 4. Testar conexão
        if not test_connection():
            raise Exception("Falha no teste de conexão")
        
        logger.info("Configuração completa do banco de dados finalizada com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro na configuração completa: {e}")
        raise e 