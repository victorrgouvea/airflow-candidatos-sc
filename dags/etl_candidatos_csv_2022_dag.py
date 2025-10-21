"""
DAG de ETL para processamento de dados CSV de candidatos 2022 usando chaves naturais
Processa dados de bens, candidatos e notas fiscais dos CSV's do TSE
"""

from datetime import datetime, timedelta
import pandas as pd
import logging
import os
from typing import Dict, List, Any, Tuple

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Configuração de logging
logger = logging.getLogger(__name__)

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def get_db_hook():
    """
    Retorna hook do PostgreSQL usando conexão do Airflow
    """
    return PostgresHook(postgres_conn_id='postgres_candidatos')

def execute_sql(sql_query, parameters=None):
    """
    Executa SQL usando hook do Airflow
    """
    hook = get_db_hook()
    return hook.run(sql_query, parameters=parameters)

def get_first_result(sql_query, parameters=None):
    """
    Executa query e retorna primeiro resultado usando hook do Airflow
    """
    hook = get_db_hook()
    return hook.get_first(sql_query, parameters=parameters)

@dag(
    dag_id='etl_candidatos_csv_2022',
    default_args=default_args,
    description='ETL para dados CSV de candidatos 2022 (bens, candidatos, notas fiscais)',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'candidatos', '2022', 'csv'],
    max_active_runs=1,
)
def etl_candidatos_csv_2022():
    """
    DAG principal de ETL para dados CSV de candidatos 2022
    """

    @task
    def extract_csv_data() -> Dict[str, Any]:
        """
        Extrai dados dos três arquivos CSV
        """
        base_path = '/opt/airflow/dags/data'
        
        def read_csv_with_encoding(file_path, sep=';'):
            """
            Tenta ler CSV com diferentes codificações
            """
            # Priorizar latin1/iso-8859-1 baseado na detecção do tipo de arquivo
            encodings = ['latin1', 'iso-8859-1', 'cp1252', 'windows-1252', 'utf-8']
            
            # Tentar detectar automaticamente com chardet
            try:
                import chardet
                with open(file_path, 'rb') as file:
                    raw_data = file.read(10000)  # Ler apenas os primeiros 10KB para detecção
                    result = chardet.detect(raw_data)
                    detected_encoding = result['encoding']
                    confidence = result['confidence']
                    
                    logger.info(f"Chardet detectou encoding {detected_encoding} com confiança {confidence}")
                    
                    # Se a confiança for alta, tentar primeiro o encoding detectado
                    if confidence > 0.7 and detected_encoding not in encodings:
                        encodings.insert(0, detected_encoding)
                    elif detected_encoding in encodings:
                        # Mover o detectado para o início da lista
                        encodings.remove(detected_encoding)
                        encodings.insert(0, detected_encoding)
            except ImportError:
                logger.info("Chardet não disponível, usando lista padrão de encodings")
            except Exception as e:
                logger.warning(f"Erro na detecção automática de encoding: {e}")
            
            for encoding in encodings:
                try:
                    logger.info(f"Tentando ler {file_path} com encoding {encoding}")
                    df = pd.read_csv(file_path, sep=sep, encoding=encoding, dtype=str)
                    logger.info(f"Sucesso com encoding {encoding}")
                    return df
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"Erro com encoding {encoding}: {e}")
                    continue
            
            raise Exception(f"Não foi possível ler o arquivo {file_path} com nenhuma codificação testada")
        
        try:
            # Carregar CSV de candidatos
            candidatos_file = f'{base_path}/cand/consulta_cand_2022_SC.csv'
            logger.info(f"Lendo arquivo de candidatos: {candidatos_file}")
            df_candidatos = read_csv_with_encoding(candidatos_file)
            
            # Carregar CSV de bens
            bens_file = f'{base_path}/bens/bem_candidato_2022_SC.csv'
            logger.info(f"Lendo arquivo de bens: {bens_file}")
            df_bens = read_csv_with_encoding(bens_file)
            
            # Carregar CSV de notas fiscais
            notas_file = f'{base_path}/nota_fiscal/nota_fiscal_candidato_2022_SC.csv'
            logger.info(f"Lendo arquivo de notas fiscais: {notas_file}")
            df_notas = read_csv_with_encoding(notas_file)
            
            logger.info(f"Dados extraídos com sucesso:")
            logger.info(f"  Candidatos: {len(df_candidatos)} registros")
            logger.info(f"  Bens: {len(df_bens)} registros")
            logger.info(f"  Notas Fiscais: {len(df_notas)} registros")
            
            return {
                'status': 'success',
                'candidatos': df_candidatos.to_dict('records'),
                'bens': df_bens.to_dict('records'),
                'notas_fiscais': df_notas.to_dict('records'),
                'total_candidatos': len(df_candidatos),
                'total_bens': len(df_bens),
                'total_notas': len(df_notas)
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados CSV: {e}")
            raise Exception(f"Erro ao extrair dados CSV: {e}")

    @task
    def transform_data(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transforma os dados extraídos para o formato do banco de dados
        """
        candidatos_raw = raw_data['candidatos']
        bens_raw = raw_data['bens']
        notas_raw = raw_data['notas_fiscais']
        
        # Função auxiliar para limpar strings
        def clean_string(value):
            """
            Limpa e normaliza strings removendo aspas e caracteres problemáticos
            """
            if pd.isna(value) or value is None:
                return ''
            
            # Converter para string se não for
            value = str(value)
            
            # Remover aspas duplas no início e fim
            value = value.strip().strip('"').strip("'")
            
            # Substituir valores nulos
            if value.upper() in ['#NULO', 'NULL', 'NONE', 'NAN']:
                return ''
            
            return value.strip()
        
        def safe_int(value, default=None):
            """
            Converte valor para int de forma segura
            """
            try:
                cleaned = clean_string(value)
                if not cleaned:
                    return default
                return int(float(cleaned))
            except (ValueError, TypeError):
                return default
        
        def safe_float(value, default=0.0):
            """
            Converte valor para float de forma segura
            """
            try:
                cleaned = clean_string(value)
                if not cleaned:
                    return default
                # Substituir vírgula por ponto para números decimais
                cleaned = cleaned.replace(',', '.')
                return float(cleaned)
            except (ValueError, TypeError):
                return default
        
        def safe_date(value, format_str='%d/%m/%Y'):
            """
            Converte data de forma segura
            """
            try:
                cleaned = clean_string(value)
                if not cleaned:
                    return None
                
                # Correção específica para datas com ano "0022" que deveriam ser "2022"
                if cleaned.startswith('0022-'):
                    cleaned = cleaned.replace('0022-', '2022-')
                elif cleaned.startswith('"0022-'):
                    cleaned = cleaned.replace('"0022-', '2022-').strip('"')
                
                # Remover timestamp se existir (ex: "2022-09-16 00:00:00" -> "2022-09-16")
                if ' ' in cleaned:
                    cleaned = cleaned.split(' ')[0]
                
                # Tentar diferentes formatos de data
                formats = [format_str, '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d']
                
                for fmt in formats:
                    try:
                        return pd.to_datetime(cleaned, format=fmt).strftime('%Y-%m-%d')
                    except:
                        continue
                
                # Se nenhum formato funcionou, tentar parsing automático
                return pd.to_datetime(cleaned).strftime('%Y-%m-%d')
                
            except:
                return None
        
        # Estruturas para armazenar dados transformados
        eleicoes = []
        partidos = []
        candidatos = []
        candidaturas = []
        bens = []
        notas_fiscais = []
        
        # Dicionários para controlar candidatos únicos e suas informações mais recentes
        candidatos_por_titulo = {}  # titulo_eleitor -> dados do candidato
        partidos_set = set()
        
        logger.info(f"Iniciando transformação dos dados")
        
        # Eleição 2022 (única para todos) - usando chave natural
        eleicoes.append({
            'ano': 2022,
            'nivel': 'Federal'
        })
        
        # Processar candidatos
        for idx, candidato_data in enumerate(candidatos_raw, 1):
            try:
                cpf = clean_string(candidato_data.get('NR_CPF_CANDIDATO', ''))
                titulo_eleitor = clean_string(candidato_data.get('NR_TITULO_ELEITORAL_CANDIDATO', ''))
                
                # VALIDAÇÃO CRÍTICA: Título de eleitor é obrigatório
                if not titulo_eleitor or len(titulo_eleitor) < 10:
                    logger.warning(f"Registro {idx} pulado: Título de eleitor inválido ou ausente '{titulo_eleitor}'. Nome: {clean_string(candidato_data.get('NM_CANDIDATO', 'N/A'))}")
                    continue
                
                # Processar partido
                partido_numero = safe_int(candidato_data.get('NR_PARTIDO'))
                if partido_numero and partido_numero not in partidos_set:
                    partidos.append({
                        'numero': partido_numero,
                        'sigla': clean_string(candidato_data.get('SG_PARTIDO', '')),
                        'nome': clean_string(candidato_data.get('NM_PARTIDO', '')).title()
                    })
                    partidos_set.add(partido_numero)
                
                # Dados do candidato atual
                data_nasc = safe_date(candidato_data.get('DT_NASCIMENTO', ''))
                candidato_atual = {
                    'titulo_eleitor': titulo_eleitor,
                    'cpf': cpf,
                    'nome': clean_string(candidato_data.get('NM_CANDIDATO', '')),
                    'data_nasc': data_nasc,
                    'sexo': 'M' if clean_string(candidato_data.get('DS_GENERO', '')) == 'MASCULINO' else 'F' if clean_string(candidato_data.get('DS_GENERO', '')) == 'FEMININO' else None,
                    'ano_eleicao': 2022  # Para controle de qual ano são os dados
                }
                
                # Verificar se já temos dados desta pessoa
                if titulo_eleitor in candidatos_por_titulo:
                    # Pessoa já existe, comparar anos para manter dados mais recentes
                    candidato_existente = candidatos_por_titulo[titulo_eleitor]
                    if candidato_atual['ano_eleicao'] >= candidato_existente['ano_eleicao']:
                        # Dados atuais são mais recentes ou iguais, atualizar
                        candidatos_por_titulo[titulo_eleitor] = candidato_atual
                        logger.debug(f"Atualizando dados do candidato {titulo_eleitor} com dados de {candidato_atual['ano_eleicao']}")
                else:
                    # Pessoa nova
                    candidatos_por_titulo[titulo_eleitor] = candidato_atual
                
                # Processar candidatura (sempre criar, pois uma pessoa pode ter várias)
                candidatura = {
                    'candidato_titulo_eleitor': titulo_eleitor,
                    'eleicao_ano': 2022,
                    'eleicao_nivel': 'Federal',
                    'partido_numero': partido_numero,
                    'cargo': clean_string(candidato_data.get('DS_CARGO', '')).title(),
                    'num_urna': safe_int(candidato_data.get('NR_CANDIDATO')),
                    'situacao_atual': clean_string(candidato_data.get('DS_SIT_TOT_TURNO', '')).title(),
                    'cidade': None,  # Essa fonte não tem cidade
                    'estado': clean_string(candidato_data.get('SG_UF', ''))
                }
                candidaturas.append(candidatura)
                
            except Exception as e:
                logger.error(f"Erro ao processar candidato {idx}: {e}")
                continue
        
        # Converter dicionário de candidatos únicos para lista
        for titulo, candidato_data in candidatos_por_titulo.items():
            # Remover campo de controle antes de adicionar à lista final
            candidato_final = {k: v for k, v in candidato_data.items() if k != 'ano_eleicao'}
            candidatos.append(candidato_final)
        
        # Mapear SQ_CANDIDATO para TITULO_ELEITOR para processar bens e notas
        sq_to_titulo = {}
        for candidatura in candidaturas:
            # Tentar encontrar o SQ_CANDIDATO correspondente
            for candidato_data in candidatos_raw:
                titulo_original = clean_string(candidato_data.get('NR_TITULO_ELEITORAL_CANDIDATO', ''))
                
                # Verificar se título é válido antes de fazer mapeamento
                if titulo_original and len(titulo_original) >= 10:
                    if titulo_original == candidatura['candidato_titulo_eleitor']:
                        sq_candidato = clean_string(candidato_data.get('SQ_CANDIDATO'))
                        if sq_candidato:
                            sq_to_titulo[sq_candidato] = candidatura['candidato_titulo_eleitor']
                        break
        
        logger.info(f"Mapeamento SQ_CANDIDATO -> TITULO_ELEITOR: {len(sq_to_titulo)} registros válidos")
        
        # Processar bens
        for idx, bem_data in enumerate(bens_raw, 1):
            try:
                sq_candidato = clean_string(bem_data.get('SQ_CANDIDATO'))
                titulo_candidato = sq_to_titulo.get(sq_candidato)
                
                if titulo_candidato:
                    valor = safe_float(bem_data.get('VR_BEM_CANDIDATO', '0'))
                    data_atualizacao = safe_date(bem_data.get('DT_ULT_ATUAL_BEM_CANDIDATO', ''))
                    
                    bem = {
                        'candidato_titulo_eleitor': titulo_candidato,
                        'eleicao_ano': 2022,
                        'eleicao_nivel': 'Federal',
                        'ordem': safe_int(bem_data.get('NR_ORDEM_BEM_CANDIDATO'), 1),
                        'descricao_tipo': clean_string(bem_data.get('DS_TIPO_BEM_CANDIDATO', '')),
                        'descricao': clean_string(bem_data.get('DS_BEM_CANDIDATO', '')),
                        'valor': valor,
                        'data_atualizacao': data_atualizacao
                    }
                    bens.append(bem)
                
            except Exception as e:
                logger.error(f"Erro ao processar bem {idx}: {e}")
                continue
        
        # Mapear NR_CANDIDATO para TITULO_ELEITOR para processar notas fiscais
        nr_candidato_to_titulo = {}
        for candidatura in candidaturas:
            if candidatura['num_urna']:
                nr_candidato_to_titulo[str(candidatura['num_urna'])] = candidatura['candidato_titulo_eleitor']
        
        # Processar notas fiscais
        for idx, nota_data in enumerate(notas_raw, 1):
            try:
                nr_candidato = clean_string(nota_data.get('NR_CANDIDATO'))
                titulo_candidato = nr_candidato_to_titulo.get(nr_candidato)
                
                if titulo_candidato:
                    cpf_cnpj_fornecedor = clean_string(nota_data.get('NR_CPF_CNPJ_EMITENTE', ''))
                    
                    # Processar valor e data
                    valor = safe_float(nota_data.get('VR_NOTA_FISCAL', '0'))
                    data_emissao = safe_date(nota_data.get('DT_EMISSAO', ''))
                    
                    # Processar série, chave de acesso e URL
                    serie = clean_string(nota_data.get('NR_SERIE', ''))
                    chave_acesso = clean_string(nota_data.get('NR_CHAVE_ACESSO', ''))
                    url_acesso = clean_string(nota_data.get('NM_URL_ACESSO', ''))
                    
                    nota_fiscal = {
                        'candidato_titulo_eleitor': titulo_candidato,
                        'eleicao_ano': 2022,
                        'eleicao_nivel': 'Federal',
                        'numero_nota': clean_string(nota_data.get('NR_NOTA_FISCAL', '')),
                        'serie': serie,
                        'cnpj_emitente': cpf_cnpj_fornecedor,
                        'valor': valor,
                        'data_emissao': data_emissao,
                        'chave_acesso': chave_acesso,
                        'url_acesso': url_acesso,
                        'ue': clean_string(nota_data.get('SG_UE', '')),
                        'unidade_arrecadadora': clean_string(nota_data.get('NM_UNIDADE_ARRECADADORA', ''))
                    }
                    notas_fiscais.append(nota_fiscal)
                
            except Exception as e:
                logger.error(f"Erro ao processar nota fiscal {idx}: {e}")
                continue
        
        resultado = {
            'eleicoes': eleicoes,
            'partidos': partidos,
            'candidatos': candidatos,
            'candidaturas': candidaturas,
            'bens': bens,
            'notas_fiscais': notas_fiscais
        }
        
        logger.info(f"Transformação concluída:")
        for tabela, dados in resultado.items():
            logger.info(f"  {tabela}: {len(dados)} registros")
        
        return resultado

    @task
    def load_eleicoes(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Eleicao
        """
        try:
            eleicoes = data['eleicoes']
            
            for eleicao in eleicoes:
                execute_sql("""
                    INSERT INTO Eleicao (ano, nivel)
                    VALUES (%(ano)s, %(nivel)s)
                    ON CONFLICT (ano, nivel) DO NOTHING
                """, parameters=eleicao)
            
            logger.info(f"Carregados {len(eleicoes)} registros na tabela Eleicao")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar eleições: {e}")
            raise

    @task
    def load_partidos(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Partido
        """
        try:
            partidos = data['partidos']
            
            for partido in partidos:
                execute_sql("""
                    INSERT INTO Partido (numero, sigla, nome)
                    VALUES (%(numero)s, %(sigla)s, %(nome)s)
                    ON CONFLICT (numero) DO UPDATE SET
                        sigla = EXCLUDED.sigla,
                        nome = EXCLUDED.nome
                """, parameters=partido)
            
            logger.info(f"Carregados {len(partidos)} registros na tabela Partido")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar partidos: {e}")
            raise

    @task
    def load_candidatos(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Candidato usando titulo_eleitor como chave primária
        """
        try:
            candidatos = data['candidatos']
            candidatos_processados = 0
            candidatos_pulados = 0
            
            for candidato in candidatos:
                # VALIDAÇÃO CRÍTICA: Verificar se título de eleitor é válido
                titulo_eleitor = candidato.get('titulo_eleitor', '')
                if not titulo_eleitor or len(titulo_eleitor) < 10:
                    logger.error(f"Título de eleitor inválido detectado no carregamento: '{titulo_eleitor}' para {candidato.get('nome', 'N/A')}")
                    candidatos_pulados += 1
                    continue
                
                # Verificar se a pessoa já existe
                existing_candidate = get_first_result("""
                    SELECT titulo_eleitor FROM Candidato WHERE titulo_eleitor = %(titulo_eleitor)s
                """, parameters={'titulo_eleitor': candidato['titulo_eleitor']})
                
                if existing_candidate:
                    # Pessoa existe, atualizar dados
                    logger.info(f"Atualizando candidato existente título: {candidato['titulo_eleitor']}")
                    execute_sql("""
                        UPDATE Candidato SET
                            cpf = COALESCE(%(cpf)s, cpf),
                            nome = %(nome)s,
                            data_nasc = COALESCE(%(data_nasc)s, data_nasc),
                            sexo = COALESCE(NULLIF(%(sexo)s, ''), sexo)
                        WHERE titulo_eleitor = %(titulo_eleitor)s
                    """, parameters=candidato)
                    candidatos_processados += 1
                else:
                    # Pessoa nova, inserir
                    logger.info(f"Inserindo novo candidato título: {candidato['titulo_eleitor']}")
                    execute_sql("""
                        INSERT INTO Candidato (titulo_eleitor, cpf, nome, data_nasc, sexo)
                        VALUES (%(titulo_eleitor)s, %(cpf)s, %(nome)s, %(data_nasc)s, %(sexo)s)
                    """, parameters=candidato)
                    candidatos_processados += 1
            
            logger.info(f"Candidatos processados: {candidatos_processados}, pulados: {candidatos_pulados}")
            
            if candidatos_pulados > 0:
                logger.warning(f"ATENÇÃO: {candidatos_pulados} candidatos foram pulados por título de eleitor inválido")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar candidatos: {e}")
            raise

    @task
    def load_candidaturas(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Candidatura
        """
        try:
            candidaturas = data['candidaturas']
            
            for candidatura in candidaturas:
                execute_sql("""
                    INSERT INTO Candidatura (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, partido_numero, cargo, num_urna, situacao_atual, cidade, estado)
                    VALUES (%(candidato_titulo_eleitor)s, %(eleicao_ano)s, %(eleicao_nivel)s, %(partido_numero)s, %(cargo)s, %(num_urna)s, %(situacao_atual)s, %(cidade)s, %(estado)s)
                    ON CONFLICT (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel) DO UPDATE SET
                        partido_numero = EXCLUDED.partido_numero,
                        cargo = EXCLUDED.cargo,
                        num_urna = EXCLUDED.num_urna,
                        situacao_atual = EXCLUDED.situacao_atual,
                        cidade = EXCLUDED.cidade,
                        estado = EXCLUDED.estado
                """, parameters=candidatura)
            
            logger.info(f"Carregadas {len(candidaturas)} registros na tabela Candidatura")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar candidaturas: {e}")
            raise

    @task
    def load_bens(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Bem
        """
        try:
            bens = data['bens']
            
            for bem in bens:
                execute_sql("""
                    INSERT INTO Bem (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, ordem, descricao_tipo, descricao, valor, data_atualizacao)
                    VALUES (%(candidato_titulo_eleitor)s, %(eleicao_ano)s, %(eleicao_nivel)s, %(ordem)s, %(descricao_tipo)s, %(descricao)s, %(valor)s, %(data_atualizacao)s)
                    ON CONFLICT (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, ordem) DO UPDATE SET
                        descricao_tipo = EXCLUDED.descricao_tipo,
                        descricao = EXCLUDED.descricao,
                        valor = EXCLUDED.valor,
                        data_atualizacao = EXCLUDED.data_atualizacao
                """, parameters=bem)
            
            logger.info(f"Carregados {len(bens)} registros na tabela Bem")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar bens: {e}")
            raise

    @task
    def load_notas_fiscais(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Nota_Fiscal
        """
        try:
            notas_fiscais = data['notas_fiscais']
            
            for nota in notas_fiscais:
                execute_sql("""
                    INSERT INTO Nota_Fiscal (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, numero_nota, serie, cnpj_emitente, valor, data_emissao, chave_acesso, url_acesso, ue, unidade_arrecadadora)
                    VALUES (%(candidato_titulo_eleitor)s, %(eleicao_ano)s, %(eleicao_nivel)s, %(numero_nota)s, %(serie)s, %(cnpj_emitente)s, %(valor)s, %(data_emissao)s, %(chave_acesso)s, %(url_acesso)s, %(ue)s, %(unidade_arrecadadora)s)
                    ON CONFLICT (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, numero_nota, serie) DO UPDATE SET
                        cnpj_emitente = EXCLUDED.cnpj_emitente,
                        valor = EXCLUDED.valor,
                        data_emissao = EXCLUDED.data_emissao,
                        chave_acesso = EXCLUDED.chave_acesso,
                        url_acesso = EXCLUDED.url_acesso,
                        ue = EXCLUDED.ue,
                        unidade_arrecadadora = EXCLUDED.unidade_arrecadadora
                """, parameters=nota)
            
            logger.info(f"Carregadas {len(notas_fiscais)} registros na tabela Nota_Fiscal")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar notas fiscais: {e}")
            raise

    @task
    def validate_data_quality() -> Dict[str, Any]:
        """
        Valida a qualidade dos dados carregados com foco em pessoas únicas e múltiplas candidaturas
        """
        try:
            # Contadores de registros
            counts = {}
            
            tables = [
                'Eleicao', 'Partido', 'Candidato', 'Candidatura', 
                'Bem', 'Nota_Fiscal'
            ]
            
            for table in tables:
                result = get_first_result(f"SELECT COUNT(*) FROM {table}")
                counts[table] = result[0] if result else 0
            
            # Validações específicas
            validations = {}
            
            # Verificar candidaturas por eleição
            result = get_first_result("""
                SELECT eleicao_ano, eleicao_nivel, COUNT(*) 
                FROM Candidatura 
                GROUP BY eleicao_ano, eleicao_nivel 
                ORDER BY eleicao_ano, eleicao_nivel
            """)
            if result:
                validations['candidaturas_por_eleicao'] = f"{result[0]} - {result[1]}: {result[2]} candidaturas"
            
            # Verificar pessoas com múltiplas candidaturas
            result = get_first_result("""
                SELECT COUNT(*) FROM (
                    SELECT candidato_titulo_eleitor, COUNT(*) as num_candidaturas
                    FROM Candidatura 
                    GROUP BY candidato_titulo_eleitor 
                    HAVING COUNT(*) > 1
                ) multiplas_candidaturas
            """)
            validations['pessoas_com_multiplas_candidaturas'] = result[0] if result else 0
            
            # Verificar dados de 2022
            result = get_first_result("""
                SELECT COUNT(*) FROM Candidatura 
                WHERE eleicao_ano = 2022 AND eleicao_nivel = 'Federal'
            """)
            validations['candidaturas_2022'] = result[0] if result else 0
            
            # Verificar bens de candidatos 2022
            result = get_first_result("""
                SELECT COUNT(*) FROM Bem 
                WHERE eleicao_ano = 2022 AND eleicao_nivel = 'Federal'
            """)
            validations['bens_2022'] = result[0] if result else 0
            
            # Verificar notas fiscais de 2022
            result = get_first_result("""
                SELECT COUNT(*) FROM Nota_Fiscal 
                WHERE eleicao_ano = 2022 AND eleicao_nivel = 'Federal'
            """)
            validations['notas_fiscais_2022'] = result[0] if result else 0
            
            # Verificar integridade: candidatos sem candidatura
            result = get_first_result("""
                SELECT COUNT(*) FROM Candidato c
                LEFT JOIN Candidatura ca ON c.titulo_eleitor = ca.candidato_titulo_eleitor
                WHERE ca.candidato_titulo_eleitor IS NULL
            """)
            validations['candidatos_sem_candidatura'] = result[0] if result else 0
            
            # Verificar candidaturas órfãs
            result = get_first_result("""
                SELECT COUNT(*) FROM Candidatura ca
                LEFT JOIN Candidato c ON ca.candidato_titulo_eleitor = c.titulo_eleitor
                WHERE c.titulo_eleitor IS NULL
            """)
            validations['candidaturas_orfas'] = result[0] if result else 0
            
            # VALIDAÇÃO CRÍTICA: Verificar se há títulos inválidos no banco
            result = get_first_result("""
                SELECT COUNT(*) FROM Candidato 
                WHERE titulo_eleitor IS NULL 
                   OR LENGTH(titulo_eleitor) < 10 
                   OR titulo_eleitor !~ '^[0-9]+$'
            """)
            validations['titulos_invalidos'] = result[0] if result else 0
            
            # Verificar títulos duplicados (não deveria acontecer por causa da PK)
            result = get_first_result("""
                SELECT COUNT(*) FROM (
                    SELECT titulo_eleitor, COUNT(*) 
                    FROM Candidato 
                    GROUP BY titulo_eleitor 
                    HAVING COUNT(*) > 1
                ) duplicados
            """)
            validations['titulos_duplicados'] = result[0] if result else 0
            
            logger.info("Validação de qualidade dos dados CSV 2022:")
            logger.info(f"Contadores por tabela: {counts}")
            logger.info(f"Validações: {validations}")
            
            # ALERTA CRÍTICO se houver títulos inválidos
            if validations.get('titulos_invalidos', 0) > 0:
                logger.error(f"ALERTA CRÍTICO: {validations['titulos_invalidos']} títulos de eleitor inválidos encontrados no banco!")
            
            if validations.get('titulos_duplicados', 0) > 0:
                logger.error(f"ALERTA CRÍTICO: {validations['titulos_duplicados']} títulos de eleitor duplicados encontrados no banco!")
            
            return {
                'status': 'success',
                'counts': counts,
                'validations': validations,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro na validação de qualidade: {e}")
            raise

    # Definir o fluxo da DAG
    raw_data = extract_csv_data()
    transformed_data = transform_data(raw_data)
    
    # Tasks de carregamento - ordem de dependências importante
    eleicoes_loaded = load_eleicoes(transformed_data)
    partidos_loaded = load_partidos(transformed_data)
    
    # Candidatos devem ser carregados após eleições
    candidatos_loaded = load_candidatos(transformed_data)
    candidatos_loaded.set_upstream(eleicoes_loaded)
    
    # Candidaturas devem ser carregadas após candidatos E partidos
    candidaturas_loaded = load_candidaturas(transformed_data)
    candidaturas_loaded.set_upstream([candidatos_loaded, partidos_loaded])
    
    # Tabelas dependentes de candidaturas
    bens_loaded = load_bens(transformed_data)
    notas_loaded = load_notas_fiscais(transformed_data)
    
    bens_loaded.set_upstream(candidaturas_loaded)
    notas_loaded.set_upstream([candidaturas_loaded])
    
    # Validação final
    validation_result = validate_data_quality()
    validation_result.set_upstream([bens_loaded, notas_loaded])

# Instanciar a DAG
dag_instance = etl_candidatos_csv_2022() 