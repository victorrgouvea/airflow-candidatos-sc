from datetime import datetime, timedelta
import json
import logging
import pandas as pd
from typing import Dict, List, Any

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Configuração de logging
logger = logging.getLogger(__name__)

def convert_date_to_iso(date_str):
    """
    Converte data do formato brasileiro DD/MM/YYYY para formato ISO YYYY-MM-DD
    """
    if not date_str or date_str.strip() == '':
        return None
    
    try:
        # Tentar formato brasileiro DD/MM/YYYY
        if '/' in date_str:
            dt = datetime.strptime(date_str, '%d/%m/%Y')
            return dt.date().isoformat()
        # Se já estiver em formato ISO, retornar como está
        elif '-' in date_str:
            return date_str
        else:
            return None
    except ValueError:
        logger.warning(f"Não foi possível converter data: {date_str}")
        return None

def validate_numeric_value(value, max_value=9999999999999.99):
    if value is None:
        return None
    
    try:
        numeric_value = float(value)
        
        if abs(numeric_value) > max_value:
            logger.warning(f"Valor numérico muito grande ignorado (provável dado incorreto): {value}")
            return None
        
        return numeric_value
    except (ValueError, TypeError):
        logger.warning(f"Valor numérico inválido: {value}")
        return None

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
    dag_id='etl_candidatos_2024',
    default_args=default_args,
    description='ETL completo para dados de candidatos 2024',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'candidatos', '2024'],
    max_active_runs=1,
)
def etl_candidatos_2024():
    """
    DAG principal de ETL para dados de candidatos usando chaves naturais
    """

    @task
    def extract_json_data() -> Dict[str, Any]:
        """
        Extrai dados do arquivo JSON dos candidatos
        """
        json_file_path = '/opt/airflow/dags/data/candidatos_sc_2024.json'
        
        try:
            logger.info(f"Lendo arquivo JSON: {json_file_path}")
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            candidatos_data = data.get('candidatos', [])
            
            logger.info(f"Arquivo JSON carregado com sucesso. Total de registros: {len(candidatos_data)}")
            
            return {
                'status': 'success',
                'total_registros': len(candidatos_data),
                'data_geracao': data.get('data_geracao'),
                'dados': candidatos_data
            }
            
        except FileNotFoundError:
            logger.error(f"Arquivo JSON não encontrado: {json_file_path}")
            raise FileNotFoundError(f"Arquivo JSON não encontrado: {json_file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON: {e}")
            raise ValueError(f"Erro ao decodificar JSON: {e}")
        except Exception as e:
            logger.error(f"Erro inesperado ao extrair dados: {e}")
            raise Exception(f"Erro inesperado ao extrair dados: {e}")

    @task
    def transform_data(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transforma os dados extraídos para o formato do banco de dados
        """
        candidatos_raw = raw_data['dados']
        
        # Estruturas para armazenar dados transformados
        eleicoes = []
        partidos = []
        candidatos = []
        candidaturas = []
        bens = []
        resumos_despesas = []
        doadores = []
        doacoes = []
        fornecedores = []
        despesas = []
        notas_fiscais = []
        
        # Sets para evitar duplicatas
        partidos_set = set()
        doadores_set = set()
        fornecedores_set = set()
        
        logger.info(f"Iniciando transformação de {len(candidatos_raw)} candidatos")
        
        # Eleição 2024 (única para todos) - usando chave natural
        eleicoes.append({
            'ano': 2024,
            'nivel': 'Municipal'
        })
        
        for idx, candidato_data in enumerate(candidatos_raw, 1):
            try:
                # Dados de identificação
                candidato_info = candidato_data.get('candidato', {})
                candidatura_info = candidato_data.get('candidatura', {})
                financeiro = candidato_data.get('financeiro', {})
                partido_info = candidatura_info.get('partido', {})
                
                # VALIDAÇÃO CRÍTICA: Título de eleitor é obrigatório
                titulo_eleitor = candidato_info.get('tituloEleitor', '').strip()
                if not titulo_eleitor or len(titulo_eleitor) < 10:
                    logger.warning(f"Registro {idx} pulado: Título de eleitor inválido ou ausente '{titulo_eleitor}'. Nome: {candidato_info.get('nome', 'N/A')}")
                    continue
                
                # Processar partido
                partido_numero = partido_info.get('numero')
                if partido_numero is not None and partido_numero not in partidos_set:
                    partidos.append({
                        'numero': partido_numero,
                        'sigla': partido_info.get('sigla'),
                        'nome': partido_info.get('nome')
                    })
                    partidos_set.add(partido_numero)
                    logger.debug(f"Adicionado partido: {partido_numero} - {partido_info.get('sigla')}")
                
                if partido_numero is not None:
                    candidato = {
                        'titulo_eleitor': titulo_eleitor,
                        'cpf': None,
                        'nome': candidato_info.get('nome'),
                        'data_nasc': candidato_info.get('dataNasc'),
                        'sexo': 'M' if candidato_info.get('sexo') == 'MASC.' else 'F' if candidato_info.get('sexo') == 'FEM.' else None
                    }
                    candidatos.append(candidato)
                    
                    candidatura = {
                        'candidato_titulo_eleitor': titulo_eleitor,
                        'eleicao_ano': 2024,
                        'eleicao_nivel': 'Municipal',
                        'partido_numero': partido_numero,
                        'cargo': candidatura_info.get('cargo'),
                        'num_urna': candidatura_info.get('numUrna'),
                        'situacao_atual': candidatura_info.get('situacaoAtual'),
                        'cidade': candidatura_info.get('cidade').title(),
                        'estado': candidatura_info.get('estado')
                    }
                    candidaturas.append(candidatura)
                    
                    # Processar bens
                    bens_candidato = financeiro.get('bens', [])
                    for bem in bens_candidato:
                        if bem.get('ordem') is not None:
                            bens.append({
                                'candidato_titulo_eleitor': titulo_eleitor,
                                'eleicao_ano': 2024,
                                'eleicao_nivel': 'Municipal',
                                'ordem': bem.get('ordem'),
                                'descricao_tipo': bem.get('descricaoTipoBem'),
                                'descricao': bem.get('descricao'),
                                'valor': validate_numeric_value(bem.get('valor')),
                                'data_atualizacao': bem.get('dataUltimaAtualizacao')
                            })
                    
                    # Processar resumo de despesas
                    resumo_info = financeiro.get('resumoDespesas', {})
                    if any([resumo_info.get('limiteDeGastos'),
                            resumo_info.get('totalDespesasContratadas'),
                            resumo_info.get('totalDespesasPagas'),
                            resumo_info.get('doacoesParaOutrosCandidatosOuPartidos')]):
                        resumo_despesa = {
                            'candidato_titulo_eleitor': titulo_eleitor,
                            'eleicao_ano': 2024,
                            'eleicao_nivel': 'Municipal',
                            'limite_gastos': validate_numeric_value(resumo_info.get('limiteDeGastos')),
                            'total_despesas_contratadas': validate_numeric_value(resumo_info.get('totalDespesasContratadas')),
                            'total_despesas_pagas': validate_numeric_value(resumo_info.get('totalDespesasPagas')),
                            'doacoes_outros_candidatos': validate_numeric_value(resumo_info.get('doacoesParaOutrosCandidatosOuPartidos'))
                        }
                        resumos_despesas.append(resumo_despesa)
                    
                    # Processar doadores e doações
                    doadores_candidato = financeiro.get('doadores', [])
                    for doador in doadores_candidato:
                        cpf_cnpj = doador.get('cpfCnpj')
                        if not cpf_cnpj:  # Chave obrigatória
                            continue
                            
                        if cpf_cnpj not in doadores_set:
                            doadores.append({
                                'cpf_cnpj': cpf_cnpj,
                                'nome': doador.get('nome')
                            })
                            doadores_set.add(cpf_cnpj)
                        
                        # Processar múltiplas doações do mesmo doador
                        doacoes_doador = doador.get('doacoes', [])
                        for doacao in doacoes_doador:
                            doacoes.append({
                                'candidato_titulo_eleitor': titulo_eleitor,
                                'eleicao_ano': 2024,
                                'eleicao_nivel': 'Municipal',
                                'cpf_cnpj_doador': cpf_cnpj,
                                'valor': validate_numeric_value(doacao.get('valor')),
                                'nr_recibo_eleitoral': doacao.get('nr_recibo_eleitoral'),
                                'nr_documento': doacao.get('nr_documento'),
                                'data': convert_date_to_iso(doacao.get('data')),
                                'ds_receita': doacao.get('ds_receita'),
                                'especie_recurso': doacao.get('especie_recurso')
                            })
                    
                    # Processar fornecedores e despesas
                    fornecedores_candidato = financeiro.get('fornecedores', [])
                    for fornecedor in fornecedores_candidato:
                        cpf_cnpj = fornecedor.get('cpfCnpj')
                        if not cpf_cnpj:
                            continue
                            
                        if cpf_cnpj not in fornecedores_set:
                            fornecedores.append({
                                'cpf_cnpj': cpf_cnpj,
                                'nome': fornecedor.get('nome')
                            })
                            fornecedores_set.add(cpf_cnpj)
                        
                        # Processar múltiplas despesas do mesmo fornecedor
                        despesas_fornecedor = fornecedor.get('despesas_pagas', [])
                        for despesa in despesas_fornecedor:
                            despesas.append({
                                'candidato_titulo_eleitor': titulo_eleitor,
                                'eleicao_ano': 2024,
                                'eleicao_nivel': 'Municipal',
                                'fornecedor_cpf_cnpj': cpf_cnpj,
                                'tipo': despesa.get('tipo'),
                                'descricao': despesa.get('descricao'),
                                'valor': validate_numeric_value(despesa.get('valor')),
                                'data': convert_date_to_iso(despesa.get('data')),
                                'especie_documento_emitido': despesa.get('especie_documento_emitido')
                            })
                    
                    # Processar notas fiscais
                    notas_candidato = financeiro.get('notasFiscais', [])
                    for nota in notas_candidato:
                        numero_nota = nota.get('numeroNota')
                        serie = nota.get('serie')
                        
                        if numero_nota:  # Chave obrigatória
                            # Converter timestamp para data se necessário
                            data_emissao = nota.get('dataEmissao')
                            if isinstance(data_emissao, int):
                                # Converter timestamp unix para data
                                data_emissao = datetime.fromtimestamp(data_emissao / 1000).date() if data_emissao else None
                            
                            notas_fiscais.append({
                                'candidato_titulo_eleitor': titulo_eleitor,
                                'eleicao_ano': 2024,
                                'eleicao_nivel': 'Municipal',
                                'numero_nota': numero_nota,
                                'serie': serie,
                                'cnpj_emitente': nota.get('cnpjEmitente'),
                                'valor': validate_numeric_value(nota.get('valor')),
                                'data_emissao': data_emissao,
                                'chave_acesso': nota.get('chaveAcesso'),
                                'url_acesso': nota.get('urlAcesso'),
                                'ue': nota.get('ue'),
                                'unidade_arrecadadora': nota.get('unidadeArrecadadora')
                            })
                    
                else:
                    logger.warning(f"Candidato {idx} sem partido válido, pulando candidato e candidatura")
                
            except Exception as e:
                logger.error(f"Erro ao processar candidato {idx}: {e}")
                continue
        
        resultado = {
            'eleicoes': eleicoes,
            'partidos': partidos,
            'candidatos': candidatos,
            'candidaturas': candidaturas,
            'bens': bens,
            'resumos_despesas': resumos_despesas,
            'doadores': doadores,
            'doacoes': doacoes,
            'fornecedores': fornecedores,
            'despesas': despesas,
            'notas_fiscais': notas_fiscais
        }
        
        logger.info(f"Transformação concluída:")
        for tabela, dados in resultado.items():
            logger.info(f"  {tabela}: {len(dados)} registros")
        
        return resultado

    @task
    def load_eleicoes(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Eleicao usando chaves naturais
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
        Carrega dados da tabela Partido usando chaves naturais
        """
        try:
            partidos = data['partidos']
            logger.info(f"Iniciando carregamento de {len(partidos)} partidos")
            
            for partido in partidos:
                logger.debug(f"Carregando partido: {partido}")
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
        Carrega dados da tabela Candidato
        """
        try:
            candidatos = data['candidatos']
            
            for candidato in candidatos:
                execute_sql("""
                    INSERT INTO Candidato (titulo_eleitor, cpf, nome, data_nasc, sexo)
                    VALUES (%(titulo_eleitor)s, %(cpf)s, %(nome)s, %(data_nasc)s, %(sexo)s)
                    ON CONFLICT (titulo_eleitor) DO UPDATE SET
                        cpf = EXCLUDED.cpf,
                        nome = EXCLUDED.nome,
                        data_nasc = EXCLUDED.data_nasc,
                        sexo = EXCLUDED.sexo
                """, parameters=candidato)
            
            logger.info(f"Carregados {len(candidatos)} registros na tabela Candidato")
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
    def load_resumos_despesas(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Resumo_Despesas
        """
        try:
            resumos = data['resumos_despesas']
            
            for resumo in resumos:
                execute_sql("""
                    INSERT INTO Resumo_Despesas (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, limite_gastos, total_despesas_contratadas, total_despesas_pagas, doacoes_outros_candidatos)
                    VALUES (%(candidato_titulo_eleitor)s, %(eleicao_ano)s, %(eleicao_nivel)s, %(limite_gastos)s, %(total_despesas_contratadas)s, %(total_despesas_pagas)s, %(doacoes_outros_candidatos)s)
                    ON CONFLICT (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel) DO UPDATE SET
                        limite_gastos = EXCLUDED.limite_gastos,
                        total_despesas_contratadas = EXCLUDED.total_despesas_contratadas,
                        total_despesas_pagas = EXCLUDED.total_despesas_pagas,
                        doacoes_outros_candidatos = EXCLUDED.doacoes_outros_candidatos
                """, parameters=resumo)
            
            logger.info(f"Carregados {len(resumos)} registros na tabela Resumo_Despesas")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar resumos de despesas: {e}")
            raise

    @task
    def load_doadores(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Doador usando chaves naturais
        """
        try:
            doadores = data['doadores']
            
            for doador in doadores:
                execute_sql("""
                    INSERT INTO Doador (cpf_cnpj, nome)
                    VALUES (%(cpf_cnpj)s, %(nome)s)
                    ON CONFLICT (cpf_cnpj) DO UPDATE SET
                        nome = EXCLUDED.nome
                """, parameters=doador)
            
            logger.info(f"Carregados {len(doadores)} registros na tabela Doador")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar doadores: {e}")
            raise

    @task
    def load_doacoes(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Doacao
        """
        try:
            doacoes = data['doacoes']
            
            for doacao in doacoes:
                
                try:
                    execute_sql("""
                        INSERT INTO Doacao (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, cpf_cnpj_doador, valor, nr_recibo_eleitoral, nr_documento, data, ds_receita, especie_recurso)
                        VALUES (%(candidato_titulo_eleitor)s, %(eleicao_ano)s, %(eleicao_nivel)s, %(cpf_cnpj_doador)s, %(valor)s, %(nr_recibo_eleitoral)s, %(nr_documento)s, %(data)s, %(ds_receita)s, %(especie_recurso)s)
                        ON CONFLICT (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, cpf_cnpj_doador, valor, data) DO UPDATE SET
                            nr_recibo_eleitoral = EXCLUDED.nr_recibo_eleitoral,
                            nr_documento = EXCLUDED.nr_documento,
                            ds_receita = EXCLUDED.ds_receita,
                            especie_recurso = EXCLUDED.especie_recurso
                    """, parameters=doacao)
                except Exception as e:
                    logger.warning(f"Erro ao inserir doação específica: {e}. Dados: {doacao}")
                    continue
            
            logger.info(f"Carregadas {len(doacoes)} registros na tabela Doacao")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar doações: {e}")
            raise

    @task
    def load_fornecedores(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Fornecedor usando chaves naturais
        """
        try:
            fornecedores = data['fornecedores']
            
            for fornecedor in fornecedores:
                execute_sql("""
                    INSERT INTO Fornecedor (cpf_cnpj, nome)
                    VALUES (%(cpf_cnpj)s, %(nome)s)
                    ON CONFLICT (cpf_cnpj) DO UPDATE SET
                        nome = EXCLUDED.nome
                """, parameters=fornecedor)
            
            logger.info(f"Carregados {len(fornecedores)} registros na tabela Fornecedor")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar fornecedores: {e}")
            raise

    @task
    def load_despesas(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Despesa
        """
        try:
            despesas = data['despesas']
            
            for despesa in despesas:
                
                try:
                    execute_sql("""
                        INSERT INTO Despesa (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, fornecedor_cpf_cnpj, tipo, descricao, valor, data, especie_documento_emitido)
                        VALUES (%(candidato_titulo_eleitor)s, %(eleicao_ano)s, %(eleicao_nivel)s, %(fornecedor_cpf_cnpj)s, %(tipo)s, %(descricao)s, %(valor)s, %(data)s, %(especie_documento_emitido)s)
                        ON CONFLICT (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, fornecedor_cpf_cnpj, valor, data) DO UPDATE SET
                            tipo = EXCLUDED.tipo,
                            descricao = EXCLUDED.descricao,
                            especie_documento_emitido = EXCLUDED.especie_documento_emitido
                    """, parameters=despesa)
                except Exception as e:
                    logger.warning(f"Erro ao inserir despesa específica: {e}. Dados: {despesa}")
                    continue
            
            logger.info(f"Carregadas {len(despesas)} registros na tabela Despesa")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar despesas: {e}")
            raise

    @task
    def load_notas_fiscais(data: Dict[str, List[Dict[str, Any]]]) -> bool:
        """
        Carrega dados da tabela Nota_Fiscal
        """
        try:
            notas = data['notas_fiscais']
            
            for nota in notas:
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
            
            logger.info(f"Carregadas {len(notas)} registros na tabela Nota_Fiscal")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar notas fiscais: {e}")
            raise

    @task
    def validate_data_quality() -> Dict[str, Any]:
        """
        Valida a qualidade dos dados carregados usando titulo_eleitor
        """
        try:
            # Contadores de registros
            counts = {}
            
            tables = [
                'Eleicao', 'Partido', 'Candidato', 'Candidatura', 
                'Bem', 'Resumo_Despesas', 'Doador', 'Doacao', 
                'Fornecedor', 'Despesa', 'Nota_Fiscal'
            ]
            
            for table in tables:
                result = get_first_result(f"SELECT COUNT(*) FROM {table}")
                counts[table] = result[0] if result else 0
            
            # Validações específicas
            validations = {}
            
            # Verificar se todos os candidatos têm candidatura
            result = get_first_result("""
                SELECT COUNT(*) FROM Candidato c 
                LEFT JOIN Candidatura ca ON c.titulo_eleitor = ca.candidato_titulo_eleitor 
                WHERE ca.candidato_titulo_eleitor IS NULL
            """)
            validations['candidatos_sem_candidatura'] = result[0] if result else 0
            
            # Verificar candidaturas com partidos inválidos
            result = get_first_result("""
                SELECT COUNT(*) FROM Candidatura ca 
                LEFT JOIN Partido p ON ca.partido_numero = p.numero 
                WHERE ca.partido_numero IS NOT NULL AND p.numero IS NULL
            """)
            validations['candidaturas_com_partido_invalido'] = result[0] if result else 0
            
            # Verificar consistência de dados
            result = get_first_result("""
                SELECT COUNT(*) FROM Candidatura ca
                WHERE ca.eleicao_ano = 2024 AND ca.eleicao_nivel = 'Municipal'
            """)
            validations['candidaturas_eleicao_2024'] = result[0] if result else 0
            
            logger.info("Validação de qualidade dos dados:")
            logger.info(f"Contadores por tabela: {counts}")
            logger.info(f"Validações: {validations}")
            
            return {
                'status': 'success',
                'counts': counts,
                'validations': validations,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro na validação de qualidade: {e}")
            raise

    # Definir o fluxo da DAG com chaves naturais
    raw_data = extract_json_data()
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
    resumos_loaded = load_resumos_despesas(transformed_data)
    doadores_loaded = load_doadores(transformed_data)
    fornecedores_loaded = load_fornecedores(transformed_data)
    
    bens_loaded.set_upstream(candidaturas_loaded)
    resumos_loaded.set_upstream(candidaturas_loaded)
    doadores_loaded.set_upstream(candidaturas_loaded)
    fornecedores_loaded.set_upstream(candidaturas_loaded)
    
    doacoes_loaded = load_doacoes(transformed_data)
    despesas_loaded = load_despesas(transformed_data)
    notas_loaded = load_notas_fiscais(transformed_data)
    
    doacoes_loaded.set_upstream([candidaturas_loaded, doadores_loaded])
    despesas_loaded.set_upstream([candidaturas_loaded, fornecedores_loaded])
    notas_loaded.set_upstream(candidaturas_loaded)
    
    # Validação final
    validation_result = validate_data_quality()
    validation_result.set_upstream([
        bens_loaded, resumos_loaded, doacoes_loaded, despesas_loaded, notas_loaded
    ])

# Instanciar a DAG
dag_instance = etl_candidatos_2024() 