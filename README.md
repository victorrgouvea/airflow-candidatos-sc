# ETL de Candidatos SC

Sistema de ETL (Extract, Transform, Load) usando Apache Airflow para processar dados de candidatos das eleições federais e municipais de Santa Catarina em 2022 e 2024.

## Sobre o Projeto

Este projeto foi desenvolvido para automatizar o processamento de dados eleitorais das eleições de Santa Catarina. O sistema captura informações completas sobre candidatos, suas candidaturas, bens declarados, doações recebidas e despesas de campanha, organizando tudo em um banco de dados estruturado para facilitar análises e consultas.

### O que foi desenvolvido

Criamos um sistema automatizado que processa dados eleitorais em formato JSON e CSV e os transforma em um banco de dados relacional bem estruturado. O sistema inclui validações automáticas para garantir a integridade dos dados, como verificação de campos obrigatórios, tratamento de valores inválidos e controle de duplicatas.

O projeto utiliza containers Docker para garantir que funcione de forma consistente em diferentes ambientes, com banco de dados PostgreSQL para armazenamento e Redis para coordenação das tarefas.

## Como executar

### Pré-requisitos

- Docker e Docker Compose instalados
- Porta 8080 disponível

### Inicializar o projeto

```bash
# Criar diretórios necessários
mkdir -p airflow/logs airflow/plugins airflow/config

# Inicializar Airflow
docker compose up airflow-init

# Iniciar todos os serviços
docker compose up -d
```

### Acessar o sistema

- **Interface Web**: http://localhost:8080
- **Usuário**: `airflow`
- **Senha**: `airflow`
- **Banco PostgreSQL**: localhost:5434 (usuário: `airflow`, senha: `airflow`, database: `candidatos_sc`)

### Configuração inicial

Após iniciar os serviços, é necessário executar a DAG `setup_database` **antes de executar qualquer outra DAG**:

1. Acesse a interface web do Airflow: http://localhost:8080
2. Faça login com usuário e senha
3. Localize a DAG `setup_database` na lista de DAGs
4. Ative a DAG (toggle switch) e execute-a manualmente
5. Aguarde a conclusão das tarefas `setup_database` e `test_connection`

**O que a DAG `setup_database` faz:**

- Cria o banco de dados `candidatos_sc` se não existir
- Cria a conexão `postgres_candidatos` no Airflow
- Testa a conectividade com o banco de dados

## Estrutura do Projeto

```
.
├── dags/
│   ├── data/                          # Dados de entrada
│   │   ├── candidatos_sc_2024.json   # Dados JSON 2024
│   │   ├── bens/                      # Dados de bens dos candidatos
│   │   ├── cand/                      # Dados de candidatos
│   │   └── nota_fiscal/               # Dados de notas fiscais
│   ├── etl_candidatos_dag.py          # DAG principal ETL (JSON)
│   ├── etl_candidatos_csv_2022_dag.py # DAG ETL para CSV 2022
│   ├── setup_database_dag.py          # DAG de configuração do banco
│   └── database_setup.py              # Funções de banco e conexão
├── airflow/
│   ├── logs/                          # Logs do Airflow
│   ├── plugins/                        # Plugins customizados
│   └── config/                         # Configurações
├── database/scripts/01-schema.sql      # Schema do banco
├── docker-compose.yml                  # Configuração Docker
├── .env                                # Variáveis de ambiente
└── requirements.txt                    # Dependências Python
```
