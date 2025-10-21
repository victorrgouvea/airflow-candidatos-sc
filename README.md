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

## Estrutura do Projeto

```
airflow/
├── dags/
│   ├── data/candidatos_sc_2024.json    # Dados de entrada
│   ├── etl_candidatos_dag.py          # DAG principal ETL
│   ├── setup_database_dag.py         # DAG de configuração
│   └── database_setup.py              # Funções de banco
├── database/scripts/01-schema.sql     # Schema do banco
├── docker-compose.yml                 # Configuração Docker
├── .env                              # Variáveis de ambiente
└── requirements.txt                   # Dependências Python
```
