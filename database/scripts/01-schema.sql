-- Tabela para armazenar informações sobre as eleições
CREATE TABLE Eleicao (
    ano INT NOT NULL,
    nivel VARCHAR(255) NOT NULL,
    PRIMARY KEY (ano, nivel)
);

-- Tabela para armazenar informações sobre os partidos políticos  
CREATE TABLE Partido (
    numero INT PRIMARY KEY,
    sigla VARCHAR(50),
    nome VARCHAR(255)
);

-- Tabela para armazenar informações sobre os candidatos
CREATE TABLE Candidato (
    titulo_eleitor VARCHAR(12) PRIMARY KEY,
    cpf VARCHAR(11),
    nome VARCHAR(255) NOT NULL,
    data_nasc DATE,
    sexo VARCHAR(50)
);

-- Tabela central que representa a candidatura de um candidato em uma eleição por um partido
CREATE TABLE Candidatura (
    candidato_titulo_eleitor VARCHAR(12) NOT NULL,
    eleicao_ano INT NOT NULL,
    eleicao_nivel VARCHAR(255) NOT NULL,
    partido_numero INT,
    cargo VARCHAR(255),
    num_urna INT,
    situacao_atual VARCHAR(255),
    cidade VARCHAR(255),
    estado VARCHAR(2),
    PRIMARY KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel),
    FOREIGN KEY (candidato_titulo_eleitor) REFERENCES Candidato(titulo_eleitor),
    FOREIGN KEY (partido_numero) REFERENCES Partido(numero),
    FOREIGN KEY (eleicao_ano, eleicao_nivel) REFERENCES Eleicao(ano, nivel)
);

-- Tabela para os bens declarados por uma candidatura
CREATE TABLE Bem (
    candidato_titulo_eleitor VARCHAR(12) NOT NULL,
    eleicao_ano INT NOT NULL,
    eleicao_nivel VARCHAR(255) NOT NULL,
    ordem INT NOT NULL,
    descricao_tipo VARCHAR(255),
    descricao TEXT,
    valor DECIMAL(15, 2),
    data_atualizacao TIMESTAMP,
    PRIMARY KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, ordem),
    FOREIGN KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel) 
        REFERENCES Candidatura(candidato_titulo_eleitor, eleicao_ano, eleicao_nivel)
);

-- Tabela para armazenar resumo das despesas por candidatura
CREATE TABLE Resumo_Despesas (
    candidato_titulo_eleitor VARCHAR(12) NOT NULL,
    eleicao_ano INT NOT NULL,
    eleicao_nivel VARCHAR(255) NOT NULL,
    limite_gastos DECIMAL(15, 2),
    total_despesas_contratadas DECIMAL(15, 2),
    total_despesas_pagas DECIMAL(15, 2),
    doacoes_outros_candidatos DECIMAL(15, 2),
    PRIMARY KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel),
    FOREIGN KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel) 
        REFERENCES Candidatura(candidato_titulo_eleitor, eleicao_ano, eleicao_nivel)
);

-- Tabela para armazenar informações sobre doadores
CREATE TABLE Doador (
    cpf_cnpj VARCHAR(14) PRIMARY KEY,
    nome VARCHAR(255)
);

-- Tabela para armazenar doações recebidas por uma candidatura
CREATE TABLE Doacao (
    candidato_titulo_eleitor VARCHAR(12) NOT NULL,
    eleicao_ano INT NOT NULL,
    eleicao_nivel VARCHAR(255) NOT NULL,
    cpf_cnpj_doador VARCHAR(14) NOT NULL,
    valor DECIMAL(15, 2),
    nr_recibo_eleitoral VARCHAR(255),
    nr_documento VARCHAR(255),
    data DATE,
    ds_receita TEXT,
    especie_recurso VARCHAR(255),
    PRIMARY KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, cpf_cnpj_doador, valor, data),
    FOREIGN KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel) 
        REFERENCES Candidatura(candidato_titulo_eleitor, eleicao_ano, eleicao_nivel),
    FOREIGN KEY (cpf_cnpj_doador) REFERENCES Doador(cpf_cnpj)
);

-- Tabela para armazenar informações sobre fornecedores
CREATE TABLE Fornecedor (
    cpf_cnpj VARCHAR(14) PRIMARY KEY,
    nome VARCHAR(255)
);

-- Tabela para armazenar despesas de uma candidatura
CREATE TABLE Despesa (
    candidato_titulo_eleitor VARCHAR(12) NOT NULL,
    eleicao_ano INT NOT NULL,
    eleicao_nivel VARCHAR(255) NOT NULL,
    fornecedor_cpf_cnpj VARCHAR(14) NOT NULL,
    tipo VARCHAR(255),
    descricao TEXT,
    valor DECIMAL(15, 2),
    data DATE,
    especie_documento_emitido VARCHAR(255),
    PRIMARY KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, fornecedor_cpf_cnpj, valor, data),
    FOREIGN KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel) 
        REFERENCES Candidatura(candidato_titulo_eleitor, eleicao_ano, eleicao_nivel),
    FOREIGN KEY (fornecedor_cpf_cnpj) REFERENCES Fornecedor(cpf_cnpj)
);

-- Tabela para notas fiscais
CREATE TABLE Nota_Fiscal (
    candidato_titulo_eleitor VARCHAR(12) NOT NULL,
    eleicao_ano INT NOT NULL,
    eleicao_nivel VARCHAR(255) NOT NULL,
    numero_nota VARCHAR(255) NOT NULL,
    serie VARCHAR(10),
    cnpj_emitente VARCHAR(14),
    valor DECIMAL(15, 2),
    data_emissao DATE,
    chave_acesso VARCHAR(255),
    url_acesso TEXT,
    ue VARCHAR(10),
    unidade_arrecadadora VARCHAR(255),
    PRIMARY KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel, numero_nota, serie),
    FOREIGN KEY (candidato_titulo_eleitor, eleicao_ano, eleicao_nivel) 
        REFERENCES Candidatura(candidato_titulo_eleitor, eleicao_ano, eleicao_nivel)
);

-- Índices para otimizar consultas comuns
CREATE INDEX idx_candidato_cpf ON Candidato(cpf);
CREATE INDEX idx_candidatura_partido ON Candidatura(partido_numero);
CREATE INDEX idx_candidatura_cidade ON Candidatura(cidade, estado);
CREATE INDEX idx_bem_valor ON Bem(valor);
CREATE INDEX idx_doacao_valor ON Doacao(valor);
CREATE INDEX idx_despesa_valor ON Despesa(valor);
CREATE INDEX idx_despesa_tipo ON Despesa(tipo);
CREATE INDEX idx_nota_fiscal_valor ON Nota_Fiscal(valor);
CREATE INDEX idx_nota_fiscal_emitente ON Nota_Fiscal(cnpj_emitente); 