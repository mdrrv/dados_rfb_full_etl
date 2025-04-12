# Dados Públicos CNPJ

- **Fonte oficial da Receita Federal do Brasil:** [Acesso aos dados](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/)  
- **Layout dos arquivos:** [Ver layout](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf)

A Receita Federal do Brasil disponibiliza bases com os dados públicos do Cadastro Nacional de Pessoas Jurídicas (CNPJ). Em geral, essas bases contêm as mesmas informações exibidas na consulta individual (cartão do CNPJ), acrescidas de dados relativos a Simples Nacional, sócios e outras informações que permitem análises aprofundadas — desde estudos econômicos até investigações mercadológicas.

Neste repositório está presente um processo de ETL (Extração, Transformação e Carga) que realiza as seguintes etapas:

1. **Download dos Arquivos:**  
   O script identifica automaticamente a pasta com a data mais recente (ex.: `2025-03/`) na fonte oficial e baixa os arquivos compactados (.zip).

2. **Extração dos Dados:**  
   Após o download, os arquivos .zip são descompactados para um diretório configurado, preparando o ambiente para o processamento.

3. **Leitura, Tratamento e Inserção dos Dados:**  
   Os dados extraídos são lidos, transformados — com ajustes de tipos, limpeza e padronização — e, em seguida, inseridos em um banco de dados PostgreSQL. Caso as tabelas já existam, elas serão truncadas para atualizar os dados sem modificar a estrutura e os índices previamente definidos.

4. **Otimização e Estruturação:**  
   Após a carga dos dados, os scripts configuram índices (especialmente na coluna `cnpj_basico`) para otimizar as consultas e manter a integridade relacional entre as tabelas.

---

## Infraestrutura Necessária

- [Python 3.8](https://www.python.org/downloads/release/python-3810/)
- [PostgreSQL 14.2](https://www.postgresql.org/download/)

---

## Como Utilizar

### 1. Preparação do Banco de Dados
- Instale o PostgreSQL e inicie o servidor (local ou remoto).  
- Crie o banco de dados conforme as instruções do arquivo `banco_de_dados.sql` incluído no repositório.

### 2. Configuração do Ambiente
- Crie um arquivo `.env` no diretório `code` (ou renomeie o arquivo `.env_template`) definindo as seguintes variáveis:
  - **OUTPUT_FILES_PATH:** Caminho para o diretório de destino dos arquivos baixados.
  - **EXTRACTED_FILES_PATH:** Caminho para o diretório onde os arquivos .zip serão descompactados.
  - **DB_USER:** Usuário do banco de dados.
  - **DB_PASSWORD:** Senha do usuário do banco.
  - **DB_HOST:** Endereço do servidor do banco de dados.
  - **DB_PORT:** Porta de conexão com o banco de dados.
  - **DB_NAME:** Nome do banco de dados (ex.: `Dados_RFB` conforme o arquivo `banco_de_dados.sql`).
  - **DB_SCHEMA:** Nome do schema (ex.: `Dados_RFB` conforme o arquivo `banco_de_dados.sql`).

### 3. Instalação das Dependências
Execute o seguinte comando no terminal para instalar todas as bibliotecas necessárias, conforme definido em `requirements.txt`:

`pip install -r requirements.txt`

---

### 4. Executando o Processo ETL
- **Script Principal:**  
  Para iniciar o processo ETL – que inclui o download dos arquivos, extração, transformação e carga no banco de dados – execute o script principal:

  `python etl_data.py

- **Script de Teste e Validação:**
Caso queira realizar testes de download, extração e carga de forma isolada, utilize o script ```bash testing.py´´´:

`python testing.py`

- **Verificação de Conexão:**
Para confirmar que a conexão com o PostgreSQL está funcionando corretamente, execute o script de teste:

`python test_conn.py`

---

## Estrutura dos Arquivos e Diretórios

A estrutura do projeto está organizada da seguinte forma:

```bash
.
├── .env                         # Arquivo de variáveis de ambiente (paths, dados do BD e schema)
├── etl_data.py                  # Script principal do processo ETL
├── test_conn.py                 # Script para testar a conexão com o PostgreSQL
├── testing.py                   # Script para testes de download, extração e carga de dados
├── requirements.txt             # Dependências do projeto
├── sql
│   ├── create_database.sql      # Script SQL para criação da base de dados
│   └── exemple_query.sql        # Exemplo de consulta SQL para análise dos dados
└── README.md                    # Este arquivo
```

-**env:**
Contém as variáveis de ambiente que definem os caminhos para os diretórios de saída e extração, além dos dados de acesso ao banco de dados.

-**etl_data.py:**
Realiza todo o fluxo ETL: baixa os arquivos, extrai o conteúdo, processa os dados e os insere no banco de dados (cria ou trunca as tabelas conforme necessário).

-**test_conn.py:**
Verifica a conexão com o PostgreSQL utilizando as configurações do arquivo .env.

-**testing.py:**
Executa testes para o download, extração e carga dos dados, servindo como um ambiente de validação do processo.

-**sql/create_database.sql:**
Script com as instruções para criação da base de dados, conforme a estrutura necessária para este projeto.

-**sql/exemple_query.sql:**
Um exemplo de consulta SQL para unir e transformar os dados do CNPJ, demonstrando o potencial de análise das informações carregadas.

---

---

## Tabelas Criadas

Durante a execução do processo ETL, os dados são organizados e inseridos em diversas tabelas no banco de dados PostgreSQL. Cada tabela representa um aspecto dos dados públicos do CNPJ e foi criada automaticamente (ou truncada para atualização) pelo método `to_sql`. A seguir, uma descrição de cada uma:

### 1. Tabela `empresa`
Contém os dados básicos das empresas.
- **Colunas:**  
  - `cnpj_basico` – Identificador principal do CNPJ (sem dígitos verificadores).  
  - `razao_social` – Nome ou razão social da empresa.  
  - `natureza_juridica` – Código que indica a natureza jurídica.  
  - `qualificacao_responsavel` – Qualificação do responsável pela empresa.  
  - `capital_social` – Valor do capital social, tratado com conversão de vírgula para ponto e para o tipo float.  
  - `porte_empresa` – Indicador do porte da empresa.  
  - `ente_federativo_responsavel` – Órgão federativo responsável (quando aplicável).

### 2. Tabela `estabelecimento`
Armazena informações detalhadas dos estabelecimentos (filiais/matrizes) das empresas.
- **Colunas:**  
  - `cnpj_basico` – Identificador do CNPJ da empresa.  
  - `cnpj_ordem`, `cnpj_dv` – Componentes do CNPJ para estabelecimento.  
  - `identificador_matriz_filial` – Indicador que diferencia matriz e filial.  
  - `nome_fantasia` – Nome fantasia do estabelecimento.  
  - `situacao_cadastral` – Situação cadastral atual.  
  - `data_situacao_cadastral` – Data em que a situação foi registrada.  
  - `motivo_situacao_cadastral` – Motivo da situação cadastrada.  
  - `nome_cidade_exterior`, `pais` – Informações para estabelecimentos fora do Brasil.  
  - `data_inicio_atividade` – Data de início de atividade.  
  - `cnae_fiscal_principal` – Código do CNAE fiscal principal.  
  - `cnae_fiscal_secundaria` – Códigos CNAE secundários (podem ser múltiplos).  
  - Campos adicionais: `tipo_logradouro`, `logradouro`, `numero`, `complemento`, `bairro`, `cep`, `uf`, `municipio`, `ddd_1`, `telefone_1`, `ddd_2`, `telefone_2`, `ddd_fax`, `fax`, `correio_eletronico`, `situacao_especial` e `data_situacao_especial`.

### 3. Tabela `socios`
Contém informações sobre os sócios das empresas.
- **Colunas:**  
  - `cnpj_basico` – Relaciona o sócio com a empresa.  
  - `identificador_socio` – Identifica de forma única o sócio.  
  - `nome_socio_razao_social` – Nome ou razão social do sócio.  
  - `cpf_cnpj_socio` – CPF ou CNPJ do sócio.  
  - `qualificacao_socio` – Código de qualificação do sócio.  
  - `data_entrada_sociedade` – Data de entrada do sócio na empresa.  
  - `pais` – País do sócio.  
  - `representante_legal` – Indica se o sócio é representante legal.  
  - `nome_do_representante` – Nome do representante, se aplicável.  
  - `qualificacao_representante_legal` – Qualificação do representante legal.  
  - `faixa_etaria` – Faixa etária do sócio.

### 4. Tabela `simples`
Armazena informações relativas à adesão das empresas ao regime do Simples Nacional.
- **Colunas:**  
  - `cnpj_basico`  
  - `opcao_pelo_simples` – Indica se a empresa optou pelo Simples.  
  - `data_opcao_simples` – Data da opção pelo Simples.  
  - `data_exclusao_simples` – Data de exclusão do Simples (se aplicável).  
  - `opcao_mei` – Indica se a empresa optou pelo MEI.  
  - `data_opcao_mei` – Data da opção pelo MEI.  
  - `data_exclusao_mei` – Data de exclusão do MEI (se aplicável).

### 5. Tabela `cnae`
Contém as classificações de atividades econômicas segundo o CNAE.
- **Colunas:**  
  - `codigo` – Código do CNAE.  
  - `descricao` – Descrição detalhada da atividade.

### 6. Tabela `moti`
Armazena os motivos associados a alterações ou situações cadastrais.
- **Colunas:**  
  - `codigo` – Código do motivo.  
  - `descricao` – Descrição do motivo.

### 7. Tabela `munic`
Contém informações sobre os municípios.
- **Colunas:**  
  - `codigo` – Código do município.  
  - `descricao` – Nome ou descrição do município.

### 8. Tabela `natju`
Armazena os tipos de natureza jurídica.
- **Colunas:**  
  - `codigo` – Código da natureza jurídica.  
  - `descricao` – Descrição da natureza jurídica.

### 9. Tabela `pais`
Contém dados sobre países.
- **Colunas:**  
  - `codigo` – Código do país.  
  - `descricao` – Nome do país.

### 10. Tabela `quals`
Armazena as qualificações dos sócios.
- **Colunas:**  
  - `codigo` – Código que indica a qualificação.  
  - `descricao` – Descrição da qualificação.

---

Cada uma dessas tabelas é tratada de forma modular no processo ETL, o que permite atualizar os dados de forma incremental e manter um histórico consistente na base. A criação (ou truncamento) e a carga dos dados são feitas utilizando o método `to_sql` do Pandas, que cria as tabelas automaticamente se elas não existirem. Após a inserção dos dados, os índices são configurados para otimizar as consultas baseadas nas colunas mais utilizadas, como `cnpj_basico`.

Esta organização facilita análises futuras e integra diversos aspectos dos dados públicos do CNPJ, permitindo cruzamentos e relatórios que abrangem desde informações empresariais até dados cadastrais dos sócios e classificações de atividades econômicas.

---

## Exemplo de consulta SQL

O arquivo `bash sql/exemple_query.sql` contém um exemplo de como extrair e transformar os dados.
Segue um trecho ilustrativo:

```bash
WITH rfb_dataset AS (
  SELECT
    es.cnpj,
    emp.razao_social,
    es.sigla_uf,
    es.id_municipio,
    mun.nome AS nome_municipio,
    es.cnae_fiscal_principal,
    cnae.descricao AS descricao_atividade_principal,
    SPLIT(es.cnae_fiscal_secundaria) AS cnae_secundario,
    emp.data AS data_info
  FROM empresas emp
  LEFT JOIN estabelecimentos es
    ON emp.cnpj_basico = es.cnpj_basico
  LEFT JOIN cnae cnae
    ON es.cnae_fiscal_principal = cnae.codigo
  LEFT JOIN munic mun
    ON es.id_municipio = mun.codigo
  WHERE emp.data = '2023-05-18'
    AND es.data = '2023-05-18'
    AND (razao_social LIKE 'COPEL%')
)
SELECT *
FROM rfb_dataset;
```

---
Desenvolvido por [Vinicius Madureira](http://linkedin.com/in/madureirav/)
© 2025