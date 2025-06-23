# Projeto ETL - Open Brewery DB (Airflow)

## Visão Geral

## Introdução

O projeto **Breweries Data Pipeline** foi desenvolvido para demonstrar, na prática, os conceitos de arquitetura de dados moderna, com ênfase em pipelines ELT (Extract, Load, Transform) robustos, rastreáveis e escaláveis.

Utilizando dados públicos do [Open Brewery DB](https://www.openbrewerydb.org/), esta solução implementa todas as etapas essenciais de um pipeline analítico — desde a ingestão e tratamento de dados brutos (camadas Bronze, Silver e Gold) até a carga em um Data Warehouse relacional (PostgreSQL), culminando em um dashboard dinâmico no Power BI.

O pipeline está completamente orquestrado pelo **Apache Airflow** e foi concebido para ser facilmente executado em ambientes locais usando Docker, facilitando testes, evolução e deploy em ambientes reais de produção.

### Destaques do projeto

* **Padrão de Camadas (Bronze, Silver, Gold):** Facilita rastreabilidade, auditoria, reprocessamento e governança dos dados.
* **Data Warehouse relacional:** Modelo dimensional pronto para análises rápidas e históricas.
* **Automação e orquestração:** Todas as etapas controladas por DAGs no Airflow.
* **Qualidade de dados:** Tratamento de valores nulos, caracteres especiais e padronização já implementados nos scripts.
* **Dashboard pronto para consumo:** Power BI conectado ao DW com exemplos de análises.
* **Reprodutibilidade total:** Pipeline preparado para subir em minutos com Docker Compose.

Este repositório serve como base tanto para aprendizado quanto para aplicação direta em cenários reais de engenharia de dados, podendo ser expandido para outras fontes, outros domínios de negócio ou camadas de validação mais sofisticadas

Este projeto implementa um pipeline ETL completo para ingestão, transformação e carga de dados da API [Open Brewery DB](https://www.openbrewerydb.org/), utilizando Apache Airflow, Python e PostgreSQL. O fluxo cobre desde a ingestão bruta (bronze), passando por transformação (silver), particionamento (gold) e carga das dimensões e fatos em um Data Warehouse relacional.

## Arquitetura do Pipeline

O pipeline é composto por múltiplas camadas:

1. **Ingestão (Bronze)** : Coleta dados crus da API Open Brewery e armazena em arquivos JSON.
2. **Transformação (Silver)** : Limpa, normaliza e padroniza os dados, salvando em formato Parquet.
3. **Modelagem (Gold)** : Organiza os dados em Parquet particionado por país/estado, prontos para consumo analítico.
4. **Carga no Data Warehouse** : Popula tabelas dimensionais e fato em um banco relacional PostgreSQL.
5. **Consumo analítico** : Conecta o DW ao Power BI para criação de dashboards interativos
6. **Bronze**: Coleta e armazenamento dos dados brutos da API, no formato JSON.
7. **Silver**: Normalização, limpeza e padronização dos dados, convertendo para Parquet.
8. **Gold**: Particionamento dos dados por país/estado, prontos para ingestão no DW.
9. **DW**: Carregamento das dimensões e tabela fato no PostgreSQL.



**Diagrama Macro: Visão Geral da Arquitetura**

O diagrama abaixo representa o **fluxo macro** do pipeline de dados, partindo da ingestão da API pública até o consumo analítico em dashboards. Cada bloco representa uma etapa fundamental na arquitetura moderna de dados:

* **Bronze Layer:** Armazena dados brutos (raw) em formato JSON, garantindo rastreabilidade total.
* **Silver Layer:** Realiza limpeza, normalização e padronização, produzindo um Parquet único e consistente.
* **Gold Layer:** Estrutura o dado em Parquet já particionado, facilitando a leitura e a carga analítica por país/estado.
* **Data Warehouse:** Centraliza dados já prontos para análise, modelados em tabelas dimensionais e fato, em um banco relacional (PostgreSQL).
* **Power BI:** Consumidor dos dados do DW, onde são construídos dashboards e relatórios dinâmicos para insights de negócio.

![](air_flow/img/breweries_macro.png)



**Diagrama Micro: Orquestração no Airflow**


Este segundo diagrama detalha o microprocesso de execução no Airflow, mostrando cada tarefa (task) da DAG e suas dependências:

* **fetch_raw_data**: Inicia o processo baixando os dados brutos da API (bronze).
* **transform_and_partition**: Converte, limpa e normaliza os dados, salvando o resultado em Parquet (silver).
* **generate_gold_files**: Organiza os dados particionados prontos para carga (gold).
* **truncate_fact_breweries**: Antes de inserir novos dados, esvazia a tabela fato (garantindo consistência).
* **Truncates nas dimensões**: Esvazia as dimensões (location, type, name) para garantir carga limpa.
* **Loads das dimensões**: Popula cada dimensão a partir dos dados gold.
* **load_fact_breweries**: Por fim, executa o merge de dimensões e insere na tabela fato.

O encadeamento das tasks garante a ordem correta de execução e a integridade dos dados carregados no DW.


![](air_flow/img/breweries_micro.png)


## Data Warehouse

O Data Warehouse deste projeto foi modelado seguindo o padrão **estrela** (*star schema* ), facilitando consultas analíticas e integração com ferramentas de BI. A estrutura centraliza os dados na tabela de **fato** (ações das cervejarias) e utiliza dimensões para descrever atributos de localização, nome e tipo de cervejaria.

### Tabelas do DW

#### **Tabela Fato: `fact_breweries`**

Tabela central do DW, armazena eventos/contagens analíticas de cervejarias com as principais métricas e chaves estrangeiras das dimensões.

**Principais campos:**

* `fact_id`: Identificador único do fato.
* `location_id`: FK para a dimensão de localização.
* `brewery_type_id`: FK para o tipo de cervejaria.
* `brewery_name_id`: FK para o nome da cervejaria.
* `brewery_count`: Quantidade (usualmente 1, para granularidade por cervejaria).
* `has_website`: Flag indicando se a cervejaria possui website (1 = sim, 0 = não).
* `has_location`: Flag indicando se latitude e longitude estão presentes (1 = sim, 0 = não).
* `latitude`, `longitude`: Localização geográfica (adicionados conforme seu pedido).
* `created_at`: Data de carga do registro.

#### **Dimensões**

* **`dim_location`**

  Armazena cidades, estados e países das cervejarias, normalizando a granularidade de localização.

  * `location_id`, `city`, `state`, `country`
* **`dim_brewery_type`**

  Descreve o tipo de cervejaria (ex: micro, brewpub, large).

  * `brewery_type_id`, `brewery_type`
* **`dim_brewery_name`**

  Centraliza nomes distintos de cervejarias para evitar redundância textual.

  * `brewery_name_id`, `brewery_name`

### Relacionamento entre tabelas

O relacionamento segue a lógica:

* A **tabela fato** faz referência (FK) às três dimensões, garantindo integridade e facilitando filtros/agrupamentos.
* Cada dimensão pode ser compartilhada por múltiplos registros da fato.

![](air_flow/img/dw.png)


## Como executar

1. **Clone o projeto e suba o ambiente Docker:**

   git clone https://github.com/renatoweb76/test_ab_inbev.git
   cd seurepo
   docker compose up -d
2. **Acesse o Airflow:**

http://localhost:8080
usuário/senha padrão: admin/QSSXNyCFpanp3cMK

2.1. Execute a DAG breweries_etl_full
2.1.1. Ela executa toda a pipeline (bronze → silver → gold → carga no DW).

3. **Acesse o Banco de Dados Postgres**
4. Baixe o Dbever Community: https://dbeaver.io/download/
5. Crie uma nova conexão com o Database breweries_dw
6. Utilize o usuário/senha airflow/airflow
7. Abara o Schema dw e veja as tabelas criadas.

## Estrutura dos scripts:

1. bronze_layer_full.py:              Busca dados brutos da API e salva em JSON.
2. silver_layer_full.py:              Limpa, normaliza e salva em Parquet único.
3. gold_layer_full.py:                Particiona Parquet por país/estado.
4. load_dw/load_dim_location.py:      Carrega dimensão de localização.
5. load_dw/load_dim_brewery_type.py:  Carrega dimensão de tipo de cervejaria.
6. load_dw/load_dim_brewery_name.py:  Carrega dimensão de nome de cervejaria.
7. load_dw/load_fact_breweries.py:    Carrega tabela fato principal.

## Qualidade de Dados e Validações:

1. Caracteres inválidos tratados a partir da silver layer.
2. Checagem de FKs inválidas e descartes na carga da fato.
3. Prints, logs e mensagens em todas as etapas para debugging.

## Dashboard Power BI

Um dashboard em Power BI foi criado para visualizar as principais métricas extraídas do Data Warehouse, incluindo:

- Número de cervejarias por país e estado
- Tipos mais comuns de cervejarias
- Outras análises relevantes

### Como acessar

* O arquivo .pbix está disponível na pasta `/dashboard`
* Exemplos de visualizações:
