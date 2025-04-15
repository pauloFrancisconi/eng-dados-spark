# Projeto: Apache Spark com Delta Lake e Apache Iceberg

Este repositório contém um projeto de demonstração de uso das tecnologias Apache Spark, Delta Lake e Apache Iceberg aplicado ao dataset público de cidades brasileiras.

## Alunos

- Gabriel Guzzatti
- Paulo Francisconi
- Gabriel Milano

## Professor Orientador

- Prof.° Jorge Luiz Da Silva

## Requisitos

- Python 3.12
- [Poetry](https://python-poetry.org/docs/#installation)
- Java 8 ou superior
- Jupyter Lab

## Setup do Ambiente

### 1. Clone o repositório

```bash
git clone https://github.com/pauloFrancisconi/eng-dados-spark.git
cd eng-dados-spark
```

### 2. Instale o Poetry (caso não tenha)

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### 3. Instale as dependências

```bash
poetry install
```

### 4. Ative o ambiente

```bash
poetry shell
```

### 5. Inicie o Jupyter Lab

```bash
jupyter lab
```

## 📁 Estrutura do Projeto

```
.
├── data/                    # Dataset cidades_brasileiras.csv
├── docs/                   # Documentação em Markdown para o MkDocs
├── notebooks/              # Notebooks do projeto (Delta e Iceberg)
├── output/                 # Tabelas geradas pelo Spark
├── poetry.lock
├── pyproject.toml
├── spark_session.py        # Função utilitária para iniciar sessão Spark
└── README.md
```

## Dataset

Usamos o dataset **Cidades Brasileiras** do Kaggle:

[Kaggle - Cidades Brasileiras](https://www.kaggle.com/datasets/gilbertotrindade/cidades-brasileiras)

Colunas:
- id
- cidade
- estado
- sigla
- ibge
- latitude
- longitude

## Notebooks

- `notebooks/delta_lake.ipynb`: Criação da tabela Delta + INSERT, UPDATE, DELETE
- `notebooks/iceberg.ipynb`: Criação da tabela Iceberg + INSERT, UPDATE, DELETE
- `notebooks/spark_init.ipynb`: Inicialização da sessão Spark

## Documentação

Gerada com MkDocs a partir dos arquivos `.md` em `docs/`.
