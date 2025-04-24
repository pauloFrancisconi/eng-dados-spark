
## Ferramentas utilizadas

- Python 3.11
- PySpark 3.5.3
- Delta Lake 3.2.0
- Apache Iceberg 0.5.0
- Poetry para gerenciamento do ambiente
- Jupyter Lab para os notebooks
- MkDocs para documentação

## Estrutura

- `notebooks/`: notebooks Jupyter com os testes e manipulação dos dados.
- `data/`: dataset utilizado (`cidades_brasileiras.csv`)
- `docs/`: documentação da aplicação e exemplos com Delta e Iceberg.
- `output/`: arquivos gerados com os dados processados.

## Dataset

O dataset contém dados públicos de cidades brasileiras.
### Estrutura dos dados CSV

O arquivo CSV contém as seguintes colunas:
- id
- cidade
- estado
- sigla
- ibge
- latitude
- longitude

localizado em `data/cidades_brasileiras.csv`.

## Conteúdo

- [Delta Lake](delta.md)
- [Apache Iceberg](iceberg.md)
