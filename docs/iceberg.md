# Apache Iceberg

Este documento descreve a criação e manipulação de uma tabela Iceberg com base no dataset `cidades_brasileiras.csv`.

## Dataset

O arquivo CSV contém as seguintes colunas:
- id
- cidade
- estado
- sigla
- ibge
- latitude
- longitude

localizado em `data/cidades_brasileiras.csv`.

## Criação da Tabela

```python
df.writeTo("spark_catalog.default.cidades_iceberg").using("iceberg").createOrReplace()
```

## INSERT

```sql
INSERT INTO spark_catalog.default.cidades_iceberg VALUES
(9999, 'Nova Esperança', 'Paraná', 'PR', 9999999, -23.5, -51.5)
```

## UPDATE

```sql
UPDATE spark_catalog.default.cidades_iceberg SET cidade = 'Cidade Atualizada' WHERE id = 9999
```

## DELETE

```sql
DELETE FROM spark_catalog.default.cidades_iceberg WHERE id = 9999
```

## Resultado Final

```sql
SELECT * FROM spark_catalog.default.cidades_iceberg
```
