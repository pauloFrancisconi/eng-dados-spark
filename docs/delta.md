# Delta Lake

Este documento descreve a criação e manipulação de uma tabela Delta Lake com base no dataset `cidades_brasileiras.csv`.

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
df = spark.read.csv("../data/cidades_brasileiras.csv", header=True, schema=schema)
df.write.format("delta").mode("overwrite").save("../output/delta/cidades")
spark.sql("CREATE TABLE IF NOT EXISTS cidades_delta USING DELTA LOCATION '../output/delta/cidades'")
```

## INSERT

```python
nova_cidade = [Row(id=9999, cidade="Nova Esperança", estado="Paraná", sigla="PR", ibge=9999999, latitude=-23.5, longitude=-51.5)]
nova_df = spark.createDataFrame(nova_cidade)
nova_df.write.format("delta").mode("append").save("../output/delta/cidades")
```

## UPDATE

```sql
UPDATE cidades_delta SET cidade = 'Cidade Atualizada' WHERE id = 9999
```

## DELETE

```sql
DELETE FROM cidades_delta WHERE id = 9999
```

## Resultado Final

```python
df_final = spark.read.format("delta").load("../output/delta/cidades")
df_final.show()
```
