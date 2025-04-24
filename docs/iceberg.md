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

## Modelo ER

A estrutura da tabela segue um modelo entidade-relacionamento simples, com uma única entidade `CIDADES`, cujos atributos são:

- `id`: Identificador único da cidade (chave primária)
- `cidade`: Nome da cidade
- `estado`: Nome do estado
- `sigla`: Sigla do estado
- `ibge`: Código IBGE da cidade
- `latitude`: Latitude geográfica
- `longitude`: Longitude geográfica

## Criação do Schema e import do dataset

```
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("sigla", StringType(), True),
    StructField("ibge", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])
df = spark.read.csv("../data/cidades_brasileiras.csv", header=True, schema=schema)
df.show(5)
```

## Criação da Tabela

```
df.writeTo("local.cidades_iceberg").using("iceberg").createOrReplace()

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
