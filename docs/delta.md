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

## Modelo ER

A estrutura da tabela segue um modelo entidade-relacionamento simples, com uma única entidade `CIDADES`, cujos atributos são:

- `id`: Identificador único da cidade (chave primária)
- `cidade`: Nome da cidade
- `estado`: Nome do estado
- `sigla`: Sigla do estado
- `ibge`: Código IBGE da cidade
- `latitude`: Latitude geográfica
- `longitude`: Longitude geográfica

## Configuração Inicial do DELTA

Faz a importação correta do pyspark para ativar a sessão SparkSession, além de importar os tipos de dados como String, Double, Int...

```
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from delta import *

import logging

logging.getLogger("py4j").setLevel(logging.DEBUG)
```
Logo após temos o builder inicial da Spark Session com as packages necessárias

```
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

## Criação do Schema e import do dataset
```python

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

df.write.format("delta").mode("overwrite").saveAsTable("cidades_delta")
spark.sql("DROP TABLE IF EXISTS cidades_delta")
spark.sql("CREATE TABLE cidades_delta USING DELTA LOCATION '../output/delta/cidades'")

```
 Os dados são lidos de um arquivo CSV, convertidos em DataFrame com schema definido e salvos no formato Delta Lake. A tabela cidades_delta é criada apontando para o diretório com os dados no formato Delta.

## INSERT

```python
spark.sql("""
    INSERT INTO cidades_delta VALUES
    (9999, 'Cidade Exemplo', 'Estado Exemplo', 'EX', 9999999, -10.1234, -50.5678)
""")

spark.sql("SELECT * FROM cidades_delta WHERE id = 9999").show()
```
Este comando adiciona uma nova linha à tabela Delta Lake, inserindo uma cidade fictícia no conjunto de dados existente.


## UPDATE

```python
spark.sql("""
    UPDATE cidades_delta
    SET latitude = -11.0000, longitude = -51.0000
    WHERE id = 9999
""")

spark.sql("SELECT * FROM cidades_delta WHERE id = 9999").show()
```

Atualiza o valor dos campos latitude e longitude na linha onde o id é 9999. Isso demonstra como realizar atualizações em tabelas Delta usando comandos SQL.

## DELETE

```python
spark.sql("""
    DELETE FROM cidades_delta
    WHERE id = 9999
""")

spark.sql("SELECT * FROM cidades_delta WHERE id = 9999").show()
```

Remove a linha da tabela Delta onde o id é igual a 9999, demonstrando a operação de exclusão de dados em uma tabela Delta.

## Resultado Final

```python
df_final = spark.read.format("delta").load("../output/delta/cidades")
df_final.show()
```
Carrega novamente os dados da tabela Delta após as operações e exibe as primeiras linhas para verificar o estado final da tabela.