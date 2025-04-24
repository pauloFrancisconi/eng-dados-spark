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

## Configuração incial do APACHE ICEBERG

Faz a importação correta do pyspark para ativar a sessão SparkSession, além de configurar o builder da SparkSession com os packages necessários

```
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergExample")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "./output/iceberg-warehouse")  
    .getOrCreate()
)


```

## Criação do Schema

Definição do schema além da definiçõ dos tipos usados como Int, Double, String...

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
```

Após isso temos a leitura do csv para o dataframe
```
df = spark.read.csv("../data/cidades_brasileiras.csv", header=True, schema=schema)
df.show(5)
```

## Criação da Tabela

```
df.writeTo("local.cidades_iceberg").using("iceberg").createOrReplace()

```

## INSERT

```sql
spark.sql("""
    INSERT INTO local.cidades_iceberg VALUES
    (9999, 'Cidade Exemplo', 'Estado Exemplo', 'EX', 9999999, -10.1234, -50.5678)
""")

spark.sql("SELECT * FROM local.cidades_iceberg WHERE id = 9999").show()

```

Este comando adiciona uma nova linha à tabela Iceberg, inserindo uma cidade fictícia no conjunto de dados existente.

## UPDATE

```sql
spark.sql("UPDATE local.cidades_iceberg SET cidade = 'Cidade Atualizada' WHERE id = 9999")
spark.sql("SELECT * FROM local.cidades_iceberg WHERE id = 9999").show()
```

Atualiza o valor dos campos latitude e longitude na linha onde o id é 9999. Isso demonstra como realizar atualizações em tabelas Iceberg usando comandos SQL.

## DELETE

```sql
spark.sql("DELETE FROM local.cidades_iceberg WHERE id = 9999")
spark.sql("SELECT * FROM local.cidades_iceberg WHERE id = 9999").show()
```

Remove a linha da tabela Iceberg onde o id é igual a 9999, demonstrando a operação de exclusão de dados em uma tabela Iceberg.

## Resultado Final

```sql
df_final = spark.sql("SELECT * FROM local.cidades_iceberg")
df_final.show(5)
```

Carrega novamente os dados da tabela Iceberg em um dataframe final após as operações e exibe as primeiras linhas para verificar o estado final da tabela.
