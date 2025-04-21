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

## Criação da Tabela

```python
df = spark.read.csv("../data/cidades_brasileiras.csv", header=True, schema=schema)
df.write.format("delta").mode("overwrite").save("../output/delta/cidades")
spark.sql("CREATE TABLE IF NOT EXISTS cidades_delta USING DELTA LOCATION '../output/delta/cidades'")


```
 Os dados são lidos de um arquivo CSV, convertidos em DataFrame com schema definido e salvos no formato Delta Lake. A tabela cidades_delta é criada apontando para o diretório com os dados no formato Delta.

## INSERT

```python
nova_cidade = [Row(id=9999, cidade="Nova Esperança", estado="Paraná", sigla="PR", ibge=9999999, latitude=-23.5, longitude=-51.5)]
nova_df = spark.createDataFrame(nova_cidade)
nova_df.write.format("delta").mode("append").save("../output/delta/cidades")
```
Este comando adiciona uma nova linha à tabela Delta Lake, inserindo uma cidade fictícia no conjunto de dados existente.


## UPDATE

```sql
UPDATE cidades_delta SET cidade = 'Cidade Atualizada' WHERE id = 9999
```

Atualiza o valor do campo cidade para 'Cidade Atualizada' na linha onde o id é 9999. Isso demonstra como realizar atualizações em tabelas Delta usando comandos SQL.

## DELETE

```sql
DELETE FROM cidades_delta WHERE id = 9999
```

Remove a linha da tabela Delta onde o id é igual a 9999, demonstrando a operação de exclusão de dados em uma tabela Delta.

## Resultado Final

```python
df_final = spark.read.format("delta").load("../output/delta/cidades")
df_final.show()
```
Carrega novamente os dados da tabela Delta após as operações e exibe as primeiras linhas para verificar o estado final da tabela.