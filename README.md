# Trabalho – Pipeline de Dados com PySpark

## Descrição

Este repositório contém a solução final do trabalho de Data Engineering em PySpark. O projeto lê dados de pedidos e pagamentos, aplica regras de negócio específicas e gera um relatório Parquet com os pedidos de 2025 que tiveram pagamento recusado e avaliação de fraude legítima.

## Escopo atendido

O relatório final contém exatamente os atributos exigidos pelo trabalho:

1. `id_pedido`
2. `uf`
3. `forma_pagamento`
4. `valor_total`
5. `data_criacao`

Regras de negócio aplicadas:

- `status = false` (pagamento recusado)
- `avaliacao_fraude.fraude = false` (fraude legítima)
- pedidos do ano de 2025
- ordenação por `uf`, `forma_pagamento` e `data_criacao`
- saída em formato Parquet

## Estrutura do projeto

```text
trabalho-data-engineering-pyspark/
├── config/
│   └── settings.yaml
├── data/
│   ├── input/
│   │   ├── datasets-csv-pedidos/
│   │   └── datasets-json-pagamentos/
│   └── output/
├── src/
│   ├── config/
│   │   └── settings.py
│   ├── io_utils/
│   │   └── data_handler.py
│   ├── pipeline/
│   │   └── pipeline.py
│   ├── processing/
│   │   └── transformations.py
│   ├── session/
│   │   └── spark_session.py
│   └── main.py
├── tests/
│   ├── unit/
│   └── integration/
├── pyproject.toml
├── requirements.txt
└── MANIFEST.in
```

## Arquitetura aplicada

O projeto foi desenvolvido seguindo os critérios do trabalho:

- schemas explícitos em todas as leituras de DataFrame;
- classes e módulos separados por responsabilidade;
- injeção de dependências via `main.py`;
- configuração centralizada em `config/settings.yaml`;
- logging configurado no fluxo principal;
- tratamento de erros com `try/except` e logs.

## Pacotes e classes principais

- `src/main.py`
  - aggregation root do projeto
  - configura o `logging` global com `logging.basicConfig`
  - instancia `Config`, `SparkSessionManager`, `DataHandler`, `Transformation` e `Pipeline`
  - injeta as dependências no `Pipeline`
  - usa `try/except` para tratamento de erros críticos e encerra a sessão Spark no `finally`

- `src/config/settings.py`
  - classe `Config`
  - carrega o arquivo `config/settings.yaml` usando `yaml.safe_load`
  - expõe os valores de configuração com `__getitem__`
  - permite centralizar caminhos de entrada, saída e opções de leitura

- `src/session/spark_session.py`
  - classe `SparkSessionManager`
  - método estático `get_spark_session(app_name)`
  - cria a sessão Spark com `.master("local[*]")`
  - mantém a criação de sessão isolada do fluxo de negócio

- `src/io_utils/data_handler.py`
  - classe `DataHandler`
  - método `_get_schema_pedidos()` define schema explícito para CSV de pedidos
  - método `_get_schema_pagamentos()` define schema explícito para JSON de pagamentos
  - `load_pedidos(...)` lê pedidos com `spark.read.csv(schema=...)`, `header`, `sep` e `compression`
  - `load_pagamentos(...)` lê pagamentos com `spark.read.json(schema=...)`
  - `write_parquet(df, path)` salva DataFrames em Parquet, checando se o DataFrame está vazio
  - tratamento de erro com `AnalysisException` e `Py4JJavaError`

- `src/processing/transformations.py`
  - classe `Transformation`
  - `add_valor_total_pedidos(pedidos_df)` adiciona coluna `valor_total`
  - `join_pedidos_pagamentos(pedidos_df, pagamentos_df)` faz join entre pedidos e pagamentos por `id_pedido`
  - `get_top_10_pedido_cliente(pedidos_df)` calcula top 10 clientes por valor total
  - `gerar_relatorio_pagamentos_recusados_legitimos(pedidos_pagamentos_df)` aplica filtros de negócio:
    - `status == False`
    - `avaliacao_fraude.fraude == False`
    - `year(data_criacao) == 2025`
    - seleciona apenas as colunas do relatório
    - ordena por `uf`, `forma_pagamento` e `data_criacao`
  - usa `logging` e `try/except` para registrar e relançar erros

- `src/pipeline/pipeline.py`
  - classe `Pipeline`
  - método `run(config)` orquestra o fluxo completo:
    1. lê `pedidos` usando `DataHandler.load_pedidos`
    2. adiciona `valor_total` via `Transformation.add_valor_total_pedidos`
    3. lê `pagamentos` usando `DataHandler.load_pagamentos`
    4. junta os DataFrames com `Transformation.join_pedidos_pagamentos`
    5. escreve o resultado de pedidos+pagamentos em `data/output/pedidos_pagamentos`
    6. gera o relatório final com `Transformation.gerar_relatorio_pagamentos_recusados_legitimos`
    7. escreve o relatório final em `data/output/relatorios`
  - obtém parâmetros do YAML para caminhos, compressão e separador
  - registra etapas com `logging.info`

## Tecnologias utilizadas

- Python 3.10+
- PySpark 4.1.1
- PyYAML 6.0.3
- pytest
- Git

## Requisitos

- Python 3.10 ou superior
- Java 11 ou superior
- Git
- Ambiente virtual recomendado

## Instalação

```bash
git clone https://github.com/<seu-usuario>/trabalho-data-engineering-pyspark.git
cd trabalho-data-engineering-pyspark
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

No Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Configuração

A configuração do projeto fica em `config/settings.yaml`.

Valores principais:

```yaml
spark:
  app_name: "Analise de Pedidos e pagamentos"

paths:
  pagamentos: "./trabalho-data-engineering-pyspark/data/input/datasets-json-pagamentos/data/pagamentos/"
  pedidos: "./trabalho-data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/"
  output: "./trabalho-data-engineering-pyspark/data/output/pedidos_pagamentos"
  output_relatorios: "./trabalho-data-engineering-pyspark/data/output/relatorios"

file_options:
  pedidos_csv:
    compression: "gzip"
    header: True
    sep: ";"
  pagamentos_json:
    compression: "gzip"
    header: True
```

Ajuste os caminhos se os dados estiverem em uma localização diferente.

## Dados de entrada

Os datasets devem ser colocados nos diretórios abaixo:

- `data/input/datasets-csv-pedidos/data/pedidos/`
- `data/input/datasets-json-pagamentos/data/pagamentos/`

Repositórios de origem:

- https://github.com/infobarbosa/datasets-csv-pedidos
- https://github.com/infobarbosa/dataset-json-pagamentos

## Execução

Execute o pipeline a partir da raiz do projeto:

```bash
PYTHONPATH=src python src/main.py
```

Ou instale o pacote em modo editável e execute:

```bash
pip install -e .
run-data-pipeline
```

## Saída gerada

O pipeline grava resultados em Parquet nos diretórios:

- `data/output/pedidos_pagamentos`
- `data/output/relatorios`

O relatório final contém as colunas:

- `id_pedido`
- `uf`
- `forma_pagamento`
- `valor_total`
- `data_criacao`

E está ordenado por:

- `uf`
- `forma_pagamento`
- `data_criacao`

## Evidências do código-fonte

Abaixo estão trechos do código-fonte que comprovam a implementação dos requisitos do trabalho.

### `src/main.py`

```python
config = Config()
app_name = config['spark']['app_name']
spark = SparkSessionManager.get_spark_session(app_name=app_name)
data_handler = DataHandler(spark)
transformer = Transformation()
pipeline = Pipeline(data_handler, transformer)
pipeline.run(config=config)
```

### `src/config/settings.py`

```python
class Config:
    def __init__(self, path: str = "./trabalho-data-engineering-pyspark/config/settings.yaml"):
        self.path = path
        self._dados = self._carregar(path)

    def _carregar(self, path: str) -> dict:
        logger.info(f"Carregando arquivo de configuração: {path}")
        with open(path, 'r') as file:
            return yaml.safe_load(file)

    def __getitem__(self, key):
        return self._dados[key]
```

### `src/session/spark_session.py`

```python
class SparkSessionManager:
    @staticmethod
    def get_spark_session(app_name: str = "Analise de Pedidos e Pagamentos") -> SparkSession:
        return SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .getOrCreate()
```

### `src/io_utils/data_handler.py`

```python
class DataHandler:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pedidos(self) -> StructType:
        return StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", FloatType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True)
        ])

    def _get_schema_pagamentos(self) -> StructType:
        return StructType([
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", DoubleType(), True)
            ]), True),
            StructField("data_processamento", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("id_pedido", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("valor_pagamento", DoubleType(), True)
        ])

    def load_pedidos(self, path: str, compression: str, header: bool, sep: str) -> DataFrame:
        schema = self._get_schema_pedidos()
        return self.spark.read.option("compression", compression).option("mode", "FAILFAST").csv(path, header=header, schema=schema, sep=sep)

    def load_pagamentos(self, path: str, compression: str) -> DataFrame:
        schema = self._get_schema_pagamentos()
        return self.spark.read.option("compression", compression).json(path, schema=schema)
```

### `src/processing/transformations.py`

```python
class Transformation:
    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        return pedidos_df.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))

    def join_pedidos_pagamentos(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        return pedidos_df.join(pagamentos_df, pedidos_df.id_pedido == pagamentos_df.id_pedido, "inner") \
            .select(
                pedidos_df.id_pedido,
                pedidos_df.produto,
                pedidos_df.valor_unitario,
                pedidos_df.quantidade,
                pedidos_df.data_criacao,
                pedidos_df.uf,
                pedidos_df.id_cliente,
                pedidos_df.valor_total,
                pagamentos_df.forma_pagamento,
                pagamentos_df.valor_pagamento,
                pagamentos_df.status,
                pagamentos_df.data_processamento,
                pagamentos_df.avaliacao_fraude
            )

    def gerar_relatorio_pagamentos_recusados_legitimos(self, pedidos_pagamentos_df: DataFrame) -> DataFrame:
        return (
            pedidos_pagamentos_df
            .filter(
                (F.col("status") == False)
                & (F.col("avaliacao_fraude.fraude") == False)
                & (F.year(F.col("data_criacao")) == 2025)
            )
            .select(
                "id_pedido",
                "uf",
                "forma_pagamento",
                "valor_total",
                "data_criacao"
            )
            .orderBy("uf", "forma_pagamento", "data_criacao")
        )
```

### `src/pipeline/pipeline.py`

```python
class Pipeline:
    def __init__(self, data_handler: DataHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config):
        pedidos_df = self.data_handler.load_pedidos(
            path=config['paths']['pedidos'],
            compression=config['file_options']['pedidos_csv']['compression'],
            header=config['file_options']['pedidos_csv']['header'],
            sep=config['file_options']['pedidos_csv']['sep']
        )

        pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)

        pagamentos_df = self.data_handler.load_pagamentos(
            path=config['paths']['pagamentos'],
            compression=config['file_options']['pagamentos_json']['compression']
        )

        pedidos_pagamentos = self.transformer.join_pedidos_pagamentos(pedidos_df, pagamentos_df)
        self.data_handler.write_parquet(df=pedidos_pagamentos, path=config['paths']['output'])

        relatorio = self.transformer.gerar_relatorio_pagamentos_recusados_legitimos(pedidos_pagamentos)
        self.data_handler.write_parquet(df=relatorio, path=config['paths']['output_relatorios'])
```

## Testes

O projeto inclui testes com `pytest`.

Para executar:

```bash
pytest
```

## Critérios específicos atendidos

- schemas explícitos para todos os DataFrames
- orientação a objetos com classes e pacotes separados
- injeção de dependências via `main.py`
- configuração centralizada em `config/settings.yaml`
- gestão de sessão Spark em `src/session/spark_session.py`
- leitura e escrita de dados em `src/io_utils/data_handler.py`
- lógica de negócio em `src/processing/transformations.py`
- orquestração do pipeline em `src/pipeline/pipeline.py`
- logging configurado em `src/main.py`
- tratamento de erros com `try/except` e logs
- empacotamento com `pyproject.toml`, `requirements.txt`, `MANIFEST.in` e `README.md`
- testes unitários usando `pytest`

## Link do repositório

https://github.com/seu-usuario/trabalho-data-engineering-pyspark

## Licença

MIT---
