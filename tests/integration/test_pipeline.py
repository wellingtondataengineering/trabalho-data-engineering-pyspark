import gzip
import json
import pytest
from unittest.mock import MagicMock

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    FloatType,
    TimestampType,
    BooleanType,
    DoubleType,
)

from io_utils.data_handler import DataHandler
from pipeline.pipeline import Pipeline
from processing.transformations import Transformation


SCHEMA_PEDIDOS = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", FloatType(), True),
    StructField("quantidade", LongType(), True),
    StructField("data_criacao", TimestampType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True)
])

SCHEMA_PAGAMENTOS = StructType([
    StructField(
        "avaliacao_fraude",
        StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score", DoubleType(), True)
        ]),
        True
    ),
    StructField("data_processamento", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("id_pedido", StringType(), True),
    StructField("status", BooleanType(), True),
    StructField("valor_pagamento", DoubleType(), True)
])


@pytest.fixture
def config_teste():
    return {
        "paths": {
            "pedidos": "/mock/pedidos/",
            "pagamentos": "/mock/pagamentos/",
            "output": "/mock/output/",
            "output_relatorios": "/mock/output_relatorios/"
        },
        "file_options": {
            "pedidos_csv": {
                "compression": "gzip",
                "header": True,
                "sep": ";"
            },
            "pagamentos_json": {
                "compression": "gzip"
            }
        }
    }


@pytest.fixture
def dataframes_mock(spark):

    pedidos_df = spark.createDataFrame([
        ("PED001", "TV", 1500.0, 2, None, "SP", 1),
        ("PED002", "NOTEBOOK", 3000.0, 1, None, "RJ", 2)
    ], SCHEMA_PEDIDOS)

    pagamentos_df = spark.createDataFrame([
        (
            {"fraude": False, "score": 0.98},
            "2024-01-01",
            "PIX",
            "PED001",
            True,
            3000.0
        ),
        (
            {"fraude": False, "score": 0.99},
            "2024-01-02",
            "CARTAO",
            "PED002",
            True,
            3000.0
        )
    ], SCHEMA_PAGAMENTOS)

    return pedidos_df, pagamentos_df


def _handler_mock(pedidos_df, pagamentos_df):

    handler = MagicMock(spec=DataHandler)

    handler.load_pedidos.return_value = pedidos_df
    handler.load_pagamentos.return_value = pagamentos_df

    return handler


class TestPipelineOrquestracao:

    def test_le_pedidos_com_parametros_da_config(
        self,
        config_teste,
        dataframes_mock
    ):
        handler = _handler_mock(*dataframes_mock)

        Pipeline(handler, Transformation()).run(config_teste)

        handler.load_pedidos.assert_called_once_with(
            path="/mock/pedidos/",
            compression="gzip",
            header=True,
            sep=";"
        )

    def test_le_pagamentos_com_path_da_config(
        self,
        config_teste,
        dataframes_mock
    ):
        handler = _handler_mock(*dataframes_mock)

        Pipeline(handler, Transformation()).run(config_teste)

        handler.load_pagamentos.assert_called_once()

    def test_grava_no_output_correto(
        self,
        config_teste,
        dataframes_mock
    ):
        handler = _handler_mock(*dataframes_mock)

        Pipeline(handler, Transformation()).run(config_teste)

        handler.write_parquet.assert_called_once()

        assert (
            handler.write_parquet.call_args.kwargs["path"]
            == "/mock/output/"
        )


class TestPipelineEndToEnd:

    def test_pipeline_completo_gera_parquet_valido(
        self,
        spark,
        tmp_path
    ):

        pedidos_lines = [
            "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
            "PED001;TV;1500.0;2;2024-01-01 10:00:00;SP;1",
            "PED002;NOTEBOOK;3000.0;1;2024-01-02 11:00:00;RJ;2"
        ]

        pedidos_path = tmp_path / "pedidos.csv.gz"

        with gzip.open(pedidos_path, "wt", encoding="utf-8") as f:
            f.write("\n".join(pedidos_lines))

        pagamentos = [
            {
                "avaliacao_fraude": {
                    "fraude": False,
                    "score": 0.98
                },
                "data_processamento": "2024-01-01",
                "forma_pagamento": "PIX",
                "id_pedido": "PED001",
                "status": True,
                "valor_pagamento": 3000.0
            },
            {
                "avaliacao_fraude": {
                    "fraude": False,
                    "score": 0.99
                },
                "data_processamento": "2024-01-02",
                "forma_pagamento": "CARTAO",
                "id_pedido": "PED002",
                "status": True,
                "valor_pagamento": 3000.0
            }
        ]

        pagamentos_path = tmp_path / "pagamentos.json.gz"

        with gzip.open(pagamentos_path, "wt", encoding="utf-8") as f:
            for p in pagamentos:
                f.write(json.dumps(p) + "\n")

        output_path = str(tmp_path / "output")
        output_relatorios_path = str(tmp_path / "output_relatorios")

        config = {
            "paths": {
                "pedidos": str(pedidos_path),
                "pagamentos": str(pagamentos_path),
                "output": output_path,
                "output_relatorios": output_relatorios_path
            },
            "file_options": {
                "pedidos_csv": {
                    "compression": "gzip",
                    "header": True,
                    "sep": ";"
                },
                "pagamentos_json": {
                    "compression": "gzip"
                }
            }
        }

        Pipeline(
            DataHandler(spark),
            Transformation()
        ).run(config)

        resultado = spark.read.parquet(output_path)

        assert resultado.count() == 2

        assert set(resultado.columns) == {
            "id_pedido",
            "produto",
            "valor_unitario",
            "quantidade",
            "data_criacao",
            "uf",
            "id_cliente",
            "forma_pagamento",
            "valor_pagamento",
            "status",
            "data_processamento",
            "avaliacao_fraude"
        }