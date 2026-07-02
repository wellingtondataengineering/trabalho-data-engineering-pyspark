# tests/unit/test_data_handler.py
import gzip
import json
import os
import pytest

from pyspark.sql.types import (StructType,StructField,StringType,LongType,FloatType,TimestampType,BooleanType,DoubleType)

from io_utils.data_handler import DataHandler


@pytest.fixture
def arquivo_pedidos_gz(tmp_path):
    """Arquivo CSV gzipado com pedidos."""
    linhas = [
        "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
        "PED001;TV;1500.0;2;2024-01-01 10:00:00;SP;1",
        "PED002;NOTEBOOK;3000.0;1;2024-01-02 11:00:00;RJ;2",
        "PED003;MONITOR;800.0;3;2024-01-03 12:00:00;MG;1"
    ]

    gz_path = tmp_path / "pedidos.csv.gz"

    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(linhas))

    return str(gz_path)


@pytest.fixture
def arquivo_pagamentos_gz(tmp_path):
    """Arquivo JSON gzipado com pagamentos."""

    pagamentos = [
        {
            "avaliacao_fraude": {
                "fraude": False,
                "score": 0.95
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
                "score": 0.98
            },
            "data_processamento": "2024-01-02",
            "forma_pagamento": "CARTAO",
            "id_pedido": "PED002",
            "status": True,
            "valor_pagamento": 3000.0
        }
    ]

    gz_path = tmp_path / "pagamentos.json.gz"

    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for registro in pagamentos:
            f.write(json.dumps(registro) + "\n")

    return str(gz_path)


class TestLoadPedidos:

    def test_le_csv_gz_e_retorna_dataframe(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz,
            compression="gzip",
            header=True,
            sep=";"
        )

        assert df.count() == 3

    def test_schema_pedidos_tem_tipos_corretos(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz,
            compression="gzip",
            header=True,
            sep=";"
        )

        tipos = {campo.name: campo.dataType for campo in df.schema.fields}

        assert isinstance(tipos["valor_unitario"], FloatType)
        assert isinstance(tipos["quantidade"], LongType)
        assert isinstance(tipos["id_cliente"], LongType)


class TestLoadPagamentos:

    def test_le_json_gz_e_retorna_dataframe(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(
            arquivo_pagamentos_gz,
            compression="gzip"
        )

        assert df.count() == 2

    def test_schema_pagamentos_tem_tipos_corretos(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(
            arquivo_pagamentos_gz,
            compression="gzip"
        )

        tipos = {campo.name: campo.dataType for campo in df.schema.fields}

        assert isinstance(tipos["status"], BooleanType)
        assert isinstance(tipos["valor_pagamento"], DoubleType)

    def test_campo_avaliacao_fraude_existe(self, spark, arquivo_pagamentos_gz):
        df = DataHandler(spark).load_pagamentos(
            arquivo_pagamentos_gz,
            compression="gzip"
        )

        assert "avaliacao_fraude" in df.columns


class TestWriteParquet:

    def test_dados_gravados_podem_ser_relidos(self, spark, tmp_path):

        schema = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("valor_total", FloatType(), True)
        ])

        df = spark.createDataFrame(
            [
                ("PED001", 3000.0),
                ("PED002", 1500.0)
            ],
            schema
        )

        output_path = str(tmp_path / "saida_parquet")

        DataHandler(spark).write_parquet(df, output_path)

        assert os.path.exists(output_path)

        df_lido = spark.read.parquet(output_path)

        assert df_lido.count() == 2

    def test_parquet_mantem_colunas(self, spark, tmp_path):

        schema = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("valor_total", FloatType(), True)
        ])

        df = spark.createDataFrame(
            [("PED001", 3000.0)],
            schema
        )

        output_path = str(tmp_path / "saida_parquet_colunas")

        DataHandler(spark).write_parquet(df, output_path)

        df_lido = spark.read.parquet(output_path)

        assert set(df_lido.columns) == {
            "id_pedido",
            "valor_total"
        }