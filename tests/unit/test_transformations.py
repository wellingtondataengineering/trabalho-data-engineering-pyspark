import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    FloatType,
    TimestampType,
    BooleanType,
    DoubleType
)
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

SCHEMA_PEDIDOS_COM_TOTAL = StructType([
    StructField("id_cliente", LongType(), True),
    StructField("valor_total", FloatType(), True)
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


class TestAddValorTotalPedidos:

    def test_calcula_valor_total(self, spark):
        df = spark.createDataFrame(
            [("P1", "TV", 1500.0, 2, None, "SP", 1)],
            SCHEMA_PEDIDOS
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total == pytest.approx(3000.0)

    def test_adiciona_coluna_valor_total(self, spark):
        df = spark.createDataFrame(
            [("P1", "TV", 100.0, 3, None, "SP", 1)],
            SCHEMA_PEDIDOS
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert "valor_total" in resultado.columns

    def test_valor_total_zero_quando_quantidade_zero(self, spark):
        df = spark.createDataFrame(
            [("P1", "TV", 100.0, 0, None, "SP", 1)],
            SCHEMA_PEDIDOS
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total == pytest.approx(0.0)

    def test_valor_total_nulo_quando_valor_unitario_nulo(self, spark):
        df = spark.createDataFrame(
            [("P1", "TV", None, 2, None, "SP", 1)],
            SCHEMA_PEDIDOS
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total is None

class TestGetTop10PedidoCliente:

    def test_retorna_apenas_10_registros(self, spark):
        dados = [(i, float(i * 100)) for i in range(1, 16)]
        df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
        resultado = Transformation().get_top_10_pedido_cliente(df)
        assert resultado.count() == 10

    def test_ordena_por_maior_valor(self, spark):
        dados = [(1, 1000.0),(2, 5000.0),(3, 3000.0)]
        
        df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
        resultado = Transformation().get_top_10_pedido_cliente(df)
        linhas = resultado.collect()
        assert linhas[0].id_cliente == 2

    def test_agrega_valores_mesmo_cliente(self, spark):
        dados = [(1, 100.0),(1, 200.0),(2, 500.0)]
        
        df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
        resultado = Transformation().get_top_10_pedido_cliente(df)
        linhas = {
            r.id_cliente: r.valor_total
            for r in resultado.collect()
        }
        assert linhas[1] == pytest.approx(300.0)

    def test_retorna_todos_quando_menos_de_10(self, spark):
        dados = [
            (1, 100.0),
            (2, 200.0),
            (3, 300.0)
        ]
        df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
        resultado = Transformation().get_top_10_pedido_cliente(df)
        assert resultado.count() == 3

class TestJoinPedidosPagamentos:

    def test_realiza_join_por_id_pedido(self, spark):
        pedidos = spark.createDataFrame([("P1", "TV", 1000.0, 1, None, "SP", 1), ("P2", "NOTEBOOK", 2000.0, 1, None, "GO", 2)],SCHEMA_PEDIDOS)
        pedidos = Transformation().add_valor_total_pedidos(pedidos)
        pagamentos = spark.createDataFrame([({"fraude": False, "score": 0.1},"2025-01-01","PIX","P1",True,1000.0)],SCHEMA_PAGAMENTOS)

        resultado = Transformation().join_pedidos_pagamentos(pedidos,pagamentos)

        assert resultado.count() == 1

    def test_retorna_colunas_esperadas(self, spark):
        pedidos = spark.createDataFrame([("P1", "TV", 1000.0, 1, None, "SP", 1)],SCHEMA_PEDIDOS)
        pedidos = Transformation().add_valor_total_pedidos(pedidos)

        pagamentos = spark.createDataFrame([({"fraude": False, "score": 0.1},"2025-01-01","PIX","P1",True,1000.0)],SCHEMA_PAGAMENTOS)

        resultado = Transformation().join_pedidos_pagamentos(pedidos,pagamentos)

        assert set(resultado.columns) == {
            "id_pedido",
            "produto",
            "valor_unitario",
            "quantidade",
            "data_criacao",
            "uf",
            "id_cliente",
            "valor_total",
            "forma_pagamento",
            "valor_pagamento",
            "status",
            "data_processamento",
            "avaliacao_fraude"
        }

    def test_inner_join_remove_pedidos_sem_pagamento(self, spark):
        pedidos = spark.createDataFrame([("P1", "TV", 1000.0, 1, None, "SP", 1),("P2", "NOTEBOOK", 2000.0, 1, None, "GO", 2)],SCHEMA_PEDIDOS)
        pedidos = Transformation().add_valor_total_pedidos(pedidos)

        pagamentos = spark.createDataFrame([({"fraude": False, "score": 0.1},"2025-01-01","PIX","P1",True,1000.0)],SCHEMA_PAGAMENTOS)

        resultado = Transformation().join_pedidos_pagamentos(pedidos,pagamentos)

        ids = [r.id_pedido for r in resultado.collect()]

        assert "P1" in ids
        assert "P2" not in ids