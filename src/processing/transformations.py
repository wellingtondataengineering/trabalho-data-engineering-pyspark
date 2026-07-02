# src/processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)

class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna 'valor_total' (valor_unitario * quantidade) ao DataFrame de pedidos."""
        logger.info("Adicionando valor total do pedido")
        try:
            return pedidos_df.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
        except Exception as e:
            logger.error(f"Erro ao calcular valor_total dos pedidos: {e}")
            raise

    def join_pedidos_pagamentos(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e pagamentos."""
        logger.info("Fazendo join entre tabelas de pedido e pagamentos")
        try:
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
        except Exception as e:
            logger.error(f"Erro ao unir pedidos e pagamentos: {e}")
            raise

    def get_top_10_pedido_cliente(self, pedidos_df: DataFrame) -> DataFrame:
        """Calcula o valor total de pedidos por cliente e retorna os 10 maiores."""
        logger.info("Trazendo os top 10 clientes com maiores pedidos")
        try:
            return pedidos_df.groupBy("id_cliente") \
                .agg(F.sum("valor_total").alias("valor_total")) \
                .orderBy(F.desc("valor_total")) \
                .limit(10)
        except Exception as e:
            logger.error(f"Erro ao calcular top 10 clientes: {e}")
            raise

    def gerar_relatorio_pagamentos_recusados_legitimos(
        self,
        pedidos_pagamentos_df: DataFrame
    ) -> DataFrame:
        """
        Gera relatório de pedidos:
        - pagamento recusado (status=False)
        - fraude=False
        - ano de 2025
        """
        logger.info("Gerando relatório de pagamentos recusados e legítimos (ano 2025)")
        try:
            return (
                pedidos_pagamentos_df
                .filter(
                    (F.col("status") == False)
                    &
                    (F.col("avaliacao_fraude.fraude") == False)
                    &
                    (F.year(F.col("data_criacao")) == 2025)
                )
                .select(
                    "id_pedido",
                    "uf",
                    "forma_pagamento",
                    "valor_total",
                    "data_criacao"
                )
                .orderBy(
                    "uf",
                    "forma_pagamento",
                    "data_criacao"
                )
            )
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de pagamentos recusados legítimos: {e}")
            raise            