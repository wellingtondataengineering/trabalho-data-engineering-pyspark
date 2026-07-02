# src/io_utils/data_handler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                              ArrayType, DateType, FloatType, TimestampType, BooleanType, DoubleType)
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError # <-- Importante para erros da JVM
import logging

logger = logging.getLogger(__name__)

class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        

    def _get_schema_pagamentos(self) -> StructType:
       return StructType([
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
    

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", FloatType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True)
        ])
        
    def load_pagamentos(self, path: str, compression: str) -> DataFrame:
        """Carrega o dataframe de pagamentos a partir de um arquivo JSON."""
        schema = self._get_schema_pagamentos()
        return self.spark.read.option("compression", compression).json(path, schema=schema)


    # src/io_utils/data_handler.py
    def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
        
        try:
            """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
            schema = self._get_schema_pedidos()
            df = self.spark.read.option("compression", compression).option("mode", "FAILFAST").csv(path, header=header, schema=schema, sep=sep)
            
            # Verificação de Dataframe Vazio
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")
            
            return df

        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo de pedidos: {e}")
            raise e # Relança o erro para o pipeline
        except Py4JJavaError as e:
            logger.critical(f"Erro Crítico na JVM (possível arquivo de pedidos corrompido ou erro de memória): {e}")
            raise e

    def write_parquet(self, df: DataFrame, path: str):
        logger.info(
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.
        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """)
        try:
            # Verifica se o DataFrame está vazio
            if df.isEmpty():
                logger.warning(
                    f"ATENÇÃO: O DataFrame está vazio. Nenhum registro será salvo em '{path}'."
                )
                return
    
            df.write.mode("overwrite").parquet(path)
    
            logger.info(f"Dados salvos com sucesso em: {path}")
    
        except AnalysisException as e:
            logger.error(f"Erro de análise ao salvar arquivo Parquet: {e}")
            raise e
    
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar arquivo Parquet em '{path}': {e}")
            raise e