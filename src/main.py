# src/main.py
from config.settings import Config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline
import logging


# Crie a configuração do logging
def configurar_logging():
  """Configura o logging para todo o projeto."""
  logging.basicConfig(
      # Nível mínimo de severidade para ser registrado.
      # DEBUG < INFO < WARNING < ERROR < CRITICAL
      level=logging.INFO,

      # Formato da mensagem de log.
      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S',

      # Lista de handlers. Aqui, estamos logando para um arquivo e para o console.
      handlers=[
          logging.FileHandler("trabalho-dataeng-pyspark-poo.log"), # Log para arquivo
          logging.StreamHandler()                         # Log para o console (terminal)
      ]
  )
  logging.info("Logging configurado.")


def main():
    configurar_logging()
    logger = logging.getLogger(__name__)

    config = Config()
    app_name = config['spark']['app_name']
    logger.info(f"Obtido o app name: {app_name}")
    
    
    spark = None
    
    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name)
        data_handler = DataHandler(spark)
        transformer = Transformation()
        pipeline = Pipeline(data_handler, transformer)
        pipeline.run(config=config)

    except Exception as e:
        logging.error(f"FALHA CRÍTICA NO PIPELINE: {e}")
    finally:
    
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    main()