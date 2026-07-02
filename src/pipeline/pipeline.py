# src/pipeline/pipeline.py
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import logging

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """
    def __init__(self, data_handler: DataHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        logger.info("Pipeline iniciado...")        
        
        logger.info("Abrindo o dataframe de pedidos")
        path_pedidos = config['paths']['pedidos']
        compression_pedidos = config['file_options']['pedidos_csv']['compression']
        header_pedidos = config['file_options']['pedidos_csv']['header']
        separator_pedidos = config['file_options']['pedidos_csv']['sep']
        logger.info(f"""
        Obtidos os seguintes parâmetros de pedidos: 
        - path: {path_pedidos}
        - compression_pedidos: {compression_pedidos}
        - header_pedidos: {header_pedidos}
        - separator_pedidos: {separator_pedidos}
        """)
       
        pedidos_df = self.data_handler.load_pedidos(path = path_pedidos, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)
        logger.info("Adicionando a coluna valor_total")
        pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)
        pedidos_df.show(5, truncate=False)
        
        pedido_top_10 = self.transformer.get_top_10_pedido_cliente(pedidos_df)
        
        logger.info("Abrindo o dataframe de pagamentos com schema explícito definido em DataHandler")
        path_pagamentos = config['paths']['pagamentos']
        compression_pagamentos = config['file_options']['pagamentos_json']['compression']
       
        
        path_pagamentos = config['paths']['pagamentos']
        compression_pagamentos = config['file_options']['pagamentos_json']['compression']
        
        logger.info(f"""
        Obtidos os seguintes parâmetros de pagamentos:
        - path: {path_pagamentos}
        - compression_pagamentos: {compression_pagamentos}
        """)        
    
        compression_pagamentos = config['file_options']['pagamentos_json']['compression']
        pagamentos = self.data_handler.load_pagamentos(path=path_pagamentos, compression="gzip")
        pagamentos.show(5, truncate=False)
        
        pedidos_pagamentos = self.transformer.join_pedidos_pagamentos(pedidos_df, pagamentos)
        
        logger.info("Escrevendo o resultado em parquet do dataframe Pedidos pagamentos")
        path_output = config['paths']['output']
        print(f"Obtido o path de saída: {path_output}")
        self.data_handler.write_parquet(df=pedidos_pagamentos, path=path_output)



        logger.info("Escrevendo o resultado em parquet do dataframe Pagamentos recusados")
        path_output = config['paths']['output_relatorios']
        
        relatorio = self.transformer.gerar_relatorio_pagamentos_recusados_legitimos(pedidos_pagamentos) 
        relatorio.show(20, truncate=False)
        
        self.data_handler.write_parquet(df=relatorio, path=path_output)
        relatorio.show(20, truncate=False)
        
        logger.info("Pipeline concluído com sucesso!")        