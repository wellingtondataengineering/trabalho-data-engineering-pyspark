# src/config/settings.py
import yaml
import logging

logger = logging.getLogger(__name__)


class Config:
    """
    Classe de configuração centralizada da aplicação.
    Carrega o arquivo settings.yaml e expõe seus valores ao restante do pipeline.
    """

    def __init__(self, path: str = "./trabalho-data-engineering-pyspark/config/settings.yaml"):
        self.path = path
        self._dados = self._carregar(path)

    def _carregar(self, path: str) -> dict:
        """Carrega um arquivo de configuração YAML."""
        logger.info(f"Carregando arquivo de configuração: {path}")
        with open(path, 'r') as file:
            return yaml.safe_load(file)

    def __getitem__(self, key):
        return self._dados[key]

    def as_dict(self) -> dict:
        """Retorna a configuração completa como dicionário."""
        return self._dados
