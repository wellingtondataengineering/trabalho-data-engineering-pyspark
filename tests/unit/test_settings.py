# tests/unit/test_settings.py

import pytest
import yaml

from config.settings import Config


@pytest.fixture
def arquivo_config_valido(tmp_path):
    """Cria um settings.yaml válido para testes."""

    config_data = {
        "spark": {
            "app_name": "Analise de Pedidos e pagamentos"
        },
        "paths": {
            "pagamentos": "/dados/pagamentos/",
            "pedidos": "/dados/pedidos/",
            "output": "/dados/output/"
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

    config_file = tmp_path / "settings.yaml"
    config_file.write_text(yaml.dump(config_data))

    return str(config_file)


class TestCarregarConfig:

    def test_carrega_yaml_valido_como_dicionario(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido)

        assert isinstance(resultado.as_dict(), dict)

    def test_ler_app_name(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido)

        assert resultado["spark"]["app_name"] == "Analise de Pedidos e pagamentos"

    def test_ler_path_pagamentos(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido)

        assert resultado["paths"]["pagamentos"] == "/dados/pagamentos/"

    def test_ler_path_pedidos(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido)

        assert resultado["paths"]["pedidos"] == "/dados/pedidos/"

    def test_ler_output(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido)

        assert resultado["paths"]["output"] == "/dados/output/"

    def test_ler_configuracao_csv(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido)

        assert resultado["file_options"]["pedidos_csv"]["compression"] == "gzip"
        assert resultado["file_options"]["pedidos_csv"]["header"] is True
        assert resultado["file_options"]["pedidos_csv"]["sep"] == ";"

    def test_ler_configuracao_json(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido)

        assert resultado["file_options"]["pagamentos_json"]["compression"] == "gzip"

    def test_arquivo_inexistente_lanca_excecao(self):
        with pytest.raises(FileNotFoundError):
            Config("/caminho/que/nao/existe/settings.yaml")

    def test_config_possui_chaves_obrigatorias(self, arquivo_config_valido):
        resultado = Config(arquivo_config_valido).as_dict()

        assert "spark" in resultado
        assert "paths" in resultado
        assert "file_options" in resultado