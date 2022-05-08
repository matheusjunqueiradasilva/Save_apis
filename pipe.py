from numpy import save
from sqlalchemy import create_engine
import pandas as pd
import json
import psycopg2
import requests
import pprint
from flatten_json import flatten

class ApiMiddleware:
    def __init__(self,endpoint):
        # Inicializa a engine do postgress e seta o endpoint
        self.endpoint = endpoint
        self.engine = create_engine("")


    def formata(self,nome_tabela = "export_default"):
         # Realizar requisicao para o endpoint
        _requisicao = requests.get(self.endpoint)
        _resultado = _requisicao.json()
        _resultado = _resultado.get("data")

# formata o json e envia pra um dataframe
        dict_flatten = (flatten(d) for d in _resultado)

        df = pd.DataFrame(dict_flatten)
# Salva o dataframe dentro do SQL
        df.to_sql(nome_tabela, self.engine, if_exists='replace')


    
pegar_dados= ApiMiddleware("")
pegar_dados.formata("")

