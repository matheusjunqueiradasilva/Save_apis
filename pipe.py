from numpy import save
from sqlalchemy import create_engine
import pandas as pd
import json
import psycopg2
import requests
import pprint
from flatten_json import flatten

class APIMiddleware:
    def __init__(self, endpoint):
        # Inicializa a engine do postgress e seta o endpoint
        self.endpoint = endpoint
        self.engine = create_engine("")

    def save(self, nome_tabela="export_default"):
        # Realizar requisicao para o endpoint
        _requisicao = requests.get(self.endpoint)
        _resultado = _requisicao.json()
    
        print('requisição feita com sucesso')
       
        # Pega o objeto Data
        request_data = _resultado.get("data")

        # Salvo o objeto data dentro do dataframe
        request_data_frame = pd.DataFrame(request_data)

        # Salva o dataframe dentro do SQL
        request_data_frame.to_sql(nome_tabela, self.engine, if_exists='replace')
        
        print("tabela", nome_tabela, "salva com sucesso")

    def formata(self,nome_tabela = "export_default"):
        _requisicao = requests.get(self.endpoint)
        _resultado = _requisicao.json()
        _resultado = _resultado.get("data")

# formata o json e envia pra um dataframe
        dict_flatten = (flatten(d) for d in _resultado)

        df = pd.DataFrame(dict_flatten)
# Salva o dataframe dentro do SQL
        df.to_sql(nome_tabela, self.engine, if_exists='replace')


    


pegar_dados_organizations =APIMiddleware("")
pegar_dados_organizations.save("organizations")

pegar_dados_dels= APIMiddleware("")
pegar_dados_dels.save("dels")

pegar_dados_person = APIMiddleware('')
pegar_dados_person.formata("person")

