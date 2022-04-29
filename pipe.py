from numpy import save
from sqlalchemy import create_engine
import pandas as pd
import json
import psycopg2
import requests
import pprint

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
        pprint.pprint(request_data)

        # Salvo o objeto data dentro do dataframe
        request_data_frame = pd.DataFrame(request_data)

        # Salva o dataframe dentro do SQL
        request_data_frame.to_sql(nome_tabela, self.engine, if_exists='replace')
        
        print("tabela", nome_tabela, "salva com sucesso")
        


#pegar_dados_organizations =APIMiddleware("")
#pegar_dados_organizations.save("salvando_organizations")

#pegar_dados_dels= APIMiddleware("")
#pegar_dados_dels.save("salvando dels")

pegar_dados_person = APIMiddleware('')
pegar_dados_person.save("salvando person")