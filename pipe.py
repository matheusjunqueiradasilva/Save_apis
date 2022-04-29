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
        self.engine = create_engine("postgresql://postgres:2ky7wM&duA4@localhost:5433/CostumeSucess")

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
        


#pegar_dados_organizations =APIMiddleware("https://csp.pipedrive.com/api/v1/organizations(name)?api_token=844b0375fc6a4810c1ee9561204bee2e0d752e8c")
#pegar_dados_organizations.save("salvando_organizations")

#pegar_dados_dels= APIMiddleware("https://csp.pipedrive.com/api/v1/deals:(org_name,title,status,value)?api_token=844b0375fc6a4810c1ee9561204bee2e0d752e8c")
#pegar_dados_dels.save("salvando dels")

pegar_dados_person = APIMiddleware('https://csp.pipedrive.com/api/v1/persons:(name,org_name,owner_name,email,phone)?api_token=844b0375fc6a4810c1ee9561204bee2e0d752e8c')
pegar_dados_person.save("salvando person")