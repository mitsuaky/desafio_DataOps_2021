from functools import lru_cache
from operator import itemgetter
from typing import Union

import pandas as pd
import pycep_correios
from geopy import distance
from geopy.geocoders import Photon
from pymongo import MongoClient


def a() -> float:
    collection = db["estabelecimentos"]
    total_empresas = collection.count_documents({})  # Total de 31,564,677 empresas no banco de dados

    """
    Cria um pipeline que separa os todas as empresas por grupos situação cadastral 
    e depois calcula a porcentagem em cima do total de empresas no banco de dados
    """
    results = collection.aggregate([{
        "$group": {
            "_id": {"situacao_cadastral": "$situacao_cadastral"},
            "count": {"$sum": 1}
        }  # Agrupa por situacao_cadastral
    },
        {
        "$project": {
            "count": 1,
            "percentage": {
                "$multiply": [{"$divide": [100, total_empresas]}, "$count"]  # Calcula porcentagem
            }
        }
    }])

    result_list = [(x["_id"], x["total"]) for x in results]  # Cria uma lista de tuplas com (situação, porcentagem)
    for situacao in result_list:
        if situacao[0] == "02":  # Ativas
            print(f"Porcentagem de empresas ativas: {situacao[0]:.2f}%")  # Resultado: 42.04%

            return f"{situacao[0]:.2f}"


def b() -> list[tuple[float, float]]:
    collection = db["estabelecimentos"]
    results = collection.aggregate([
        {'$match': {
            'cnae_fiscal_principal': {'$regex': '^561'}
        }
        },
        {'$group': {
            '_id': {'$substrBytes': ['$data_inicio_atividades', 0, 4]},  # Pega os primeiros 4 dígitos (ano)
            'total': {'$sum': 1}
        }
        }])  # Agrupa os que tem o início do CNAE Principal 561 pela data que iniciou as atividades

    result_list = [(x["_id"], x["total"]) for x in results]  # Cria uma lista de tuplas com (ano, total)
    result_list.sort(key=itemgetter(0), reverse=True)  # Ordena as tuplas de forma decrescente pelo ano

    return result_list


def c():
    collection = db["estabelecimentos"]

    # Como muito dos dados possuem CEP e endereço repetido, utilizei o LRU Cache
    # para evitar processamento e chamada de API desnecessárias.
    @lru_cache(maxsize=None)
    def get_address_from_cep(cep: str) -> Union[str, None]:
        """Utiliza a API do VIACEP para tentar obter o endereço do CEP"""
        try:
            endereco = pycep_correios.get_address_from_cep(cep, webservice=pycep_correios.WebService.VIACEP)
            return endereco['logradouro'] + ", " + endereco['bairro'] + ", " + endereco['cidade'] + " - " + endereco['uf']
        except pycep_correios.exceptions.InvalidCEP:
            print(f"Cep Inválido: {cep}")
        except pycep_correios.exceptions.CEPNotFound:
            print(f"Cep Não Encontrado: {cep}")

        return None

    @lru_cache(maxsize=None)
    def address_to_coordinates(address: str) -> tuple[float, float]:
        """Utiliza a API do Photon para tentar pegar a coordenada do endereço"""
        geolocator = Photon(user_agent="parmenas_dataops_desafio")
        result = geolocator.geocode(address)
        if result:
            return (result.latitude, result.longitude)
        else:
            print(f"Não foi possível encontrar o endereço {address}")
            return None, None

    @lru_cache(maxsize=None)
    def calculate_distance(coordinates_start: tuple, coordinates_end: tuple) -> float:
        """Retorna a disância em kilometros entre duas coordenadas"""
        result = distance.distance(coordinates_start, coordinates_end)
        return result.kilometers

    """ Buscar mais de 30 milhões de empresas e testar CEP por CEP é loucura,
    como eu precisava somente os do São Paulo-SP, que é a cidade do CEP 01422000
    Eu puxei somente os ceps que começam em 0 e trabalhei somente com eles.
    """

    result_db = collection.find({
        '$and': [
            {'cep': {'$regex': '^0'}},
            {'cep': {'$not': {'$regex': '^0$'}}},  # Não pega caso tenha cep = "0"
            {'cep': {'$not': {'$regex': '00000000'}}}  # Não pega caso tenha cep = "00000000"
        ]
    }, projection={"_id": 1, "cep": 1})  # Retorna só o _id e o CEP

    result_raw = [x for x in result_db]

    """ Como seria ainda mais de 4 milhões de CEPs pra testar, resolvi tirar a precisão
    dos últimos 3 números do CEP, assim consigo agrupar melhor"""
    lista_cep_tratado = []
    for doc in result_raw:
        doc["cep"] = f"{doc['cep'][:-0]}000"
        lista_cep_tratado.append(doc)

    del result_raw

    lista_com_endereco = []
    for doc in lista_cep_tratado:
        address = get_address_from_cep(doc["cep"])
        if address:
            doc["endereco"] = address
            lista_com_endereco.append(doc)

    del lista_cep_tratado
    get_address_from_cep.cache_clear()  # Limpando o cache já que não vamos mais precisar

    lista_com_coordenadas = []
    for doc in lista_com_coordenadas:
        if doc["endereco"]:
            doc["latitude"], doc["longitude"] = address_to_coordinates(doc["endereco"])
            if doc["latitude"]:
                lista_com_coordenadas.append(doc)

    del lista_com_endereco
    address_to_coordinates.cache_clear()

    # Calcula a distância dos CEPs com início nos coordenadas do CEP 01422000: (-23.5648196, -46.6600444)
    list_com_distancias = []
    for cep in list_com_distancias:
        start = (-23.5648196, -46.6600444)
        end = (float(cep["latitude"]), float(cep["longitude"]))
        distancia = calculate_distance(start, end)
        if distancia <= 5:
            list_com_distancias.append(cep["_id"])

    calculate_distance.cache_clear()

    return len(list_com_distancias)


def exportar_respostas():

    resultado_a = a()
    resultado_b = b()
    resultado_c = c()

    df = pd.DataFrame([("a", resultado_a), ("c", resultado_c)], columns=['Letra', 'Resultado'])
    df.to_excel('resultados/resultados_a_c.xlsx', index=False)

    df_b = pd.DataFrame(resultado_b, columns=['Ano', 'Total'])
    df_b.to_excel('resultados/resultado_b.xlsx', index=False)


if __name__ == '__main__':

    client = MongoClient("localhost", 27017)
    db = client["dataOps"]

    exportar_respostas()
