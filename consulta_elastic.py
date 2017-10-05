#!/usr/bin/env python3
import requests
from resultado import Resultado


class Elasticsearch():

    def __init__(self):
        self.response = ''

    def request(self, host, request, port=9200, index='logstash-2017*'):
        url = 'http://%s:%s/%s/_search' % (host, port, index)
        print("Request para a url {0}".format(url))
        response = requests.post(url=url,json=request)
        if(response.status_code==200):
            self.response = response.json()
        else:
            raise Exception('Erro ao consultar retornando codigo diferente de 200, retornado {0}'.format(response.status_code))

    def agrupa_resultados_por_sessao(self):
        resultado_list = {}
        self.total = self.response['hits']['total']
        for hits in self.response['hits']['hits']:
            payload = hits['_source']['payload']
            time = hits['_source']['time']
            # Caso exista adiciona novo elemento
            if (hits['_source']['sessao'] in resultado_list.keys()):
                resultado_list[hits['_source']['sessao']].payload.append(payload)
                resultado_list[hits['_source']['sessao']].time = time
            # Senao inclui novo elemento
            else:
                resultado = Resultado()
                resultado.payload.append(payload)
                resultado.time = time
                resultado_list[hits['_source']['sessao']] = resultado
        return resultado_list