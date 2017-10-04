#!/usr/bin/env python3
from consulta_elastic import Elasticsearch
import json


if __name__ == '__main__':
    with open('request.json') as data_file:
        request = json.load(data_file)
    elasticsearch  = Elasticsearch()
    elasticsearch.request('localhost', request)
    source_list = elasticsearch.agrupa_resultados_por_sessao()
    for payloads in source_list :
        if any('cpf' in s for s in source_list[payloads].payload) and any('true' in s for s in source_list[payloads].payload):
            with open('cartao.txt', 'a') as out:
                for content in source_list[payloads].payload:
                    if ('cpf' in content):
                        jsoncliente = json.loads(content)
                        out.write(jsoncliente['cpf'] + ';' + source_list[payloads].time + '\n')
