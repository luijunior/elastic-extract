#!/usr/bin/env python3
import json


def como_arquivo(resultado_list):
    for payloads in resultado_list :
        if any('cpf' in s for s in resultado_list[payloads].payload) and any('true' in s for s in resultado_list[payloads].payload):
            with open('cartao.txt', 'a') as out:
                for content in resultado_list[payloads].payload:
                    if ('cpf' in content):
                        jsoncliente = json.loads(content)
                        out.write(jsoncliente['cpf'] + ';' + resultado_list[payloads].time + '\n')


def como_string(resultado_list):
    lista_final = ''
    for payloads in resultado_list :
        if any('cpf' in s for s in resultado_list[payloads].payload) and any('true' in s for s in resultado_list[payloads].payload):
            with open('cartao.txt', 'a') as out:
                for content in resultado_list[payloads].payload:
                    if ('cpf' in content):
                        jsoncliente = json.loads(content)
                        lista_final += (jsoncliente['cpf'] + ';' + resultado_list[payloads].time + '\n')
    return lista_final