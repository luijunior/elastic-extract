#!/usr/bin/env python3
from consulta_elastic import Elasticsearch
import json
import gera_saida
from flask import Flask, render_template, request, send_from_directory, make_response

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './'

search_payload = '''
{
  "size": %s,
  "from": %s,
  "_source": {
    "includes": [
      "payload",
      "sessao",
      "time",
      "fluxo"
    ]
  },
  "sort": [
    {
      "@timestamp": "asc"
    },
    {
      "_uid": "desc"
    }
  ],
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "fluxo:\\"post-/{cpf}-application/json\\"",
            "analyze_wildcard": true
          }
        }
      ],
      "should": [
        {
          "match": {
            "payload": "o_codigo\\"\\"o_msg\\"\\"o_sucesso\\":1"
          }
        },
        {
          "match": {
            "payload": "cpf"
          }
        }
      ],
      "filter": [
        {
          "range": {
            "@timestamp": {
              "gte": "%s",
              "lte": "%s"
            }
          }
        }
      ]
    }
  }
}'''


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return  render_template('index.html')
    if request.method == 'POST':
        data_inicio = request.form['data_inicio']
        data_fim = request.form['data_fim']
        if not data_inicio:
            data_inicio = 'now-3d/d'
        if not data_fim:
            data_fim = 'now/d'
        filename = 'cartao_%s_%s.txt' % (data_inicio, data_fim)
        response = make_response(gera_string(data_inicio, data_fim))
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response


def gera_string(data_inicio, data_fim):
    _from = 0
    totalRegistros = 10000
    size = 50
    resultado_final = ''
    while _from < totalRegistros:
        search_elastic_payload = json.loads(search_payload % (str(size), str(_from), data_inicio, data_fim))
        print(search_elastic_payload)
        elasticsearch = Elasticsearch()
        elasticsearch.request('10.1.60.6', search_elastic_payload)
        resultado_list = elasticsearch.agrupa_resultados_por_sessao()
        print(elasticsearch.total)
        totalRegistros = elasticsearch.total
        _from += size + 1
        resultado_final += gera_saida.como_string(resultado_list)
    return resultado_final


if __name__ == '__main__':
    app.secret_key = 'super-secret-key'
    app.debug = True
    app.run(host='0.0.0.0', port=5000)

