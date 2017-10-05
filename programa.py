#!/usr/bin/env python3
from consulta_elastic import Elasticsearch
import json
import salva_arquivo
from flask import Flask, render_template, request, send_from_directory, make_response

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './'

search_payload = '''
{
  "size": 50,
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
              "gte": "now-3d/d",
              "lte": "now/d"
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
        gera_arquivo()
        response = make_response(image_binary)
        return send_from_directory(
            app.config['UPLOAD_FOLDER'],
            'cartao.txt',
            as_attachment=True)


def gera_arquivo():
    _from = 0
    totalRegistros = 100000
    size = 50
    while _from < totalRegistros:
        search_elastic_payload = json.loads(search_payload % str(_from))
        elasticsearch = Elasticsearch()
        elasticsearch.request('localhost', search_elastic_payload)
        resultado_list = elasticsearch.agrupa_resultados_por_sessao()
        print(elasticsearch.total)
        totalRegistros = elasticsearch.total
        _from += size + 1
        salva_arquivo.salvar(resultado_list)


if __name__ == '__main__':
    app.secret_key = 'super-secret-key'
    app.debug = True
    app.run(host='0.0.0.0', port=5000)

