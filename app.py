#!/usr/bin/env python3
from consulta_elastic import Elasticsearch
import json
import gera_saida
from flask import Flask, render_template, request
from flask_mail import Mail, Message
from time import sleep
import multiprocessing
import random
import time
from threading import current_thread

from rx import Observable
from rx.concurrency import AsyncIOScheduler, ThreadPoolScheduler

app = Flask(__name__)
app.config.update(
	DEBUG=True,
	#EMAIL SETTINGS
    MAIL_SERVER='flash.email.locaweb.com.br',
    MAIL_PORT=25,
	MAIL_USE_SSL=False
	)
mail = Mail(app)

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


class PrintObserver(Observable):

    def on_next(self, value):
        print("Received {0}".format(value))

    def on_completed(self):
        print("Done!")

    def on_error(self, error):
        print("Error Occurred: {0}".format(error))


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return  render_template('index.html')
    if request.method == 'POST':
        data_inicio = request.form['data_inicio']
        data_fim = request.form['data_fim']
        email = request.form['email']
        host_elastic = request.form['host_elastic']
        if not data_inicio:
            data_inicio = 'now-1d/d'
        if not data_fim:
            data_fim = 'now/d'
        if not host_elastic:
            host_elastic = 'localhost'
        if not email:
            return render_template('index.html', sucesso=False, msg='Favor informar um email valido')
        filename = 'cartao_%s_%s.txt' % (data_inicio, data_fim)

        #Observer
        optimal_thread_count = multiprocessing.cpu_count()
        pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
        thread_data = {'data_inicio': data_inicio,
                       'data_fim': data_fim,
                       'email': email,
                       'filename': filename,
                       'host_elastic': host_elastic}
        Observable.from_([thread_data])\
                  .map(lambda s: efetua_tarefa(s))\
                  .subscribe_on(pool_scheduler) \
                  .subscribe(on_next=lambda s: print("PROCESS 1: {0} {1}".format(current_thread().name, s)),
                       on_error=lambda e: print(e),
                       on_completed=lambda: print("PROCESS 1 done!"))
        #end
        return render_template('index.html', sucesso=True)


def efetua_tarefa(thread_data):
    attach_content = gera_string(thread_data['data_inicio'], thread_data['data_fim'], thread_data['host_elastic'])
    envia_email('Resultado em anexo',
                thread_data['email'],
               'Extracao de dados',
                thread_data['filename'],
               attach_content
               )
    return True


def envia_email(body, recipient, subject, filename, attach_content):
    with app.app_context():
        msg = Message(
            subject,
            sender='noreply-soa@ipiranga.com.br',
            recipients=
            [recipient])
        msg.attach(filename=filename,
                   content_type='text/csv',
                   data=attach_content)
        msg.body = body
        mail.send(msg)


def gera_string(data_inicio, data_fim, host_elastic):
    print(host_elastic)
    _from = 0
    totalRegistros = 10000
    size = 50
    resultado_final = ''
    while _from < totalRegistros:
        search_elastic_payload = json.loads(search_payload % (str(size), str(_from), data_inicio, data_fim))
        print(search_elastic_payload)
        elasticsearch = Elasticsearch()
        elasticsearch.request(host_elastic, search_elastic_payload)
        resultado_list = elasticsearch.agrupa_resultados_por_sessao()
        print(elasticsearch.total)
        totalRegistros = elasticsearch.total
        _from += size + 1
        resultado_final += gera_saida.como_string(resultado_list)
    return resultado_final


if __name__ == '__main__':
    app.secret_key = 'super-secret-key'
    app.debug = True
    app.run(host='0.0.0.0', port=8080)

