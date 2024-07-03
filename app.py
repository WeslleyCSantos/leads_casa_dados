from urllib import request
from collections import OrderedDict
import json
import pandas as pd
import urllib
from bs4 import BeautifulSoup
import concurrent.futures
import threading
import streamlit as st
import itertools
import time



headers = OrderedDict({
    'authority': 'api.casadosdados.com.br',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    # Already added when you pass json=
    # 'content-type': 'application/json',
    'origin': 'https://casadosdados.com.br',
    'referer': 'https://casadosdados.com.br/',
    'sec-ch-ua': '"Opera";v="89", "Chromium";v="103", "_Not:A-Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'User-agent': 'Its a me mario',
})

lista_parametros = ['CNPJ','Razão Social','Telefone','E-MAIL','Quadro Societário','Atividade Principal']


def prepara_request(page=1):
  json_data =  OrderedDict({
    'query': {
        'termo': [],
        'atividade_principal': [],
        'natureza_juridica': [],
        'uf': ["PB"],
        'municipio': ['CABEDELO'],
        'situacao_cadastral': 'ATIVA',
        'cep': [],
        'ddd': [],
    },
    'range_query': {
        'data_abertura': {
            'lte': None,
            'gte': None,
        },
        'capital_social': {
            'lte': None,
            'gte': None,
        },
    },
    'extras': {
        'somente_mei': False,
        'excluir_mei': False,
        'com_email': False,
        'incluir_atividade_secundaria': False,
        'com_contato_telefonico': False,
        'somente_fixo': False,
        'somente_celular': False,
        'somente_matriz': False,
        'somente_filial': False,
    },
    'page': page,
})
  data = json.dumps(json_data)
  data = data.encode('utf-8')
  return data



def gera_cliente(cnpj,ctx):
  st.report_thread.add_report_ctx(threading.currentThread(), ctx)
  request_client = urllib.request.Request(f'https://casadosdados.com.br/solucao/cnpj/{cnpj}', headers=headers)
  site_cliente = urllib.request.urlopen(request_client).read().decode('utf-8')
  soup = BeautifulSoup(site_cliente, 'html.parser')
  v = soup.find_all('p')
  dados_clientes = {}
  for element in range(len(v)):
    if v[element].text in lista_parametros and element+1 < len(v):
      if v[element+1].text not in lista_parametros:
        dados_clientes[v[element].text] = v[element+1].text
      else:
        dados_clientes[v[element].text] = ' '
  return dados_clientes

@st.cache
def gera_csv():
    lista_clientes = []
    lista_cnpjs = []
    backoff_factor = 1.2
    max_wait_time = 20
    error_time = 1
    for i in range(1,51):
        data = prepara_request(i)
        request = urllib.request.Request('https://api.casadosdados.com.br/v2/public/cnpj/search', headers=headers,data=data)
        request.add_header('Content-Type', 'application/json; charset=utf-8')
        request.add_header('Content-Length', len(data))
        try:
            r = urllib.request.urlopen(request).read().decode('utf-8')
            response = json.loads(r)
            lista_cnpjs.append([cnpj['cnpj'] for cnpj in response['data']['cnpj']])
        except urllib.error.HTTPError as e:
            if e.code == 429:  # Check if it's a "Too Many Requests" error
                time.sleep(min( backoff_factor** error_time,max_wait_time))  # Exponential backoff: wait for increasing durations
                error_time+=1
                print(f"Too many requests. Retrying after {backoff_factor ** error_time} seconds...")
                i -= 1  # Retry the same page
            else:
                raise  # Raise other HTTP errors
        else:
            error_time = 1  # Reset error counter on successful request
    lista_cnpjs = list(itertools.chain.from_iterable(lista_cnpjs))
    print('Done getting ',len(lista_cnpjs))
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        ctx = st.report_thread.get_report_ctx()
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(gera_cliente, cnpj,ctx): cnpj for cnpj in lista_cnpjs}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                pass
            else:
                lista_clientes.append(data)
    clientes_df = pd.DataFrame(lista_clientes)
    return clientes_df


@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode('utf-8')

def main():
    st.header('Gerador de Leads')
    st.subheader('Demonstração de MVP')
    st.markdown('---')
    df = gera_csv()
    st.download_button(label='Baixar leads como CSV',data=convert_df(df),file_name='Arquivo_clientes.csv',mime='text/csv')    



if __name__ == '__main__':
    main()
