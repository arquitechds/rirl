# Request data
import requests
from bs4 import BeautifulSoup
from loguru import logger
import unicodedata
import pandas as pd
def get_raw_entries_by_page(page):
    ''' 
    Extracts all html given a page number
        input page: page to iterate throw the website
    
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        # 'Cookie': '_ga_XT5D9P1XZZ=GS1.1.1709495574.3.1.1709498582.15.0.0; _ga=GA1.1.886665048.1709169090; XSRF-TOKEN=eyJpdiI6IkxoR1ZVVGJNTHNVTTVKN3RRaStWZVE9PSIsInZhbHVlIjoiaUQzVFJVOTJ2anJiUDlvMkN1OVVqRDd0d0lrOFBDSmxCZjB2MHlIUWliL3Rhb20ybTNFd1YrS3dXb3pxY1Z5UnBQbFVUc3pVWVJMTnRhZEw4RDBNUUQ2Ni9WcmlzcUlwcTgvWTRKMTUzc2tvMVlsOEx2Q0hSMzZWMWgwaWxHWTciLCJtYWMiOiJhMWY0ZDk2ZDY5NmNkNzQ0M2U4NWIxNDMxNDI5OTgzNDQ0Y2RhNzM3OTQxN2ExOTA1NmE5MzAxZjAxOGZiOGYzIiwidGFnIjoiIn0%3D; repositorio_session=eyJpdiI6IjdTS0l2TTV6bTAxK01HSGFCNHkrQVE9PSIsInZhbHVlIjoiemVsRm5ROXBCcWYrdDcvSFdKQVpsdEY0My9PQTZ5U0FBL0o5MUtUVE8rYkM0QUIzclBVNXJXS3Q0MVYybUc1VFUvZmVEbVBLcVNXQWxUSEM5K1Rtd2xBeU5GQ3BYc0xONmFCeGp6RHpFT1pEY0MvTVlMNWhPNXhpMk4yR2F5Qk4iLCJtYWMiOiI5YTQ1ZDcxMzE4YTgzNTNiZjMyMzU5NDgzYjNlODRiZmZhNjc0Y2UzN2U3YThmNDJlM2ExZGE5ZDhmMDBhYzE3IiwidGFnIjoiIn0%3D; avisoprivacidad=true; informativohistorico=true; XSRF-TOKEN=eyJpdiI6InI0U1Ewamo5bGwzREJFR3pWNU1HZmc9PSIsInZhbHVlIjoiakVFbi9yeU9NNXRGRERlb1M1Ky82MVZiZ0MzL05QMVZLNDNDOVJZUmxzc1piY0t2L3BSYkxPeENIdGxKVS80aHlCN3d4OTVjS3FyOVRTVlIxZ2xmUjYxdEQwZkl5QkF3YmpPL0tOYzZqQm8yajgrWGRxMTdmZFZWdnRJRGpSRE8iLCJtYWMiOiJiZmIxNTQyZjY4YWQ3ZjA1NTYyMWZkZWE3NDFiOGQ3NjQ3MTdiNjJiMTA0MGVlYzE5MjJmYzYxNzVhY2JkOTYwIiwidGFnIjoiIn0%3D; repositorio_session=eyJpdiI6Im9oNWFRdDVRV2grcnZuZWQyMlJlb2c9PSIsInZhbHVlIjoiWTh0RGl3UDRHdjJidkNkYnJueVRqOU1CeFpGelc1NExPM1pCSHdSSldyUVZhNTBvOFBhTldKcmVxS050RnVLZjQ1QVc2aFNrd2ptRWxKSytmd1BVck1sNjFHYmRMOFRxMnM0eTdKdWZnTU1aOEJTSU9mazZ5ZkNZTzl4M2VpZ3IiLCJtYWMiOiJiZDMwMzJkOTg2NWQzMGVlYjFkNTQwMWVlMmQ5NmFlZDI2ZmRlNjY0YTgzMmJmZDU5MjE4Yzk4NDE0NWE3ZDc2IiwidGFnIjoiIn0%3D',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }

    params = {
        'cont': str(page),
        'reg': str(page),
        'asoc':str(page)
    }
    cookies = {
        '_ga_XT5D9P1XZZ': 'GS1.1.1710045020.10.1.1710045042.38.0.0',
        '_ga': 'GA1.1.886665048.1709169090',
        'XSRF-TOKEN': 'eyJpdiI6IlR6SlNrdEt1NzZ6STJQNUFQMFB4ZXc9PSIsInZhbHVlIjoidE1kTk9WZXg5eWVXWEdIbnNTUlBic1J6anFsdDQrb3VlSXE1eHRvS2hWYmFYSElyR2VzQmNIWExKaTlDUmRkSzhUOGFRZm5ISS81TUE1bjkrNTNYWWpMSU0wV2Q0SVZ6RDdMZFZwY2xuek5TUHRDaXVGN2V3NURFQm9rZnZFY3kiLCJtYWMiOiJhMDRjZTlhYWNmNTk3YmJmMjQwOWU5MDE1YTZkYjI1MGUxZGI4OTliYmM0MzA5ODFkZjYxZDRiYTlkZDU0ZTgwIiwidGFnIjoiIn0%3D',
        'repositorio_session': 'eyJpdiI6Imp1eFdzK0kxTnhYTmhIK2hxcWR3TWc9PSIsInZhbHVlIjoiWURBUkVpNzZVaExqMy9GSTN0ajR5eWJwRFpsdjMxb3FNazZ5UXRtTW5Lb0liN3VQUi9IOHdwMDF0THNjWjZ2dnVtSUpBc0FteFFSaVBpUDl5WmhKNjh0WmhTb2M5MERQa2lvQS84cWM2VVJIU0h5c3lnWnR0VXdacjc5aVFydmkiLCJtYWMiOiJjZjAyMWM0MzRiYmI1N2M4YTZkZGEyMjkzNGE1ZDBiNGU2ODc3MGUwYzljOTQ1MzE1NjA2NTlhNmRjZmVmZDI0IiwidGFnIjoiIn0%3D',
        'avisoprivacidad': 'false',
    }

    response = requests.get('https://repositorio.centrolaboral.gob.mx/', params=params, cookies=cookies, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    logger.info(f'Extracted page {page}')
    return soup



### Extract functions


def get_reglamento_entry_data(entry):
    ''' 
    Parses raw html data from reglamentos
        input entry: bs4 obj
    Returns
        data (dict)

    '''
    url = entry.find_all('a')[0]['href']
    titulo = entry.find_all('b')[0].text.strip()
    folio_unico = entry.find_all('span')[0].text.strip()
    num_expediente = entry.find_all('span')[1].text.strip()
    fecha_registro = entry.find_all('span')[2].text.strip()
    autoridad = entry.find_all('span')[3].text.strip()
    patron_empresa = entry.find_all('div')[-1].text.strip().replace('  ','').replace('Patrón, empresa(s) o establecimiento(s):\n','')
    data = {'url' : url,
            'titulo': titulo,
            'folio_unico': folio_unico,
            'num_expediente': num_expediente,
            'fecha_registro':fecha_registro,
            'autoridad': autoridad,
            'patron_empresa': patron_empresa
            }
    return data

def get_contrato_entry_data(entry):
    ''' 
    Parses raw html data from contratos
        input entry: bs4 obj
    Returns 
        data (dict)

    
    '''
    if entry.find('div').text.strip() == 'Expediente de contrato colectivo':
        status = 'vigente'
    else:
        status = 'archivo_historico'

    url = entry.find_all('a')[0]['href']
    numero_registro = entry.find_all('span')[0].text.strip()
    folio_unico = entry.find_all('span')[1].text.strip()
    entidad_origen = entry.find_all('span')[2].text.strip()
    autoridad_registro = entry.find_all('span')[3].text.strip()
    fecha_presentacion = entry.find_all('span')[4].text.strip()
    nombre_asociacion = entry.find_all('span')[5].text.strip()
    pea_antecedentes = entry.find_all('span')[6].text.strip().replace('  ','')
    pea_legitimacion = entry.find_all('span')[7].text.strip().replace('  ','')
    pea_revision_salarial = entry.find_all('span')[8].text.strip().replace('  ','')
    pea_revision_contrato = entry.find_all('span')[9].text.strip().replace('  ','')

    data = {'url' : url,
            'numero_registro': numero_registro,
            'folio_unico': folio_unico,
            'entidad_origen':entidad_origen,
            'fecha_presentacion':fecha_presentacion,
            'autoridad_registro': autoridad_registro,
            'nombre_asociacion': nombre_asociacion,
            'pea_antecedentes': pea_antecedentes,
            'pea_legitimacion':pea_legitimacion,
            'pea_revision_salarial': pea_revision_salarial,
            'pea_revision_contrato': pea_revision_contrato,
            'status': status
            }
    return data


def get_asociacion_entry_data(entry):
    ''' 
    Parses raw html data from asociaciones
        input entry: bs4 obj
        
    returns
        data (dict)
    
    '''
    url = entry.find_all('a')[0]['href']
    folio_tramite = entry.find_all('span')[0].text.strip()
    nombre_asociacion = entry.find_all('span')[1].text.strip().replace('  ','')
    num_expediente = entry.find_all('span')[2].text.strip()
    folio_unico = entry.find_all('span')[3].text.strip()
    fecha_registro = entry.find_all('span')[4].text.strip()
    fecha_ultimo_tramite = entry.find_all('span')[5].text.strip()
    entidad_origen = entry.find_all('span')[6].text.strip()
    autoridad = entry.find_all('span')[7].text.strip()
    data = {'url' : url,
            'folio_tramite': folio_tramite,
            'nombre_asociacion': nombre_asociacion,
            'num_expediente': num_expediente,
            'folio_unico': folio_unico,
            'fecha_registro':fecha_registro,
            'fecha_ultimo_tramite':fecha_ultimo_tramite,
            'entidad_origen': entidad_origen,
            'autoridad': autoridad,
            }
    return data


def extract_all_entries(soup):
    ''' 
    Divides html soup object into 4 lists of each entry (reglamento, asociaciones, contrato vig y contrato hist)
        input: bs4 soup

    Returns 4 lists of 10 items each for each entry group
    
    '''

    reglamento = 'opcion-resultado-item item-reglamento'
    asociacion = 'opcion-resultado-item item-asociacion'
    contrato_vigente = 'opcion-resultado-item item-contrato item-vigente'
    contrato_historico = 'opcion-resultado-item item-contrato item-historico'

    reglamentos = soup('div', {"class": reglamento})
    asociaciones = soup('div', {"class": asociacion})
    contratos_vig = soup('div', {"class": contrato_vigente})
    contratos_hist = soup('div', {"class": contrato_historico})


    reglamentos_entries = []
    for a in reglamentos:
        reglamentos_entries = reglamentos_entries + [get_reglamento_entry_data(a)]

    asociaciones_entries = []
    for b in asociaciones:
        asociaciones_entries = asociaciones_entries + [get_asociacion_entry_data(b)]

    contratos_vig_entries = []
    for c in contratos_vig:
        contratos_vig_entries = contratos_vig_entries + [get_contrato_entry_data(c)]

    contratos_hist_entries = []
    for d in contratos_hist:
        contratos_hist_entries = contratos_hist_entries + [get_contrato_entry_data(d)]

    return reglamentos_entries, asociaciones_entries,contratos_vig_entries, contratos_hist_entries



### EXTRACT all data!
def get_soup(url):
    ''' 
    Extracts all html given a page number
        input page: page to iterate throw the website
    
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        # 'Cookie': '_ga_XT5D9P1XZZ=GS1.1.1709495574.3.1.1709498582.15.0.0; _ga=GA1.1.886665048.1709169090; XSRF-TOKEN=eyJpdiI6IkxoR1ZVVGJNTHNVTTVKN3RRaStWZVE9PSIsInZhbHVlIjoiaUQzVFJVOTJ2anJiUDlvMkN1OVVqRDd0d0lrOFBDSmxCZjB2MHlIUWliL3Rhb20ybTNFd1YrS3dXb3pxY1Z5UnBQbFVUc3pVWVJMTnRhZEw4RDBNUUQ2Ni9WcmlzcUlwcTgvWTRKMTUzc2tvMVlsOEx2Q0hSMzZWMWgwaWxHWTciLCJtYWMiOiJhMWY0ZDk2ZDY5NmNkNzQ0M2U4NWIxNDMxNDI5OTgzNDQ0Y2RhNzM3OTQxN2ExOTA1NmE5MzAxZjAxOGZiOGYzIiwidGFnIjoiIn0%3D; repositorio_session=eyJpdiI6IjdTS0l2TTV6bTAxK01HSGFCNHkrQVE9PSIsInZhbHVlIjoiemVsRm5ROXBCcWYrdDcvSFdKQVpsdEY0My9PQTZ5U0FBL0o5MUtUVE8rYkM0QUIzclBVNXJXS3Q0MVYybUc1VFUvZmVEbVBLcVNXQWxUSEM5K1Rtd2xBeU5GQ3BYc0xONmFCeGp6RHpFT1pEY0MvTVlMNWhPNXhpMk4yR2F5Qk4iLCJtYWMiOiI5YTQ1ZDcxMzE4YTgzNTNiZjMyMzU5NDgzYjNlODRiZmZhNjc0Y2UzN2U3YThmNDJlM2ExZGE5ZDhmMDBhYzE3IiwidGFnIjoiIn0%3D; avisoprivacidad=true; informativohistorico=true; XSRF-TOKEN=eyJpdiI6InI0U1Ewamo5bGwzREJFR3pWNU1HZmc9PSIsInZhbHVlIjoiakVFbi9yeU9NNXRGRERlb1M1Ky82MVZiZ0MzL05QMVZLNDNDOVJZUmxzc1piY0t2L3BSYkxPeENIdGxKVS80aHlCN3d4OTVjS3FyOVRTVlIxZ2xmUjYxdEQwZkl5QkF3YmpPL0tOYzZqQm8yajgrWGRxMTdmZFZWdnRJRGpSRE8iLCJtYWMiOiJiZmIxNTQyZjY4YWQ3ZjA1NTYyMWZkZWE3NDFiOGQ3NjQ3MTdiNjJiMTA0MGVlYzE5MjJmYzYxNzVhY2JkOTYwIiwidGFnIjoiIn0%3D; repositorio_session=eyJpdiI6Im9oNWFRdDVRV2grcnZuZWQyMlJlb2c9PSIsInZhbHVlIjoiWTh0RGl3UDRHdjJidkNkYnJueVRqOU1CeFpGelc1NExPM1pCSHdSSldyUVZhNTBvOFBhTldKcmVxS050RnVLZjQ1QVc2aFNrd2ptRWxKSytmd1BVck1sNjFHYmRMOFRxMnM0eTdKdWZnTU1aOEJTSU9mazZ5ZkNZTzl4M2VpZ3IiLCJtYWMiOiJiZDMwMzJkOTg2NWQzMGVlYjFkNTQwMWVlMmQ5NmFlZDI2ZmRlNjY0YTgzMmJmZDU5MjE4Yzk4NDE0NWE3ZDc2IiwidGFnIjoiIn0%3D',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }


    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    logger.info(f'Extracted page')
    return soup

def clean_txt(input_str):
    input_str = input_str.replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u')
    input_str = input_str.replace(' ','_').replace(':','').lower().strip().replace('__','').replace('\n','')
    return input_str

def get_metadata(soup,url_prefix):
    values = []
    keys = []
    for entry in soup.find_all('div' , class_="dato-extra"):
        if entry.find('b') == None:
            break
        key = clean_txt(entry.find('b').text.replace('(','').replace(')',''))
        try: 
            value = clean_txt(entry.find('li').text)
        except:
            try:
                value = clean_txt(entry.find('span').text)

            except:
                value = None

        values = values + [value]
        keys = keys + [key]

    dictionary = dict(map(lambda key, value: (key, value), keys, values))
    dictionary['url'] = url_prefix
    return dictionary

def chunks(l, n):
    """
    Yield successive n-sized chunks from list l.
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]

def get_tramites_urls(soup,url_prefix):
    types = []
    urls = []
    for entry in soup.find_all('div', class_ = 'document-group-item'):
        types = types + [clean_txt(entry.find('span').text)]
        urls = urls + [entry.find('a')['href']]
    df = pd.DataFrame([urls,types]).T
    df.columns = ['file_url', 'type']
    df['source'] = 'tramites_urls'
    df['id_url'] = url_prefix
    df['is_active'] = 0

    tramites_urls = df.to_dict('records')

    return tramites_urls

def related_asocs(asociacion_soup):
    table = asociacion_soup.find('table')
    values = []
    for entry in table.find_all('span'):
        values = values + [clean_txt(entry.text)]
    df = pd.DataFrame(list(chunks(values, 5)))
    df.columns = ['folio','folio2','asociacion','fecha_const','autoridad']
    df.pop('folio2')
    related_asoc = df.to_dict('records')
    return related_asoc

def get_expedientes_urls(tramites_relacionados,url_prefix):
    table = tramites_relacionados.find('table')
    if table == None:
        return []
    values = []
    for entry in table.find_all('td'):
        values = values + [clean_txt(entry.text)]
    df = pd.DataFrame(list(chunks(values, 5)))
    df.columns = ['file_url','type','date','size','button']
    df  = df[['file_url','type']]
    df['source'] = 'expedientes_urls'
    df['id_url'] = url_prefix
    df['is_active'] = '0'
    related_expedientes = df.to_dict('records')
    return related_expedientes


def get_data(url_prefix):
    url = 'https://repositorio.centrolaboral.gob.mx'+ url_prefix
    soup = get_soup(url)

    informacion_general = soup.find_all('div', class_='detalle-informacion-seccion')[0]
    tramites_relacionados = soup.find_all('div', class_='detalle-informacion-seccion')[1]
    #asociacion = soup.find_all('div', class_='detalle-informacion-seccion')[2]
    tramites_urls =  get_tramites_urls(tramites_relacionados,url_prefix)
    expedientes_urls = get_expedientes_urls(tramites_relacionados,url_prefix)
    urls = tramites_urls + expedientes_urls
    informacion_general = get_metadata(informacion_general,url_prefix)
    tramites_relacionados = get_metadata(tramites_relacionados,url_prefix)
    
    data = {**informacion_general, **tramites_relacionados}
    #related_asocs_dict = related_asocs(asociacion)

    keys = ['autoridad_que_genero_el_registro',
    'empresa_o_persona_empleadora',
    'entidad_federativa_de_origen',
    'entidades',
    'fecha_de_constancia_de_representacion',
    'fecha_de_deposito_inicial',
    'fecha_de_la_constancia_de_legitimacion',
    'fecha_de_votacion',
    'fecha_del_dictamen',
    'fecha_del_evento_de_votacion',
    'folio_del_tramite',
    'nombre_de_la_asociacion',
    'numero_de_contrato',
    'numero_de_trabajadores',
    'ramas_economicas_de_la_industria',
    'resultado_de_la_legitimacion',
    'rfc_de_la_empresa',
    'url']

    if set(keys) == set(data.keys()):
        logger.info('Correct columns!')
    else: 
        logger.info('Incorrect columns')
        logger.info(url_prefix)
        data = []
        urls = []
    return data,urls






