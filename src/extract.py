# Request data
import requests
from bs4 import BeautifulSoup

def get_raw_entries_by_page(page):
    ''' 
    Extracts all html given a page number
        input page: page to iterate throw the website
    
    '''

    cookies = {
        #'_ga_XT5D9P1XZZ': 'GS1.1.1709495574.3.1.1709498582.15.0.0',
        #'_ga': 'GA1.1.886665048.1709169090',
        'XSRF-TOKEN': 'eyJpdiI6IkxoR1ZVVGJNTHNVTTVKN3RRaStWZVE9PSIsInZhbHVlIjoiaUQzVFJVOTJ2anJiUDlvMkN1OVVqRDd0d0lrOFBDSmxCZjB2MHlIUWliL3Rhb20ybTNFd1YrS3dXb3pxY1Z5UnBQbFVUc3pVWVJMTnRhZEw4RDBNUUQ2Ni9WcmlzcUlwcTgvWTRKMTUzc2tvMVlsOEx2Q0hSMzZWMWgwaWxHWTciLCJtYWMiOiJhMWY0ZDk2ZDY5NmNkNzQ0M2U4NWIxNDMxNDI5OTgzNDQ0Y2RhNzM3OTQxN2ExOTA1NmE5MzAxZjAxOGZiOGYzIiwidGFnIjoiIn0%3D',
        'repositorio_session': 'eyJpdiI6IjdTS0l2TTV6bTAxK01HSGFCNHkrQVE9PSIsInZhbHVlIjoiemVsRm5ROXBCcWYrdDcvSFdKQVpsdEY0My9PQTZ5U0FBL0o5MUtUVE8rYkM0QUIzclBVNXJXS3Q0MVYybUc1VFUvZmVEbVBLcVNXQWxUSEM5K1Rtd2xBeU5GQ3BYc0xONmFCeGp6RHpFT1pEY0MvTVlMNWhPNXhpMk4yR2F5Qk4iLCJtYWMiOiI5YTQ1ZDcxMzE4YTgzNTNiZjMyMzU5NDgzYjNlODRiZmZhNjc0Y2UzN2U3YThmNDJlM2ExZGE5ZDhmMDBhYzE3IiwidGFnIjoiIn0%3D',
        'avisoprivacidad': 'true',
        'informativohistorico': 'true',
        'XSRF-TOKEN': 'eyJpdiI6InI0U1Ewamo5bGwzREJFR3pWNU1HZmc9PSIsInZhbHVlIjoiakVFbi9yeU9NNXRGRERlb1M1Ky82MVZiZ0MzL05QMVZLNDNDOVJZUmxzc1piY0t2L3BSYkxPeENIdGxKVS80aHlCN3d4OTVjS3FyOVRTVlIxZ2xmUjYxdEQwZkl5QkF3YmpPL0tOYzZqQm8yajgrWGRxMTdmZFZWdnRJRGpSRE8iLCJtYWMiOiJiZmIxNTQyZjY4YWQ3ZjA1NTYyMWZkZWE3NDFiOGQ3NjQ3MTdiNjJiMTA0MGVlYzE5MjJmYzYxNzVhY2JkOTYwIiwidGFnIjoiIn0%3D',
        'repositorio_session': 'eyJpdiI6Im9oNWFRdDVRV2grcnZuZWQyMlJlb2c9PSIsInZhbHVlIjoiWTh0RGl3UDRHdjJidkNkYnJueVRqOU1CeFpGelc1NExPM1pCSHdSSldyUVZhNTBvOFBhTldKcmVxS050RnVLZjQ1QVc2aFNrd2ptRWxKSytmd1BVck1sNjFHYmRMOFRxMnM0eTdKdWZnTU1aOEJTSU9mazZ5ZkNZTzl4M2VpZ3IiLCJtYWMiOiJiZDMwMzJkOTg2NWQzMGVlYjFkNTQwMWVlMmQ5NmFlZDI2ZmRlNjY0YTgzMmJmZDU5MjE4Yzk4NDE0NWE3ZDc2IiwidGFnIjoiIn0%3D',
    }

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

    response = requests.get('https://repositorio.centrolaboral.gob.mx/', params=params, cookies=cookies, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
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