from src.extract import *
import pandas as pd
from src.db import * 
from tqdm import tqdm
import os 
from loguru import logger


##########################
#    JOBS Functions   #
##########################

# The purpose of this functions are to extract in an ordered manner the data (using extract.py package) 
# and then send it to a db (db.py package) all in a single job


rirl = rirl_scrapping_session()

def check_new_urls_for_control_tables(i):
    ''' 
    Extract all possible 4 data dicts from https://repositorio.centrolaboral.gob.mx/ (contratos vigentes, historicos, asociaciones y reglamentos)
    and insert them to mysql table
        inputs 
            i(str) of page number
    '''
    soup = rirl.get_raw_entries_by_page(i)
    reglamentos_entries, asociaciones_entries,contratos_vig_entries, contratos_hist_entries = extract_all_entries(soup)
    if len(asociaciones_entries)>0:
        insert_data(asociaciones_entries,'control.asociaciones')
        logger.info('Asociaciones data added!')
    if len(contratos_vig_entries)>0:
        insert_data(contratos_vig_entries,'control.contratos')
        logger.info('Contratos vigentes data added!')
    if len(reglamentos_entries)>0:
        insert_data(reglamentos_entries,'control.reglamentos')
        logger.info('Reglamentos data added!')
    if len(contratos_hist_entries)>0:
        logger.info(contratos_hist_entries)
        insert_data(contratos_hist_entries,'control.contratos')
        logger.info('Contratos historicos data added!')


def extract_metadata(url):
    ''' 
    Extract all possible data dicts from contratods
    and insert them to mysql table
        inputs 
            url(str) contract url prefix
    '''
    if 'reglamento' in url:
        logger.info('Reglamento url')
        get_data_f = get_data_reglamentos
        urls_table = 'archivos.reglamentos'

    elif 'contrato' in url:
        logger.info('Contrato url')
        get_data_f = get_data_contratos
        urls_table = 'archivos.contratos'

    elif 'asociacion' in url:
        logger.info('Asociacion url')
        get_data_f = get_data_asociaciones
        urls_table = 'archivos.asociaciones'


    metadata, urls, empresas, table = get_data_f(url)
    if len(metadata)> 0:
        insert_data([metadata], table)
    if len(urls)> 0:
        insert_data(urls, urls_table)
    if len(empresas)> 0:
        insert_data(empresas, 'metadata.empresas_relacionadas')




def write_document_to_s3(entry):
    ''' 
    Sends document to S3 and uploads control table
    '''
    url = entry['file_url'].strip()
    try:
        # download file
        r = requests.get(url, stream=True)
        # extract name
        name = re.findall('([^\/]+$)',entry['file_url'])[0].strip()
        if r.status_code == 200:
            # create s3 uri
            name = 'metadata/archivos/contratos/' + name
            send_to_s3(name,r.content)
            # send data to control table
            new_entry = [{'file_url' : entry['file_url'],
                            'type': entry['type'],
                            'source': entry['source'],
                            'numero_de_contrato': entry['numero_de_contrato'],
                            'numero_de_expediente': entry['numero_de_expediente'],
                            'stamp_created': entry['stamp_created'],
                            'url_active' : '1',
                            'in_s3': '1',
                            's3_uri': name,
                            }]
            insert_data(new_entry,'control.contratos_docs_urls')
        else:
            new_entry = [{'file_url' : url,
                            'type': entry['type'],
                            'source': entry['source'],
                            'numero_de_contrato': entry['numero_de_contrato'],
                            'numero_de_expediente': entry['numero_de_expediente'],
                            'stamp_created': entry['stamp_created'],
                            'url_active' : '0',
                            'in_s3': '0',
                            's3_uri': '',
                            }]
            insert_data(new_entry,'control.contratos_docs_urls')
            logger.info('File NOT inserted to S3')
    except:
        new_entry = [{'file_url' : url,
                        'type': entry['type'],
                        'source': entry['source'],
                        'numero_de_contrato': entry['numero_de_contrato'],
                        'numero_de_expediente': entry['numero_de_expediente'],
                        'stamp_created': entry['stamp_created'],
                        'url_active' : '0',
                        'in_s3': '0',
                        's3_uri': '',
                        }]
        insert_data(new_entry,'control.contratos_docs_urls')
        logger.info('File NOT inserted to S3')