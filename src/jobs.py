from src.extract import *
import pandas as pd
from src.db import * 
from tqdm import tqdm
import os 
from loguru import logger

def single_extract_job(i):
    ''' 
    Extract all possible 4 data dicts from https://repositorio.centrolaboral.gob.mx/
    and insert them to mysql table
        inputs 
            i(str) of page number
    '''
    soup = get_raw_entries_by_page(i)
    reglamentos_entries, asociaciones_entries,contratos_vig_entries, contratos_hist_entries = extract_all_entries(soup)
    if len(asociaciones_entries)>0:
        insert_data(asociaciones_entries,'asociacion')
        logger.info('Asociaciones data added!')
    if len(contratos_vig_entries)>0:
        insert_data(contratos_vig_entries,'current_contracts')
        logger.info('Contratos vigentes data added!')
    if len(reglamentos_entries)>0:
        insert_data(reglamentos_entries,'reglamentos')
        logger.info('Reglamentos data added!')
    if len(contratos_hist_entries)>0:
        logger.info(contratos_hist_entries)
        insert_data(contratos_hist_entries,'historic_contracts')
        logger.info('Contratos historicos data added!')


def single_extract_contract_job(url):
    ''' 
    Extract all possible data dicts from contratods
    and insert them to mysql table
        inputs 
            url(str) contract url prefix
    '''
    metadata, urls = get_data(url)
    if len(metadata)> 0:
        insert_data([metadata], 'contratos')
        insert_data(urls, 'contratos_docs_urls')



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
            name = 'metadata/contratos_docs_urls' + entry['id_url'] + '/' + entry['type'] + '/'+ name
            send_to_s3(name,r.content)
            # send data to control table
            new_entry = [{'file_url' : entry['file_url'],
                            'type': entry['type'],
                            'source': entry['source'],
                            'id_url': entry['id_url'],
                            'stamp_created': entry['stamp_created'],
                            'is_active' : '1',
                            }]
            insert_data(new_entry,'contratos_docs_urls')
        else:
            new_entry = [{'file_url' : url,
                            'type': entry['type'],
                            'source': entry['source'],
                            'id_url': entry['id_url'],
                            'stamp_created': entry['stamp_created'],
                            'is_active' : '0',
                            }]
            insert_data(new_entry,'contratos_docs_urls')
            logger.info('File NOT inserted to S3')
    except:
        new_entry = [{'file_url' : url,
                        'type': entry['type'],
                        'source': entry['source'],
                        'id_url': entry['id_url'],
                        'stamp_created': entry['stamp_created'],
                        'is_active' : '0',
                        }]
        insert_data(new_entry,'contratos_docs_urls')
        logger.info('File NOT inserted to S3')