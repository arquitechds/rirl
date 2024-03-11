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
        inser_data(asociaciones_entries,'asociacion')
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

