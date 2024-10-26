from src.db import *

import pandas as pd
from src.utils import tukan_df_multiple_column_text_treater
from src.aws import TextractHandler
from tqdm import tqdm
#### deposito resolucion



def get_related_pdfs(url_prefix):
    """
    Gets pdfs data given an url_prefix
    """
    url_prefix = url_prefix.replace('https://repositorio.centrolaboral.gob.mx','')
    numero_registro = read_table(f"SELECT numero_registro FROM `control`.`contratos` WHERE (`url` = '{url_prefix}')").iloc[0][0]
    related_pdfs = read_table(f"SELECT * FROM `archivos`.`contratos` WHERE `file_id` = '{numero_registro}'")
    return related_pdfs


def extract_table(resolucion_metadata):

    s3_uri = 's3://rirl-documents/' +  resolucion_metadata['s3_uri'] 
    file_id = resolucion_metadata['file_id']
    file_url = resolucion_metadata['file_url']

    th = TextractHandler()
    dfs = th.complete_textract(s3_uri)
    dfs[0].columns
    new_row = pd.DataFrame([dfs[0].columns], columns=['column','value'])
    dfs[0].columns = ['column','value']
    df_with_headers = pd.concat([new_row, dfs[0]], ignore_index=True)
    clean_df = tukan_df_multiple_column_text_treater(df_with_headers,df_with_headers.columns).T

    df = clean_df.iloc[1:2]
    df.columns = clean_df.iloc[0]
    df['file_id'] = file_id
    df['s3_uri'] = s3_uri
    df['url'] = file_url

    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace(':', '')
    df.reset_index(drop=True, inplace=True)

    return df


def get_resolucion_tables():
    dfs = []
    for url in tqdm(read_table(f'select url from control.visited_contratos_deposito_inicial WHERE resolucion_de_deposito > 0' )['url']):
        df = get_related_pdfs(url)
        resolucion_metadata = df[df['type']=='resolucion_de_deposito_inicial'].to_dict('records')[0]
        df = extract_table(resolucion_metadata)
        dfs = dfs + [df]
    return pd.concat(dfs)