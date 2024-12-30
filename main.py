#!/usr/bin/env python3 
#from src.paralel import *
# Update data from first 300 pages for all categories
#regular_run_paralel(1, 300)
#contract_metadata_upload_paralel()
#contract_files_upload_to_s3_paralel()

from multiprocessing import Process
from loguru import logger
from tqdm import tqdm
# pararel for contracts
from src.db import *
import json
from src.aws import *


def get_text(table, filter):
    # Leer datos de la tabla
    df = read_table(f'select * from archivos.{table} WHERE type = "{filter}" ORDER BY rand() limit 100')
    
    # Eliminar columnas innecesarias
    df.pop('stamp_created')
    df.pop('stamp_updated')

    # Guardar como archivo CSV
    df.to_csv(f'{table}.csv')
    
    # Convertir el DataFrame en una lista de diccionarios
    d = df.to_dict('records')

    # Procesar cada registro
    for e in tqdm(d):
        start_time = time.time()  # Registrar el tiempo inicial
        try:
            textract = TextractHandler()
            s3_id = 's3://rirl-documents/' + e['s3_uri']
            t = textract.extract_raw_text(s3_id)
            e['text'] = t
        except Exception as ex:
            e['text'] = "Error in extraction"
            print(f"Error: {ex}")
        finally:
            elapsed_time = time.time() - start_time
            if elapsed_time > 120:  # Verificar si excede 2 minutos
                print(f"Iteration exceeded time limit of 2 minutes ({elapsed_time:.2f} seconds). Skipping.")
                e['text'] = "Timeout error in extraction"

    # Guardar en un archivo JSON
    with open(f"{table}.json", "w", encoding="utf-8") as json_file:
        json.dump(d, json_file, ensure_ascii=False, indent=4)  # Formato legible para humanos

    print("Dictionary saved to JSON file")

get_text('contratos_deposito_inicial','contrato_colectivo_de_trabajo')
get_text('contratos_historicos','Expediente digitalizado en origen')
get_text('contratos_legitimados','contrato_colectivo_de_trabajo')
