#!/usr/bin/env python3
from src.paralel import *
from src.extract import *
# Update data from first 300 pages for all categories

from loguru import logger
import sys
#log_level = "DEBUG"
#log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
#logger.add(sys.stderr, level=log_level, format=log_format, colorize=True, backtrace=True, diagnose=True)
#logger.add("file.log", level=log_level, format=log_format, colorize=False, backtrace=True, diagnose=True)



#for i in tqdm(range(100,112)):
#    contract_files_upload_to_s3_paralel_selenium('contratos',limit = 5000, offset = 5000*i,status  = 'archivo_historico', table_archivos = 'contratos_historicos')

contract_files_upload_to_s3_paralel_selenium_missing('contratos',status  = 'archivo_historico', table_archivos = 'contratos_historicos')

