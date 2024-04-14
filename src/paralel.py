
from multiprocessing import Process
from src.jobs import single_extract_job,single_extract_contract_job,write_document_to_s3
from loguru import logger
from tqdm import tqdm

# Paralel framework to run jobs in many tasks

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def split_tasks(lowerp,upperp):
    # create all tasks
    processes = [Process(target=single_extract_job, args=(i,)) for i in range(lowerp,upperp)]
    tasks = lowerp-upperp
    logger.info(f'Created {tasks} tasks')
    return processes

def execute_paralel_tasks(processes):
    # start all processes
    for process in processes:
        process.start()
    # wait for all processes to complete
    for process in processes:
        process.join()

def execute_processes_list_in_batches(processes,max_paralel_tasks):
    task_batches = chunks(processes,max_paralel_tasks)
    for batch in tqdm(task_batches):
        execute_paralel_tasks(batch)

def regular_run_paralel(lowerp,upperp):
    tasks_list = split_tasks(lowerp,upperp)
    execute_processes_list_in_batches(tasks_list,4)



# pararel for contracts
from src.db import *
from src.extract import *


def split_tasks_contracts():
    # create all tasks
    available_urls = read_table('select * from control_tables.current_contracts')
    available_urls = list(available_urls['url'])
    processes = [Process(target=single_extract_contract_job, args=(url,)) for url in available_urls]
    tasks = len(available_urls)
    logger.info(f'Created {tasks} tasks')
    return processes

def contract_metadata_upload_paralel():
    tasks_list = split_tasks_contracts()
    execute_processes_list_in_batches(tasks_list,4)

# pararel for pdfs

def split_tasks_files_to_s3():
    # create all tasks
    available_pdfs = read_table('select * from metadata.contratos_docs_urls')
    logger.info('PDFs urls obtained from mysql')
    available_pdfs = available_pdfs[available_pdfs['is_active']!='1']
    available_pdfs = available_pdfs[::-1]
    available_pdfs = available_pdfs.to_dict('records')
    processes = [Process(target=write_document_to_s3, args=(url,)) for url in available_pdfs]
    tasks = len(available_pdfs)
    logger.info(f'Created {tasks} tasks')
    return processes

def contract_files_upload_to_s3_paralel():
    tasks_list = split_tasks_files_to_s3()
    execute_processes_list_in_batches(tasks_list,6)
