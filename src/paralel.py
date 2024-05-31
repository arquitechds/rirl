
from multiprocessing import Process
from src.jobs import check_new_urls_for_control_tables,extract_metadata,write_document_to_s3
from loguru import logger
from tqdm import tqdm
# pararel for contracts
from src.db import *
from src.extract import *

# Paralel framework to run jobs in many tasks

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

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


def run_paralel(split_task_function):
    ''' 
    Executes the paralel run given a split_task_function
    '''
    tasks_list = split_task_function
    execute_processes_list_in_batches(tasks_list,8)





# split custom tasks 


def split_tasks_check_new_urls_for_control_tables(lowerp = 1,upperp= 10):
    # create all tasks
    processes = [Process(target=check_new_urls_for_control_tables, args=(i,)) for i in range(lowerp,upperp)]
    tasks = lowerp-upperp
    logger.info(f'Created {tasks} tasks')
    return processes

def split_tasks_extract_metadata(table, limit=100,offset=0):
    '''
    Either contract_table is current_contracts or historic_contracts
    '''
    # create all tasks
    available_urls = read_table(f'select * from control.{table} limit {limit} offset {offset}')
    available_urls = list(available_urls['url'])
    processes = [Process(target=extract_metadata, args=(url,)) for url in available_urls[0:limit]]
    tasks = len(available_urls)
    logger.info(f'Created {tasks} tasks')
    return processes




# pararel for pdfs
def split_tasks_files_to_s3():
    # create all tasks
    available_pdfs = read_table('select * from control.contratos_docs_urls')
    logger.info('PDFs urls obtained from mysql')
    available_pdfs = available_pdfs[available_pdfs['in_s3']=='0']
    available_pdfs = available_pdfs[::-1]
    available_pdfs = available_pdfs.to_dict('records')
    processes = [Process(target=write_document_to_s3, args=(url,)) for url in available_pdfs]
    tasks = len(available_pdfs)
    logger.info(f'Created {tasks} tasks')
    return processes

def contract_files_upload_to_s3_paralel():
    tasks_list = split_tasks_files_to_s3()
    execute_processes_list_in_batches(tasks_list,1)
