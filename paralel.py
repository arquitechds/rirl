
from multiprocessing import Process
from src.jobs import single_extract_job
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



