from src.paralel import *
from src.extract import *
# Update data from first 300 pages for all categories
for i in range(0,100):
    run_paralel(split_tasks_extract_metadata('contratos',5000,5000*i))