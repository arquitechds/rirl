from src.paralel import *
from src.extract import *
# Update data from first 300 pages for all categories
run_paralel(split_tasks_extract_metadata('reglamentos',66460))
run_paralel(split_tasks_extract_metadata('asociaciones',14280))
run_paralel(split_tasks_extract_metadata('contratos',500000))