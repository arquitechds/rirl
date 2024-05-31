from src.paralel import *
from src.extract import *
# Update data from first 300 pages for all categories
run_paralel(split_tasks_extract_metadata('reglamentos',66460))
