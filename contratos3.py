from src.paralel import *
from src.extract import *
from tqdm import tqdm
# Update data from first 300 pages for all categories
for i in tqdm(range(0,36)):
    run_paralel(split_tasks_extract_metadata('contratos',357152 - 5000*i,5000))