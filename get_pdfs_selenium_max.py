
from src.paralel import *
from src.extract import *
# Update data from first 300 pages for all categories
for i in tqdm(range(80,117)):
    contract_files_upload_to_s3_paralel_selenium('contratos',limit = 5000, offset = 5000*i)