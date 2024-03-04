from src.extract import *


for i in range(1,10):
    soup = get_raw_entries_by_page(i)
    a,b,c,d = extract_all_entries(soup)
    print(a)
    print(b)
    print(c)
    print(d)