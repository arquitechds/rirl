
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import *
from selenium.webdriver.common.by import By

import datetime as dt
import time
import os
import json
from loguru import logger

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



def selenium_download(url_prefix):
    ''' 
    
    '''
    download_dir = "directory"

    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        "download.default_directory": download_dir,  # Change default directory for downloads
        "download.prompt_for_download": False,  # To auto download the file
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True  # Enable safe browsing
    })


    # Init driver
    chrome_options.add_argument("--headless") # Runs Chrome in headless mode.
    driver = webdriver.Chrome(options=chrome_options)
    #driver.set_window_size(1500, 700)


    # Navigate to sie
    driver.get("https://repositorio.centrolaboral.gob.mx"+ url_prefix)
    # Evidence 1
    driver.save_screenshot('1-welcomepage.png')

    time.sleep(2)
    element =driver.find_elements(By.XPATH, "(//button[@class='btn btn-primary'])")[0]
    driver.execute_script("arguments[0].click();", element)
    driver.save_screenshot('2-loginpage.png')

    time.sleep(1)

    # Step 1: Store the current window handle
    original_window = driver.current_window_handle
    # Step 2: Execute the JavaScript click
    documents = driver.find_elements(By.XPATH, "//a[@class='data-tracking-document']")
    logger.info(f'detected {len(documents)}')
    for file in documents:
        try:
            file.click()
        except:
            time.sleep(1)
            file.click()

    documents = driver.find_elements(By.XPATH, "//button[@class= 'btn btn-primary btn-generar-liga data-tracking-document']")

    logger.info(f'detected {len(documents)}')
    for file in documents:
        try:
            file.click()
            time.sleep(1)

        except:
            time.sleep(1)
            file.click()
            time.sleep(1)
    driver.close()