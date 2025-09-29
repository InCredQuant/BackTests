import time
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys 
from selenium.webdriver.support import expected_conditions as EC
import warnings
warnings.filterwarnings("ignore")
import os
from datetime import datetime

start_date = '2025-02-01'
end_date = '2025-02-28'
# path = rf"C:\\Vishwanath\\Data\\MCX_Bhavcopy\\{start_date[:4]}\\{datetime.strptime(start_date, '%Y-%m-%d').strftime('%b').upper()}"
path = ""

service = Service()
options = webdriver.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-sh-usage')

prefs = {
    'download.default_directory': path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing_for_trusted_sources_enabled": False,
    "safebrowsing.enabled": False
}

options.add_experimental_option('prefs', prefs)
driver = webdriver.Chrome(service=service, options=options)
actions = ActionChains(driver)

driver.get("https://www.mcxindia.com/market-data/bhavcopy")

date_range = pd.date_range(start=start_date, end=end_date)
weekdays = date_range[date_range.to_series().dt.dayofweek < 5]

for d in weekdays:
    print(d.date())
    s = driver.find_element("xpath","//*[@id='txtDate']")
    driver.execute_script("arguments[0].removeAttribute('readonly')", s)
    k = driver.find_element("id","txtDate")
    k.clear()
    if d.date().month > 9:
        k.send_keys(str(d.date().day) + "/" + str(d.date().month) + "/" + str(d.date().year))
    else:
        k.send_keys(str(d.date().day) + "/0" + str(d.date().month) + "/" + str(d.date().year))
    # k.send_keys(str(d.date().day) + "/0" + str(d.date().month) + "/" + str(d.date().year))
    actions.send_keys(Keys.ENTER)
    element = driver.find_element(By.ID, 'txtDate_hid_val')
    current_value = element.get_attribute('value')
    # print(f"Current Value: {current_value}")
    if d.date().month > 9 and d.date().day > 9:
        new_date = str(d.date().year) + str(d.date().month) + str(d.date().day)
    elif d.date().month > 9 and d.date().day < 10:
        new_date = str(d.date().year) + str(d.date().month) + "0" + str(d.date().day)
    elif d.date().month < 10 and d.date().day > 9:
        new_date = str(d.date().year) + "0" + str(d.date().month) + str(d.date().day)
    else:
        new_date = str(d.date().year) + "0" + str(d.date().month) + "0" + str(d.date().day)
    driver.execute_script("arguments[0].setAttribute('value', arguments[1]);", element, new_date)
    updated_value = element.get_attribute('value')
    # print(f"Updated Value: {updated_value}")
    b = driver.find_element("xpath","//*[@id='btnShowDatewise']").click()
    WebDriverWait(driver, 7).until(EC.element_to_be_clickable((By.XPATH, "//* [@id='lnkExpToCSV']"))).click()
    time.sleep(2)
driver.close()