
from config_path import PATH_SOURCE
import time, pandas as pd, os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox .service import Service as FirefoxService
from webdriver_manager.firefox  import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def get_data_from_html(soup):
    response=[]
    for res in soup.find_all('eui-card-header-subtitle'):
#         print(res.find_all('span')[0].text)
#         print(res.find_all('strong')[0].text)
#         print(res.find_all('strong')[1].text)
        response.append({'topic_code':res.find_all('span')[0].text, 
                        'type':res.find_all('span')[2].text, 
                        'open_date':res.find_all('strong')[0].text, 
                        'deadline':res.find_all('strong')[1].text})
    return response

def click_next(b):
    x = b[2] # 3e element qui convient (après test)
    c = x.find_element(By.CLASS_NAME, 'eui-button')
    c.click()


def get_topic_info_europa(FRAMEWORK):

    if FRAMEWORK=='H2020':
        url = f'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals?order=DESC&pageNumber=50&pageSize=0&sortBy=startDate&isExactMatch=true&status=31094503&frameworkProgramme=31045243'

    if FRAMEWORK=='HORIZON':
        status='31094501,31094502,31094503'
        type_='1,8'
        # status='31094503'
        # type_='1'
        url = f'https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals?order=DESC&pageNumber=50&pageSize=0&sortBy=startDate&isExactMatch=true&type={type_}&status={status}&frameworkProgramme=43108390'
    
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
    driver.maximize_window()
    driver.get(url)
    time.sleep(5)  

    wait = WebDriverWait(driver, 1)
    wait.until(EC.presence_of_element_located((By.ID,'cookie-consent-banner')))
    cookie = driver.find_element(By.CLASS_NAME, 'wt-ecl-button')
    cookie.click()

    data = []
    # accepter les cookies à la main

    for i in range(0, 100):
        html_page = driver.page_source
        soup = BeautifulSoup(html_page, 'lxml')
        counter = soup.find('div', class_='eui-paginator__page-range').get_text().strip().replace(',','').replace('–',' ').split(' ')
        print(counter)
        new_data = get_data_from_html(soup)
        print(len(new_data))
        data += new_data
        if counter[-3] == counter[-1]:
            break
        print('---')
        b = driver.find_elements(By.CLASS_NAME, 'eui-paginator__page-navigation-item')
        click_next(b)
        time.sleep(5)
    #     except:
    #         wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#cdk-overlay-0 button.btn"))).click()
    #         driver.execute_script("arguments[0].click()", driver.find_element_by_css_selector("#StudentSatisfactionPop button.btn"))

    pd.to_pickle(data, open(f"{PATH_SOURCE}{FRAMEWORK}/topic_info_harvest.pkl", 'wb'))