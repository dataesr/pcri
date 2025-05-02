
from config_path import PATH_SOURCE, PATH_WP
import time, pandas as pd, requests, fitz, re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox .service import Service as FirefoxService
from webdriver_manager.firefox  import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def wp_load(url, year, files_to_load):
  
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
    driver.maximize_window()
    driver.get(url)
    time.sleep(5)

    wait = WebDriverWait(driver, 1)
    wait.until(EC.presence_of_element_located((By.ID,'cookie-consent-banner')))
    cookie = driver.find_element(By.CLASS_NAME, 'wt-ecl-button')
    cookie.click()

    links = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
    for link in links:
        for k,v in files_to_load.items():
            if k in link.get_attribute("href"):
                href = link.get_attribute("href")
                r = requests.get(href, allow_redirects=True)
                open(f"{PATH_WP}{year}/{v}.pdf", 'wb').write(r.content)


def calls_by_wp(url, wp_year, load_wp=False):
    from constant_vars import FRAMEWORK

    files_to_load = {"infrastructures":"infra", 
                    "cluster1":"health",
                    "cluster2":"cluster2",
                    "cluster3":"cluster3",
                    "cluster4":"cluster4",
                    "cluster5":"cluster5",
                    "cluster6":"cluster6",
                    "eie":"eie",
                    "widera":"widera",
                    "missions":"mission"
                    }

    if load_wp==True:
        wp_load(url, wp_year, files_to_load)


    calls_by_wp=[]
    for k,v in files_to_load.items():
        doc = fitz.open(f"{PATH_WP}{wp_year}/{v}.pdf")
        all_text = chr(12).join([page.get_text() for page in doc])
        all_text = all_text.replace('\n','')
        all_text = all_text.strip()
        # print(all_text)
        match = re.search(r"table of contents(.*?)budget", all_text, re.DOTALL | re.IGNORECASE)
        if match:
            result = match.group(1).strip()
            search_text = r'HORIZON-[^:\s\\\/\)]*'
            l=re.findall(search_text, result)
            l=list(set(l))
            res={'year':wp_year,
            'wp':v,
            'calls':list(set(l))}
            calls_by_wp.append(res)
    pd.to_pickle(pd.DataFrame(calls_by_wp), open(f"{PATH_SOURCE}{FRAMEWORK}/calls_by_wp.pkl", 'wb'))


def get_data_from_html(soup):
    response=[]
    for res in soup.find_all('eui-card-header-subtitle'):
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