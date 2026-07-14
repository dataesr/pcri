
from paths import PATH_HARVEST, PATH_WP
import time, pandas as pd, requests, fitz, re, os, json, pdfplumber
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

    elements = driver.find_elements(By.XPATH, '//a[@data-wt-preview="pdf" and @href and @data-untranslated-label]')

    # Loop through elements and check data-untranslated-label content
    for el in elements:

        label = el.get_attribute("data-untranslated-label")
        href = el.get_attribute("href")
        print(f"{label} -> {href}")
        for key, value in files_to_load.items():
            if key in re.sub(r"\s+", "", label).lower():
                r = requests.get(href, allow_redirects=True)
                open(f"{PATH_WP}{year}/{value}.pdf", 'wb').write(r.content)
                break  # Stop checking once matched

def normalize_code(code: str) -> str:
    # Remove ALL whitespace inside the matched token and uppercase
    return re.sub(r"\s+", "", code).upper()

def base_call_from_code(code: str):
    # Base call = first 4 segments: HORIZON-<PART>-<YEAR>-<CALLID>
    BASE_CALL_RE   = re.compile(r"^HORIZON-[A-Z0-9]+-\d{4}-\d{1,3}$", re.IGNORECASE)
    parts = code.split("-")
    if len(parts) >= 4 and parts[0] == "HORIZON" and parts[2].isdigit() and parts[3].isdigit():
        base = "-".join(parts[:4])
        if BASE_CALL_RE.match(base):
            return base
    return None

def read_early_text(pdf_path: str, max_pages: int = 25) -> str:
    # TOC + destinations are always near the beginning; this is faster and usually enough.
    doc = fitz.open(pdf_path)
    try:
        pages = min(max_pages, doc.page_count)
        return "\n".join(doc.load_page(i).get_text("text") for i in range(pages))
    finally:
        doc.close()

def harvest_calls_topics(text_block: str):
    """
    Build calls with topics by grouping all longer codes under their base call,
    even if the base call never appears alone in the TOC.
    """
    CODE_SPACED_RE = re.compile(r"\bHORIZON(?:\s*-\s*[A-Z0-9]+)+\b", re.IGNORECASE)
    calls_map = {}  # base_call -> {"call", "topics", "budget_year"}


    for raw in CODE_SPACED_RE.findall(text_block):
        code = normalize_code(raw)
        base = base_call_from_code(code)
        if not base:
            continue

        if base not in calls_map:
            calls_map[base] = {
                "call": base,
                "topics": [],
                "budget_year": int(base.split("-")[2]),
            }

        # Any longer code that starts with base + "-" is a "topic-like" entry
        if code != base and code.startswith(base + "-"):
            calls_map[base]["topics"].append(code)

    # Deduplicate topics (preserve order) and sort calls by call id
    calls = []
    for base in sorted(calls_map.keys()):
        seen = set()
        topics = []
        for t in calls_map[base]["topics"]:
            if t not in seen:
                seen.add(t)
                topics.append(t)
        calls_map[base]["topics"] = topics
        calls.append(calls_map[base])

    return calls
    
# def calls_by_wp(url, wp_year, load_wp=False):
def get_topics_by_wp(url, wp_year, max_pages: int = 25, load_wp:bool=False):

    files_to_load = {"infrastructures":"infra", 
                    "cluster1":"health",
                    "cluster2":"cluster2",
                    "cluster3":"cluster3",
                    "cluster4":"cluster4",
                    "cluster5":"cluster5",
                    "cluster6":"cluster6",
                    "europeaninnovationecosystems":"eie",
                    "widera":"widera",
                    "missions":"mission"
                    }

    if load_wp==True:
        wp_load(url, wp_year, files_to_load)


    # topics_by_wp=[]
    # for k,v in files_to_load.items():
    #     doc = fitz.open(f"{PATH_WP}{wp_year}/{v}.pdf")
    #     all_text = chr(12).join([page.get_text() for page in doc])
    #     all_text = all_text.replace('\n','')
    #     all_text = all_text.strip()
    #     # print(all_text)
    #     match = re.search(r"table of contents(.*?)budget", all_text, re.DOTALL | re.IGNORECASE)
    #     if match:
    #         result = match.group(1).strip()
    #         search_text = r'HORIZON-[^:\s\\\/\)]*'
    #         l=re.findall(search_text, result)
    #         l=list(set(l))
    #         res={'year':wp_year,
    #         'wp':v,
    #         'topics':list(set(l))}
    #         topics_by_wp.append(res)
    # pd.to_pickle(pd.DataFrame(topics_by_wp), open(f"{PATH_WP}topics_by_wp_{wp_year}.pkl", 'wb'))


    records = []
    for _, wp in files_to_load.items():
        path = f"{PATH_WP}{wp_year}/{wp}.pdf"
        if not os.path.exists(path):
            print(f"[MISS] {wp}: {path}")
            records.append({"year": None, "wp": wp, "calls": []})
            continue

        print(f"[READ] {wp}: {path}")
        text = read_early_text(path, max_pages=max_pages)
        calls = harvest_calls_topics(text)

        # Year column: if you want a single value, take the minimum year found in calls (or None)
        # years = sorted({c["budget_year"] for c in calls})
        # year_val = years[0] if years else None

        records.append({
            "year": wp_year,
            "wp": wp,
            "calls": calls
        })

        print(f"   -> calls: {len(calls)} | topics total: {sum(len(c['topics']) for c in calls)}")

    df = pd.DataFrame(records)

    pd.to_pickle(df, open(f"{PATH_WP}topics_by_wp_{wp_year}.pkl", 'wb'))
    # pd.to_pickle(df, open(f"{PATH_WP}topics_by_wp_{wp_year}.pkl", 'wb'))

