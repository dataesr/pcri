import re, pandas as pd
import time
import math
from bs4 import BeautifulSoup
from paths import PATH_WP
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager


PAGE_SIZE = 50


def get_data_from_html(soup):
    response = []
    for res in soup.find_all("eui-card-header-subtitle"):
        spans = res.find_all("span")
        strongs = res.find_all("strong")
        if len(spans) >= 3 and len(strongs) >= 2:
            response.append(
                {
                    "topic_code": spans[0].text.strip(),
                    "type": spans[2].text.strip(),
                    "open_date": strongs[0].text.strip(),
                    "deadline": strongs[1].text.strip(),
                }
            )
    return response


def accept_cookies_if_present(driver, wait):
    try:
        wait.until(EC.presence_of_element_located((By.ID, "cookie-consent-banner")))
        btns = driver.find_elements(By.CSS_SELECTOR, "#cookie-consent-banner .wt-ecl-button, #cookie-consent-banner button")
        if btns:
            btns[0].click()
            time.sleep(0.3)
    except Exception:
        pass


def wait_paginator(driver, wait):
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.eui-paginator__page-range")))


def get_paginator_text(driver):
    return driver.find_element(By.CSS_SELECTOR, "div.eui-paginator__page-range").text.strip()


def parse_page_range(text):
    # e.g. "1–50 of 4,064"
    t = text.replace("–", "-").replace(",", "")
    m = re.search(r"(\d+)\s*-\s*(\d+)\s+of\s+(\d+)", t)
    if not m:
        return None, None, None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def build_url(framework, page_number):
    if framework == "H2020":
        return (
            "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals"
            f"?order=DESC&pageNumber={page_number}&pageSize={PAGE_SIZE}&sortBy=startDate&isExactMatch=true"
            "&status=31094503&frameworkProgramme=31045243"
        )

    if framework == "HORIZON":
        status = "31094501,31094502,31094503"
        type_ = "1,8"
        return (
            "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals"
            f"?order=DESC&pageNumber={page_number}&pageSize={PAGE_SIZE}&sortBy=startDate&isExactMatch=true"
            f"&type={type_}&status={status}&frameworkProgramme=43108390"
        )

    raise ValueError("framework must be 'H2020' or 'HORIZON'")


def wait_expected_range(driver, timeout_s, expected_start):
    """
    Wait until paginator starts with expected_start (e.g. 1, 51, 101, ...).
    This guarantees we are really on the right page.
    """
    end_time = time.time() + timeout_s
    while time.time() < end_time:
        try:
            txt = get_paginator_text(driver)
            start, end, total = parse_page_range(txt)
            if start == expected_start:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False
############################################################################
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

def wait_cards_list(driver, wait):
    # Cards list present
    wait.until(EC.presence_of_all_elements_located(
        (By.CSS_SELECTOR, "div.eui-card-header__title-container a.eui-u-text-link")
    ))

def get_card_links(driver):
    # Re-find every time (avoids stale elements after back())
    return driver.find_elements(By.CSS_SELECTOR, "div.eui-card-header__title-container a.eui-u-text-link")

def extract_call_identifier_from_detail(driver, wait):
    """
    Tries a few robust ways to get something like:
      'ENERGY (HORIZON-CL5-2027-07)'
    from the detail page.
    """
    # Ensure the detail page loaded something meaningful
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))

    # Strategy A: common pattern in this portal: a link that contains 'call' and the identifier in its text
    try:
        a = wait.until(EC.presence_of_element_located((
            By.XPATH,
            # link text often includes "HORIZON-..." and appears in the "Call" section
            "//a[contains(., 'HORIZON') or contains(., 'H2020') or contains(., 'Horizon') or contains(., 'ENERGY')]"
        )))
        txt = a.text.strip()
        if txt:
            return txt
    except TimeoutException:
        pass

    # Strategy B: find the "Call" label then the first link following it
    # Works if the page has a field label "Call" (dt) + value (dd) with an <a>
    try:
        a = wait.until(EC.presence_of_element_located((
            By.XPATH,
            "//*[normalize-space()='Call' or contains(normalize-space(), 'Call')]/following::a[1]"
        )))
        txt = a.text.strip()
        if txt:
            return txt
    except TimeoutException:
        pass

    # Strategy C: fallback – scan page source for something that looks like HORIZON-CLx-....
    html = driver.page_source
    import re
    m = re.search(r"([A-Z0-9 \-]+)\((HORIZON-[A-Z0-9\-]+|H2020-[A-Z0-9\-]+)\)", html)
    if m:
        return m.group(0).strip()

    return None

def harvest_call_identifiers_for_current_page(driver, wait):
    """
    Returns list aligned with cards order on the page:
      [call_identifier_for_card_0, call_identifier_for_card_1, ...]
    """
    wait_cards_list(driver, wait)

    call_ids = []
    n = len(get_card_links(driver))

    for i in range(n):
        # Re-find links each iteration to avoid stale references
        links = get_card_links(driver)
        if i >= len(links):
            break

        # Scroll into view + click
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", links[i])
        time.sleep(0.1)

        try:
            links[i].click()
        except StaleElementReferenceException:
            # Re-try once if it went stale mid-click
            links = get_card_links(driver)
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", links[i])
            links[i].click()

        # On detail page: extract
        call_id = extract_call_identifier_from_detail(driver, wait)
        call_ids.append(call_id)

        # Back to list
        driver.back()
        wait_cards_list(driver, wait)

    return call_ids



#############################################################################

def get_topic_from_eu_portal(framework, headless=False):
    options = webdriver.FirefoxOptions()
    if headless:
        options.add_argument("--headless")

    driver = webdriver.Firefox(
        service=FirefoxService(GeckoDriverManager().install()),
        options=options,
    )

    try:
        wait = WebDriverWait(driver, 25)

        # Load page 1
        driver.get(build_url(framework, 1))
        wait_paginator(driver, wait)
        accept_cookies_if_present(driver, wait)
        wait_paginator(driver, wait)

        # Compute total pages from paginator
        txt = get_paginator_text(driver)
        start, end, total = parse_page_range(txt)
        if total is None:
            raise RuntimeError(f"Could not parse paginator text: {txt}")

        total_pages = math.ceil(total / PAGE_SIZE)
        print(f"[Info] total={total}, page_size={PAGE_SIZE}, total_pages={total_pages}")

        results_by_code = {}

        for page in range(1, total_pages + 1):
            expected_start = (page - 1) * PAGE_SIZE + 1

            driver.get(build_url(framework, page))
            wait_paginator(driver, wait)

            # Make sure we truly landed on the requested page
            ok = wait_expected_range(driver, timeout_s=20, expected_start=expected_start)
            if not ok:
                # If the portal redirects/overrides, show what it actually loaded
                actual = get_paginator_text(driver)
                raise RuntimeError(f"Page {page}: expected start {expected_start}, but paginator is '{actual}'")

            # (A) your existing soup parsing
            soup = BeautifulSoup(driver.page_source, "lxml")
            rows = get_data_from_html(soup)

            # (B) NEW: get call identifiers by clicking each card
            call_ids = harvest_call_identifiers_for_current_page(driver, wait)

            # (C) NEW: merge call_ids into rows (same order as cards on that page)
            for idx, r in enumerate(rows):
                r["call_identifier"] = call_ids[idx] if idx < len(call_ids) else None

            # (D) your existing dict merge
            for r in rows:
                results_by_code[r["topic_code"]] = r

#################################################################
            # soup = BeautifulSoup(driver.page_source, "lxml")
            # rows = get_data_from_html(soup)
            # for r in rows:
            #     results_by_code[r["topic_code"]] = r
#################################################################


            print(f"[Page {page}/{total_pages}] {expected_start}-{min(expected_start+PAGE_SIZE-1, total)} of {total} | "
                  f"extracted {len(rows)} | unique {len(results_by_code)}")

        data = list(results_by_code.values())
        pd.to_pickle(data, open(f"{PATH_WP}topic_info_harvest.pkl", 'wb'))
        # return list(results_by_code.values())

    finally:
        driver.quit()



