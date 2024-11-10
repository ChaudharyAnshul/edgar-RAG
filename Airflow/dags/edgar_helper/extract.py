import logging
from time import sleep
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import pandas as pd

from edgar_helper.utils.maps import table_class_map

logger = logging.getLogger("airflow.task") 


base_url = "https://www.sec.gov/edgar/search/#/q={query_word}&dateRange=1y&filter_forms=10-K"

SELENIUM_SERVER_URL = 'http://34.133.23.10:4444/wd/hub'

def scrape_condition_data(topicId):
  '''
  Index Scrapping
  '''
  url = base_url.format(query_word = topicId)
  print("start 2")
  option = Options()
  # option.add_argument('--headless')
  # option.add_argument('--disable-gpu')
  # option.add_argument('--no-sandbox')
  option.add_argument('--disable-dev-shm-usage')

  driver = webdriver.Remote(
    command_executor=SELENIUM_SERVER_URL,
    options=option
  )
  
  print("start 2")
  
  driver.get(url)
  sleep(3)
  
  count = 1
  row_list = []
  
  try:
    while True:
      logger.info(f"Scraping page {count}")
      
      content = driver.page_source
      parsed_content = bs(content, 'html.parser')
      hits_div = parsed_content.find("div", attrs={"id":"hits"})
      table = hits_div.find('table', class_='table')
      rows = table.find('tbody').find_all('tr')
      
      for i, row in enumerate(rows):
        try:
          temp = {}
          cols = row.find_all('td')
          logger.info(f"Scraping page {count} - row {i+1}")
          
          for col in cols:
            class_name = ' '.join(col.get('class', []))
            text_value = col.get_text(strip=True)
            temp[table_class_map[class_name]] = text_value
          
          link = row.find('a', class_='preview-file', href=True)
          link_href = link['href']
          link_text = link.text
          xpath = f"//a[@href='{link_href}' and contains(@class, 'preview-file') and text()='{link_text}']"
          
          click_element = driver.find_element(By.XPATH, xpath)
          click_element.click()
          
          iframe_id = 'ipreviewer'
          
          WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, iframe_id))
          )
          
          iframe_content = driver.page_source
          iframe_soup = bs(iframe_content, 'html.parser')
          
          a_tag = iframe_soup.find('a', {'id': 'open-file'})
          temp["report_link"] = a_tag.get("href")
          
          close_button = driver.find_element(By.XPATH, "//button[@class='close' and @data-dismiss='modal']")
          close_button.click()
          
          row_list.append(temp)
        except Exception as e:
          logger.error(f"Error while extraction row information: {e}", exc_info=True)
      
      tab_nav = parsed_content.find('nav', id='results-pagination')
      if 'display:none' in re.sub(r" ", "", tab_nav.get("style")):
        break      
      next_button = parsed_content.find('a', class_='page-link', attrs={'data-value': 'nextPage'})
      if not next_button:
        break
      elif next_button and next_button.get('tabindex', 0) == '-1':
        break
      next_button_element = driver.find_element(By.LINK_TEXT, 'Next page')
      next_button_element.click()
      sleep(3)
      count += 1
      
  except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
  finally:
    driver.quit()
  df = pd.DataFrame(row_list)
  return df

def scraping_edgar_data(topicId):
  results = scrape_condition_data(topicId)
  print(results.head())
  print(len(results))
  return "location"


if __name__ == "__main__":
  scraping_edgar_data(topicId = 'dermatitis')