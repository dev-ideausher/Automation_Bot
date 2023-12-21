import json
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import time
from urllib.parse import urlparse
import pickle
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apify_client import ApifyClient
import json
import pandas as pd
from gspread.utils import rowcol_to_a1
from collections import Counter

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/bhavy/Desktop/Projects-IU/extra/pr/marketing-automations-401806-124a6a502fd9.json', scope)
gc = gspread.authorize(credentials)

spreadsheet = gc.open('FUTR_COMPANY PROFILES NEEDED_102523')
worksheet = spreadsheet.worksheet('321 Targets - NEED PROFILE DATA')

data = worksheet.get_all_records()
Company_URL = [row['Website'] for row in data]

header_row=worksheet.row_values(1)

industries_index=header_row.index("Industries")+1
num_emp_index=header_row.index("Number of Employees")+1
funding_status_index=header_row.index("Funding Status")+1
stock_symbol_index=header_row.index("Stock Symbol")+1
stock_URL_index=header_row.index("Stock Symbol URL")+1
stock_exchange_index=header_row.index("Stock Exchange")+1



API_KEY="apify_api_U7OUZWko4KidCl8Qh1cpQzMc6xQqnH45Yah7"

# Initialize the Apify client
client = ApifyClient(API_KEY)

def data_extract(url): 
 run_input = {
    "action": "findCompanyByWebsite",
    # "search.url": "https://www.crunchbase.com/discover/organization.companies/e2b9ad6d513ac5d0e7965bd16bd02327",
    "count": 1,
    "cursor": "",
    "minDelay": 1,
    "maxDelay": 3,
    "findCompanyByWebsite.urls": [url],
    "proxy": {
        "useApifyProxy": True,
        "apifyProxyGroups": ["RESIDENTIAL"],
    },
     "cookie":[
    {
        "domain": ".linkedin.com",
        "expirationDate": 1704610682,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_gcl_au",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "1.1.1947613475.1695212993",
        "id": 1
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1702449673.539417,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_guid",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "920f931e-1738-47e7-a395-f2ff3ef74e6a",
        "id": 2
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1701324517,
        "hostOnly": False,
        "httpOnly": False,
        "name": "aam_uuid",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "82254743986360339151757355452115584050",
        "id": 3
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1714284505,
        "hostOnly": False,
        "httpOnly": False,
        "name": "AMCV_14215E3D5995C57C0A495C55%40AdobeOrg",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "-637568504%7CMCIDTS%7C19662%7CMCMID%7C82804114781167750881777882014242895865%7CMCAAMLH-1699337305%7C12%7CMCAAMB-1699337305%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1698739705s%7CNONE%7CvVersion%7C5.1.1%7CMCCIDH%7C-1186355866",
        "id": 4
    },
    {
        "domain": ".linkedin.com",
        "hostOnly": False,
        "httpOnly": False,
        "name": "AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": True,
        "storeId": "0",
        "value": "1",
        "id": 5
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1701320360.387352,
        "hostOnly": False,
        "httpOnly": False,
        "name": "AnalyticsSyncHistory",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "AQLTezKECPBWmQAAAYuEGbMkfEO82CqzEZ-aZTkDnJM4kYg34aNnFSv1q3fJwR0R7-Vauq5Vkx7yFX9T-3U6uA",
        "id": 6
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1730268534.432927,
        "hostOnly": False,
        "httpOnly": False,
        "name": "bcookie",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "\"v=2&e058897e-2107-420f-835b-125b63a01b71\"",
        "id": 7
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1711182051,
        "hostOnly": False,
        "httpOnly": False,
        "name": "gpv_pn",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "www.linkedin.com%2Flearning%2Fchoose-your-go-framework-chi-router-fasthttp-fiber-echo-gin-gonic-go-kratos",
        "id": 8
    },
    {
        "domain": ".linkedin.com",
        "hostOnly": False,
        "httpOnly": False,
        "name": "lang",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": True,
        "storeId": "0",
        "value": "v=2&lang=en-us",
        "id": 9
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1706508533.432838,
        "hostOnly": False,
        "httpOnly": False,
        "name": "li_sugr",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "7183a758-f43d-418b-817c-7fe124c61259",
        "id": 10
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1729679357.413675,
        "hostOnly": False,
        "httpOnly": False,
        "name": "liap",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "True",
        "id": 11
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1698734082.770165,
        "hostOnly": False,
        "httpOnly": False,
        "name": "lidc",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "\"b=OB74:s=O:r=O:a=O:p=O:g=3759:u=131:x=1:i=1698732533:t=1698734082:v=2:sig=AQHujZDI-K7JuuxfHiUZxrpED6SxDlVy\"",
        "id": 12
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1701320360.77971,
        "hostOnly": False,
        "httpOnly": False,
        "name": "lms_ads",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "AQFAuQjU3HjLpwAAAYuEGbSQQl5tYLSPYJ_sr4ytrSMNdpi1Y0W0nuhEcIcCY6w8oE8lK_jqI1Cu1cjRNCItWpnBWZFFT5VE",
        "id": 13
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1701320360.779809,
        "hostOnly": False,
        "httpOnly": False,
        "name": "lms_analytics",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "AQFAuQjU3HjLpwAAAYuEGbSQQl5tYLSPYJ_sr4ytrSMNdpi1Y0W0nuhEcIcCY6w8oE8lK_jqI1Cu1cjRNCItWpnBWZFFT5VE",
        "id": 14
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1711182051,
        "hostOnly": False,
        "httpOnly": False,
        "name": "s_ips",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "836.6000000238419",
        "id": 15
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1711182051,
        "hostOnly": False,
        "httpOnly": False,
        "name": "s_tp",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "3019",
        "id": 16
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1711182051,
        "hostOnly": False,
        "httpOnly": False,
        "name": "s_tslv",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "1695630051341",
        "id": 17
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1701324532,
        "hostOnly": False,
        "httpOnly": False,
        "name": "UserMatchHistory",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "AQJBv2qxUj9vawAAAYuEWWMbHmWDXnxUuNfYLDnG_5jJyqXUDDFlk9Vpd3lHCTvTS8KhqdVq8iU3iQ0C5k6O0NmOlYs_Y3kFiTYiCOfZFXQI6o4E3u88v_i51fCNLJr_TtXbejhvX_3pSJoWBJ-pRXfje0v_Js5CJJ4FPgBCic1u5E5Ay31PfXAX9h7yBPk90JGmEYu6rshwImxNFN0OKXiwzf1YKLOsl-TDLiKSKlSojS8FFT4Mu7225rvSH_p_GWYZmis2hKwYkmTpxa87vOooPMNxEPM-m8io-eYu8KbPmPHQwZKlfd7F5JzMM7pZuJHAHxHov2OmaWct6qr-atwXWaD3Wqw",
        "id": 18
    },
    {
        "domain": ".linkedin.com",
        "expirationDate": 1729775854.241212,
        "hostOnly": False,
        "httpOnly": False,
        "name": "visit",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "v=1&M",
        "id": 19
    },
    {
        "domain": ".www.linkedin.com",
        "expirationDate": 1730268223.118569,
        "hostOnly": False,
        "httpOnly": True,
        "name": "bscookie",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "\"v=1&202309041206558ef19004-e6f4-4ba1-89e6-ad2762c3da07AQFoYQ_Z1b-vDAtI7CsDQcQpku8LfayG\"",
        "id": 20
    },
    {
        "domain": ".www.linkedin.com",
        "expirationDate": 1729679357.413741,
        "hostOnly": False,
        "httpOnly": False,
        "name": "JSESSIONID",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "\"ajax:4420809962200569563\"",
        "id": 21
    },
    {
        "domain": ".www.linkedin.com",
        "expirationDate": 1729679357.413515,
        "hostOnly": False,
        "httpOnly": True,
        "name": "li_at",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "AQEDATc0zfIEp4TDAAABirK9TEsAAAGLhUfJRE0Arppt4AMm_CpgEOoAw1TE_4b_Of1riirocrN_ngrvUrPvPYzg5Ieelqsy69j0swNsFWBDPRGVmJnWnqyDY9c9Nt2PZ0qEVjexgjqmrxtop3etIOrs",
        "id": 22
    },
    {
        "domain": ".www.linkedin.com",
        "expirationDate": 1726751865.561673,
        "hostOnly": False,
        "httpOnly": True,
        "name": "li_rm",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "AQH6AcDjU7Vu2wAAAYqXr85fgH4PO0i3NXQvDQRcSR2dNqfbSVQATfTBCxfXeKzQl2LH87WEoOQrjUoaMEOqnCpzcQCofZzf-sw_QmEipLupdCDIQyIPV0yx",
        "id": 23
    },
    {
        "domain": ".www.linkedin.com",
        "expirationDate": 1714284531,
        "hostOnly": False,
        "httpOnly": False,
        "name": "li_theme",
        "path": "/",
        "sameSite": "unspecified",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "light",
        "id": 24
    },
    {
        "domain": ".www.linkedin.com",
        "expirationDate": 1714284531,
        "hostOnly": False,
        "httpOnly": False,
        "name": "li_theme_set",
        "path": "/",
        "sameSite": "unspecified",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "app",
        "id": 25
    },
    {
        "domain": ".www.linkedin.com",
        "expirationDate": 1699942131,
        "hostOnly": False,
        "httpOnly": False,
        "name": "timezone",
        "path": "/",
        "sameSite": "unspecified",
        "secure": True,
        "session": False,
        "storeId": "0",
        "value": "Asia/Calcutta",
        "id": 26
    },
    {
        "domain": "www.linkedin.com",
        "expirationDate": 1710767864,
        "hostOnly": True,
        "httpOnly": False,
        "name": "g_state",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "{\"i_l\":0}",
        "id": 27
    }
]
}

# Run the Actor and wait for it to finish
 run = client.actor("curious_coder/crunchbase-scraper").call(run_input=run_input)
 max_source_categories = []
 
# Fetch and print Actor results from the run's dataset (if there are any)
 for item in client.dataset(run["defaultDatasetId"]).iterate_items():
  try:
   total = None
   row_index=None
   for index,row in enumerate(data):
    if row['Website'] == url:
      row_index=index+2

   if row_index is not None:
    root = item['org_similarity_list']
    for i in root:
     if 'source_categories' in i:
        source_categories = i['source_categories']
        if isinstance(source_categories, list):
            if len(source_categories) > len(max_source_categories):
                max_source_categories = source_categories
    if max_source_categories:
    # Store all values in a variable separated by commas
     industry_values = ", ".join(category.get('value', 'No value') for category in max_source_categories)
     if industry_values:
      worksheet.update_cell(row_index,industries_index,industry_values)
    else:
     print("No source categories found.")


    stock_data=(item['ipo_fields'])
    if stock_data:
     stock_symbol=stock_data['stock_link']['label']
     x=stock_symbol.split(":")
     stock_exchange= x[0]
     if stock_exchange:
      worksheet.update_cell(row_index,stock_exchange_index,stock_exchange)
     stock_symbol=x[1]
     if stock_symbol:
      worksheet.update_cell(row_index,stock_symbol_index,stock_symbol)
     stock_URL=stock_data['stock_link']['value']
     if stock_URL:
      worksheet.update_cell(row_index,stock_URL_index,stock_URL)


    num_employees_enum=(item["num_employees_enum"])
    lower_bound, upper_bound = map(int, num_employees_enum.split('_')[1:])
    human_readable_range = f"{lower_bound} - {upper_bound}"
    if human_readable_range:
     worksheet.update_cell(row_index,num_emp_index,human_readable_range)

    for i in item["recommended_search"]:
     if "organization" in i["queries"]:
        organization_queries = i["queries"]["organization"]
        
        for query in organization_queries:
            if "funding_stage" in query.get("field_id", []):
                funding_stage = query.get("values", [])[0]
                if funding_stage:
                  worksheet.update_cell(row_index,funding_status_index,funding_stage)
  except Exception as e:
    print(e)
    pass




for website in Company_URL:
 print(website)
 data_extract(website)