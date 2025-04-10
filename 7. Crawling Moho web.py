# -*- coding: utf-8 -*-
"""
Created on Thu Apr 10 15:20:00 2025

@author: aston
"""

import pandas as pd
import numpy as np
import datetime as dt
import re
import gspread
import csv
import glob
from collections import namedtuple
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options 

import os
current_path = os.get_cwd()
#%% 1. GET ALL LINK
Categories = []
Productlinks = []


options = Options()
options.binary_location = current_path+ "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path+ "/data/chromedriver.exe", ) 


driver.set_window_size(1920, 1080)
c = 0
# list_room = ['phong-khach', 'phong-an', 'phong-ngu', 'phong-lam-viec']
for i in range(1, 20):  # range(1, 20)
    driver.get('https://moho.com.vn/collections/tat-ca-san-pham-moho?page=' + str(i))  
    sleep(2)
        
    SCROLL_PAUSE_TIME = 1
    last_height = driver.execute_script("return document.body.scrollHeight")
        
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    driver.implicitly_wait(1)
    items = driver.find_elements_by_xpath('//h3[@class="pro-name"]') #items = driver.find_elements_by_xpath('//*[@id="main"]/div/div[3]/div/div[2]/div/div[2]/div')
    for item in items:   
        link = link = item.find_element_by_xpath('.//a').get_attribute('href')
        Productlinks.append(link)
        c+= 1
        print('-Link_product completed: {}'.format(c))
        sleep(1)
    # if len(Productlinks) != len(set(Productlinks)):
    #     break    
driver.quit()

Productlinks = list(dict.fromkeys(Productlinks))


link_check = 'https://moho.com.vn/products/ghe-sofa-moho-dalumd-301-mau-nau-180'                   
if link_check in Productlinks:
    print('yes')



df_p_moho = pd.read_excel(current_path+ "/data/Product_Link_Moho.xlsx")

df_p_moho['link_p'] = df_p_moho['link_p'].str.strip()

df_p_moho['link_p'] = df_p_moho['link_p'].apply(lambda x: x.replace("'", ""))
df_p_moho['link_p'] = df_p_moho['link_p'].apply(lambda x: x.replace(",", ""))                         

                          
add_link = df_p_moho['link_p'].tolist()

Productlinks += add_link


Productlinks = list(set(Productlinks))

df = pd.DataFrame({'link_p': Productlinks})




df.to_csv(current_path+ "/data/Data_Crawling_Marketing_ALL_Link.csv",
            encoding = 'utf-8-sig', index = False)

# CHECK LINK

check_link = pd.read_csv(current_path+ "/data/Data_Crawling_Marketing_ALL_Link.csv")
link_check = 'https://moho.com.vn/products/ghe-sofa-moho-dalumd-301-mau-nau-180'                   
if link_check in check_link['link_p'].tolist():
    print('yes')
                   
print('Number of link_p: {}'.format(len(df['link_p'].tolist())))  
print('                       ')      
print('                       ')   
print('     COMPLETED CRAWLING ALL LINK')      


#%%
#%% 2. GET LINK OUTLET
Categories = []
Productlinks = []


options = Options()
options.binary_location = current_path+ "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path+ "/data/chromedriver.exe", ) 

driver.set_window_size(1920, 1080)
c = 0
# list_room = ['phong-khach', 'phong-an', 'phong-ngu', 'phong-lam-viec']
for i in range(1, 10):  # range(1, 20)
    driver.get('https://moho.com.vn/collections/uu-dai?page=' + str(i))  
    sleep(2)
        
    SCROLL_PAUSE_TIME = 1
    last_height = driver.execute_script("return document.body.scrollHeight")
        
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    driver.implicitly_wait(1)
    items = driver.find_elements_by_xpath('//h3[@class="pro-name"]') #items = driver.find_elements_by_xpath('//*[@id="main"]/div/div[3]/div/div[2]/div/div[2]/div')
    for item in items:   
        link = link = item.find_element_by_xpath('.//a').get_attribute('href')
        Productlinks.append(link)
        c+= 1
        print('-Link_product completed: {}'.format(c))
        sleep(1)
    # if len(Productlinks) != len(set(Productlinks)):
    #     break    
driver.quit()

Productlinks = list(dict.fromkeys(Productlinks))


link_check = 'https://moho.com.vn/products/ghe-an-go-cao-su-tu-nhien-moho-soro-601'                   
if link_check in Productlinks:
    print('yes')

df_p_moho = pd.read_excel(current_path+ "/data/Product_Link_Moho.xlsx")

add_link = df_p_moho['link_p'].tolist()


Productlinks += add_link

df = pd.DataFrame({'link_p': Productlinks})


df['link_p'] = df['link_p'].str.strip()

df['link_p'] = df['link_p'].apply(lambda x: x.replace("'", ""))
df['link_p'] = df['link_p'].apply(lambda x: x.replace(",", ""))





df.to_csv(current_path+ "/data/Data_Crawling_Marketing_OUTLET.csv",
            encoding = 'utf-8-sig', index = False)

# CHECK LINK

check_link = pd.read_csv(current_path+ "/data/Data_Crawling_Marketing_OUTLET.csv")
link_check = 'https://moho.com.vn/products/ghe-sofa-moho-dalumd-301-mau-nau-180'                   
if link_check in check_link['link_p'].tolist():
    print('yes')
                   
print('Number of link_p: {}'.format(len(df['link_p'].tolist())))  
print('                       ')      
print('                       ')   
print('     COMPLETED CRAWLING ALL LINK')      





#%% 2. CRAWL VARIANT ID OF EACH LINK
df = pd.read_csv(current_path+ "/data/Data_Crawling_Marketing_ALL_Link.csv")
df2 = pd.read_csv(current_path+ "/data/Data_Crawling_Marketing_OUTLET.csv")
                 
df = pd.concat([df, df2])
                 
Productlinks = list(dict.fromkeys(df['link_p'].tolist()))

# link_check = 'https://moho.com.vn/products/ghe-sofa-moho-dalumd-301-mau-nau-180'                   
# if link_check in Productlinks:
#     print('yes')




options = Options()
options.binary_location = current_path+ "/data/Application/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path+ "/data/chromedriver.exe", ) # https://chromedriver.chromium.org/home

driver.set_window_size(1920, 1080)

link_test= ['https://moho.com.vn/products/bo-ban-an-go-cao-su-tu-nhien-moho-vline-602-75cm',
            'https://moho.com.vn/products/bo-ban-an-go-6-ghe-go-moho-moss-601',
            'https://moho.com.vn/products/bo-ban-an-4-ghe-go-moho-moss-601']


n_p = len(link_test)
data = []
c = 0
c_1 = 0
for link in Productlinks:   # Productlinks
    driver.get(link)

    driver.implicitly_wait(1)  # 10                              
    lst_var = []
    # lst_var2 = []

        
    for v in driver.find_elements_by_xpath("//select[@id='product-select']/option"):
        try:
            lst_var.append(v.get_attribute("value"))
        # lst_var2.append(v.get_attribute("innerText"))
        except:
            None
        

       
    dict_p = {'link': link, 'var': lst_var} # 'link_img': img  , 'var2': lst_var2
    data.append(dict_p)
    c+=1
    print('- Product completed: {}'.format(c))

driver.quit()

df = pd.DataFrame(data)


df = pd.DataFrame(df).explode(['var'])
df.drop_duplicates(subset = ['var'], inplace = True)

def fix_link(r):
    temp1 = r['link'].split('?variant=')[0]
    temp2 = r['var']
    return str(temp1) + '?variant=' + str(temp2)
df['link_variant'] = df.apply(fix_link, axis = 1) 


df = df[['link_variant']].drop_duplicates()

df.to_csv(current_path+ "/data/Data_Crawling_Marketing_ALL_Link_V.csv",
          encoding = 'utf-8-sig', index = False)


lst_link_v = list(dict.fromkeys(df['link_variant'].tolist()))

print('                       ')      
print('                       ')   
print('     COMPLETED CRAWLING LINK VARIANT')      

#%% 3. CRAWL INFO PRODUCT:
    
df = pd.read_csv(current_path+ "/data/Data_Crawling_Marketing_ALL_Link_V.csv")
        
Productlinks = list(dict.fromkeys(df['link_variant'].tolist()))

# if 'https://moho.com.vn/products/ghe-an-go-cao-su-tu-nhien-moho-soro-601?variant=1094148109' in Productlinks:
#     print('Yes')

lst_decor = []
for i in Productlinks:
    for j in ['nem-cao-su', 'nem-lo-xo']:
        if j in i:
            lst_decor.append(i)

Productlinks = [x for x in Productlinks if x not in lst_decor]


options = Options()
options.binary_location = current_path+ "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path+ "/data/chromedriver.exe", ) # https://chromedriver.chromium.org/home

driver.set_window_size(1920, 1080)



link_test = ['https://moho.com.vn/products/full-house-moho-koster-mau-nau-cam?variant=1117566538']

n_p = len(Productlinks)
data = []
c = 0
c_1 = 0
for link in Productlinks:   # Productlinks
    driver.get(link)
    driver.implicitly_wait(1)
    try:
        sku = driver.find_element(By.XPATH,"//span[@id='pro_sku']").get_attribute("innerText")
    except:
        sku = None
    try:
        sale_price = driver.find_element(By.XPATH,"//span[@class='pro-price']").get_attribute("innerText")
    except:
        sale_price = None
        
    try:
        price = driver.find_element(By.XPATH,"//div[@class='product-price']/del").get_attribute("innerText")
    except:
        price = None
        print('- Price is missing: {}'.format(link))
        
        
    try:
        status = driver.find_element(By.XPATH,"//span[@class='pro-soldold']").get_attribute("innerText")
    except:
        status = None
        
    
    try:
        Product_name = Product_name =  driver.find_elements_by_xpath('.//div[@class="product-title"]/h1')[0].text
    except:
        Product_name = None
        
        
    des = []
    for t in driver.find_elements_by_xpath('//span[@style="font-size: 10pt;"]'):  
        des.append(t.text)
    for t in driver.find_elements_by_xpath('//div[@class="pro-short-desc"]/p'):  
        des.append(t.text)
    
 

        
    dict_p = {'SKU': sku, 'price': price, 'sale_price': sale_price,'description': des, 
              'Product_name': Product_name, 'link': link, 'status': status} # 'link_img': img
    data.append(dict_p)
    c+=1
    c_1+= float(1*100/n_p) 
    print('- Product completed: {}, ({:.0f}%)'.format(c, c_1))

driver.quit()

df_1 = pd.DataFrame(data)


df_1['Variant'] = df_1['link'].apply(lambda x: x.split('variant=')[1])

df_1.to_csv(current_path+ "/data/Data_Crawling_Marketing_Phase_1.csv", encoding = 'utf-8', index = False)

### SAVE STATUS OUT OF STOCK ON WEBSITE
df_s = df_1[['SKU', 'status']].drop_duplicates(subset = ['SKU'])
df_s = df_s.loc[~df_s['SKU'].isnull()]
df_s['SKU'] = df_s['SKU'].apply(lambda x: x.split('SKU: ')[1])      
df_s.to_csv(current_path+ "/data/SKU_Website_Status.csv", encoding = 'utf-8', index = False)


print('                 ')
print('                 ')
print('         --- CRAWLING DATA MOHO WEBSITE COMPLETED ---          ')
