# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 08:09:01 2022

@author: aston
"""

import pandas as pd
import numpy as np
import datetime as dt
import re
import gspread
import csv
import time
import urllib.request
import itertools
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC

Brand = 'Baya'
now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")

import os
current_path = os.get_cwd()

#%% 1. GET TYPES

# PATH = "C:/Users/aston/Documents/playground_python/chromedriver.exe" # trang update: https://chromedriver.chromium.org/ -> last version -> chromedriver_win32.zip
options = Options()
options.binary_location = current_path + "/data//chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data//chromedriver.exe", )
driver.set_window_size(1920, 1080)


data = []
Categories = []
Productlinks = []
set_speed = 5
wait = WebDriverWait(driver, set_speed)

link_Types = []


driver.get('https://baya.vn/')
for item in driver.find_elements_by_xpath('//ul[@class="menuList-primary"]/li[@class]'):      # //ul[@class="menuList-primary"]
    sub_link = sub_link = item.find_element_by_xpath('.//a').get_attribute('href')
    link_Types.append(sub_link)

driver.quit()

print(link_Types)




#%% 2. GET ALL PRODUCT LINKS:
options = Options()
options.binary_location = current_path + "/data//chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data//chromedriver.exe", )
driver.set_window_size(1920, 1080)
c= 0

Productlinks = []
link_test = ['https://baya.vn/collections/trang-tri-nha-cua']
count_page = 35
for i in link_Types: # link_Types
    driver.get(i) 
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
    options = Options()
    options.add_argument("--disable-notifications")   
    i = 0
    while i in range(0, 3):
        i += 1
        if i <=8:
            try:
                driver.find_element_by_xpath('//a[@class="button btn-loadmore"]').click()

                
                SCROLL_PAUSE_TIME = 1
                
                last_height = driver.execute_script("return document.body.scrollHeight")
                
                while True:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    sleep(SCROLL_PAUSE_TIME)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
            except:
                break
        continue
    link_p = []    
    for item in driver.find_elements(By.XPATH,  " //a[contains(@class, 'proloop-link quickview-product')]"):
        i = item.get_attribute('href')
        print(i)
        link_p.append(i)
    
    Productlinks.append(link_p)
driver.quit()

Productlinks = list(itertools.chain.from_iterable(Productlinks))
Productlinks = list(dict.fromkeys(Productlinks))
#%% 
gc = gspread.service_account(current_path + "/data//project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') 

worksheet = sh.get_worksheet(2)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

# df_1 = df.loc[df.iloc[:,-1] == 'X']
# last_col = (df.columns)[-1]
# check = df.loc[df.iloc[:,-1] != 'X']
link_old = df_ggs['link_p'].tolist()

df_all_link = pd.read_excel(current_path + "/data//Data_Baya_Web_All_Link.xlsx")

link_new = list(set(df_all_link['link_p'].tolist()) - set(Productlinks))


df = pd.DataFrame(data = Productlinks, columns = ['link_p'])
df.to_excel(current_path + "/data//Data_Baya_Web_All_Link.xlsx", index = False)

            

#%%          
#%% TEST NEW PRODUCT AND GET DES NEW PRODUCT

df = pd.read_excel(current_path + "/data//Data_Baya_Web_All_Link.xlsx")    
Productlinks = list(dict.fromkeys(df['link_p'].tolist()))    
    
options = Options()
options.binary_location = current_path + "/data//chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path="I:/Chromedriver/BETA/chromedriver.exe", )
driver.set_window_size(1920, 1080)
c = 0
c_1 = 0
n_p = len(Productlinks)
data = []

current_date = now1.strftime("%d-%m-%Y")

link_test = ['https://baya.vn/products/ghe-nam-connemara-1096293']
for link in Productlinks:  # Productlinks

    try:
        driver.get(link)
        sleep(1)
        
        SCROLL_PAUSE_TIME = 1
        last_height = driver.execute_script("return document.body.scrollHeight")
        
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
     
        driver.implicitly_wait(0.001)
    
            
        try:
            sku = sku =  driver.find_elements(By.XPATH, '//span[@id="pro_sku"]')[0].get_attribute('innerText')
        except:
            sku = None
        dict_p = {'SKU': sku, 'link_p': link} #'description': des
        data.append(dict_p)
            
        c += 1
        c_1+= float(1*100/n_p) 
        print('- Product completed: {}, ({:.2f}%)'.format(c, c_1))
    except:
        None
    # driver.stop_client()
    # driver.close()

driver.quit()

df = pd.DataFrame(data)
df['SKU'] = df['SKU'].apply(lambda x: x.replace('Mã sản phẩm: ', ''))

df.to_excel(current_path + "/data//Data_Baya_Web_All_Link_BACKUP.xlsx", index = False)


lst_old_sku = list(dict.fromkeys(df_ggs['SKU'].tolist()))
df = df.loc[~df['SKU'].isin(lst_old_sku)]


print(' ------------------------------------   ')
print(' ------------------------------------   ')
print('             NEW LINK - BAYA: {}'.format(df.shape[0]))

if df.shape[0] == 0:
    print("NO NEED TO UPDATE NEW PRODUCT IN BAYA")
print(' ------------------------------------   ')
print(' ------------------------------------   ')

df.to_excel(current_path + "/data//Data_Baya_Web_All_Link.xlsx", index = False)
            
#%%            
#%%            
#%% 3. GET INFO Product:
df = pd.read_excel(current_path + "/data/Data_Baya_Web_All_Link.xlsx")    
Productlinks = list(dict.fromkeys(df['link_p'].tolist()))    
    
options = Options()
options.binary_location = current_path + "/data/chrome.exe" # 
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)
c = 0
c_1 = 0
n_p = len(Productlinks)
data = []

current_date = now1.strftime("%d-%m-%Y")

link_test = ['https://baya.vn/products/hop-dung-ca-phe-felicia', 'https://baya.vn/products/tham-phong-tam-xing']
for link in Productlinks:  # Productlinks

    driver.get(link)
    vers = driver.find_elements(By.XPATH,"//div[@class='select-swap']/div/label")  #//div[@class='select-swap']//label
    for ver in vers: 
        try:
            ver.click()
        except:
            None
            
        try:
            sku = sku =  driver.find_elements(By.XPATH, '//span[@id="pro_sku"]')[0].get_attribute('innerText')
        except:
            sku = None       
        
        try:
            Product_name = Product_name =  driver.find_element_by_xpath('.//div[@class="product-heading"]/h1[1]').get_attribute("innerText")  # ok
        except:
            Product_name = None
        try:
            price = driver.find_element(By.XPATH,"//span[@class='pro-price']").text  # ok                       
        except:
            price = None
        try:
            collection = driver.find_element(By.XPATH,"//span[@class='pro-vendor']").text  # ok     
        except:
            collection = None
            
        try:
            color = driver.find_element(By.XPATH,"//div[@id='variant-swatch-0']/div").get_attribute("innerText")   
        except:
            color = None
        try:
            dimension = driver.find_element(By.XPATH,"//div[@id='variant-swatch-1']/div[2]").get_attribute('innerText')
        except:
            dimension = None
        try:
            material = driver.find_element(By.XPATH,"//div[@id='variant-swatch-2']/div[2]").get_attribute('innerText')
        except:
            material = None
            

        
        elems = driver.find_elements_by_xpath(('//a[@class="product-gallery__item"]')) # Dowload image to HDD
        for elem in elems:
            time.sleep(0.01)
            try:
                sub_link = sub_link = elem.find_element_by_xpath('.//img').get_attribute('src')   
                urllib.request.urlretrieve(sub_link, current_path + "/data/Baya_" + str(sku) +  '_' + str(current_date) + '.jpg') # save image
            except:
                print('Link error: {}'.format(link))
                   
        dict_p = {'Product_name': Product_name, 'SKU': sku,'Price': price ,'link_p': link,
                  'Collection': collection, 'Color': color, 'Dimension': dimension, 'Material': material} #'description': des
        data.append(dict_p)
            
        c += 1
        c_1+= float(1*100/n_p) 
        print('- Product completed: {}, ({:.2f}%)'.format(c, c_1))

driver.quit()

df = pd.DataFrame(data)
# print(df[['Color', 'Dimension', 'Material']])


# df2 = pd.json_normalize(df['Product_description'])

# df = df.merge(df2, how = 'left', left_index= True, right_index = True)


df['SKU'] = df['SKU'].apply(lambda x: x.replace('Mã sản phẩm: ', ''))

df['SKU'] = df['SKU'].apply(lambda x: x.strip())



print('--------------- ------------------------------- -----------------')            
            
print('--------------- COMPLETED PHASE II: DESCRIPTION - BAYA-----------------')            

print('--------------- ------------------------------- -----------------')         

new_sku = list(set(df['SKU'].tolist()) - set(df_ggs['SKU'].tolist()))
print(new_sku)

df = df.loc[~df['Product_name'].isnull()]
print(len(new_sku))
df = df.loc[df['SKU'].isin(new_sku)]

print('                                      ')
print("---------------NUMBER NEW BAYA PRODUCT: {} ----------".format(len(new_sku)))


print('--------------------------------------------------------------')
print('--------------------------------------------------------------')
print('----------COMPLETED BAYA PHASE I -------')
print('--------------------------------------------------------------')



df.to_excel(current_path + "/data/Data_Baya_Web_Phase_I.xlsx", index = False)
            #%% ETL DATA
#%% REMOVE DF NULL
gc = gspread.service_account(filename=current_path + "/data/project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

worksheet = sh.get_worksheet(2)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

df_ggs = df_ggs.drop_duplicates()




df = pd.read_excel(current_path + "/data/Data_Baya_Web_Phase_I.xlsx") 

          

#%% ADD Types
# df.rename({'Function': 'Types'}, axis = 1, inplace = True)


import unidecode
def fix_t(r):
    temp = unidecode.unidecode(r['Product_name']).lower()
    temp2 = temp.split(' ')[:4]

    return ' '.join([str(elem) for elem in temp2])
df['Types'] = df.apply(fix_t, axis = 1)
    




#%% FILL = HAND
# df_sp_etl.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Sp_ETL_Types_BAYA.xlsx", index = False)

df_sp_etl = pd.read_excel(current_path + "/data/Sp_ETL_Types_BAYA.xlsx")
# df_sp_etl = pd.read_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Sp_ETL_Types_BAYA (BACK-UP).xlsx") # BACKUP               
   
df_sp_etl = df_sp_etl[['Types', 'Fix_Types']].drop_duplicates()



test = df.merge(df_sp_etl, how = 'left', on = 'Types')
       
check_types_NULL = test.loc[test['Fix_Types'].isnull()][['Types', 'Fix_Types', 'link_p']].drop_duplicates(subset = 'Types')
                
print('--------------------------')
print('--------------------------')
print('TYPES IS NULL: {}'.format(check_types_NULL))
print('--------------------------')
print('--------------------------')



#%% FIX PRICE

df['Price'] = df['Price'].astype(str)

def fix_price(r):
    lst_special = "['.₫|,']"
    temp = r['Price']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Price'] = df.apply(fix_price, axis = 1)
print('---------------------------------------------------------')
print('------------------Final Test after ETL:------------------')



#%%
df['Price'] = df['Price'].astype(str)

for i in ['Collection', 'Color', 'Dimension']:
    df[i] = df[i].astype(str)



lst_str = ['Thương hiệu: ', 'Màu sắc:', 'Kích thước:', 'Kiểu dáng:']
def fix_c(r):
    temp = r[i]
    
    for i2 in lst_str:
        temp = temp.replace(i2, '')
    return temp

for i in ['Collection', 'Color', 'Dimension']:
    df[i] = df.apply(fix_c, axis = 1)
    df[i] = df[i].replace('nan', np.nan)    
    
df.drop_duplicates(subset = ['SKU'], inplace = True)

#%% ADD Room 
df['Room'] = ''

dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room', 'Pallet Bed', 'Set Bed Room'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room', 'set 4 chairs'], #ok
            'Kitchen': ['Kitchen storage', 'Kitchen cabinet'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Set TV Cabinet', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa Bed', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater', 
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Set Storage Cabinet', 'Set Living Room', 'Set Sofa Table',
                            'Rocking Chair', 'Bench With Storage', 'Children Armchair', 'Set sofa table & chair', 'Chaise Lounge', 'Student table',
                            'Children desk chair'],
            'Office': ['Desk', 'Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room','Book shelf'],
            # 'Other': ['None', 'Gift Voucher', 'Other'],    
            'Outdoor': ['Set Outdoor Table', 'Outdoor Table', 'Folding Chair - Outdoor', 'Swing chair', 'Outdoor Chair', 'Outdoor stool'],
            
            
            'Bath Room': ['Bath Storage', 'Set Bath Room'],
            'Decoration/ Accessory': ['Blanket/ Pillow', 'Decoration Mirror', 'Decoration Accessories', 'Kitchen Accessory', 
                                      'Painting', 'Carpet', 'Candle', 'Pot', 'Lamp', 
                                      'Other', 'Seat Cushions', 'Decoration/ Accessory', 'Accessories', 'Wall shelf', 'Toy']} #, 'Table Accessory'

def fix_room(r):
    temp = r['Room']
    try:
        temp_1 = r['Types'].lower()
        for key, values in dict_all.items():
            for i in values:
                if temp_1 == i.lower():
                    return key
                continue
            continue
        return temp
    except:
        None
df['Room'] = df.apply(fix_room, axis = 1)    

check_room_null = df.loc[df['Room'].isnull()][['Types', 'Room', 'link_p']].drop_duplicates(subset = 'Types')
                
print('--------------------------')
print('--------------------------')
print('Room IS NULL: {}'.format(check_room_null))
print('--------------------------')
print('--------------------------')



check_cols_len = list(df.columns[df.isnull().any()])
print('COLS THIẾU VALUES: {}'.format(check_cols_len))


df['Brand'] = Brand
df['Date_updated'] = current_date

df.rename({'Size': 'Dimension'}, axis = 1, inplace = True)

df['Product_name'] = df['Product_name'].str.title()


df = df.drop_duplicates(subset = 'SKU')

df.info()


df.rename({'Sku':'SKU', 'Product_description': 'Description'}, axis = 1, inplace = True)

month_save = now1.strftime("%B-%Y")
df[month_save] = 'X'

print(df)




df.to_excel(current_path + "/data/Data_" + Brand + '_FINAL_' + today +'.xlsx', index = False)

            
#
new_product = df.copy()

lst_n = list(set(list(df_ggs.columns.tolist())) - set(list(df.columns.tolist())))
print(lst_n)
for i in lst_n:
    new_product[i] = ''

new_product.columns
new_product = new_product[df_ggs.columns]
new_product['Date_get_new_product'] = current_date # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251
            
lst_sku_old = list(dict.fromkeys(df_ggs['SKU'].tolist()))
          

new_product['SKU'] = new_product['SKU'].astype(str)
new_product = new_product.loc[~new_product['SKU'].isin(lst_sku_old)]

new_product.info()

new_product.columns
new_product.rename({'': 'Brand'}, axis = 1, inplace = True)
new_product['Brand'] = 'Baya'


