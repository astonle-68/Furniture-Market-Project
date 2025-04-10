# -*- coding: utf-8 -*-
"""
Created on Thu Aug 17 14:21:46 2023

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

Brand = 'Comehome'
now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = now1.strftime("%d.%m.%y")

import os
current_path = os.getcwd()

#%%
options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)
c= 0


# lst_r = ['https://comehome.com.vn/kitchen-dining.html', 'https://comehome.com.vn/livingroom.html', 'https://comehome.com.vn/bedroom.html',
#           'https://comehome.com.vn/workspace.html', 'https://comehome.com.vn/children.html', 
#           'https://comehome.com.vn/accessories.html', 'https://comehome.com.vn/storage-x-home.html'] # ENG

lst_r = ['https://comehome.com.vn/phong-khach.html', 'https://comehome.com.vn/phong-ngu.html', 'https://comehome.com.vn/bep-phong-an.html',
         'https://comehome.com.vn/phong-lam-viec.html', 'https://comehome.com.vn/danh-cho-be.html', 'https://comehome.com.vn/phu-kien.html',
         'https://comehome.com.vn/giai-phap-luu-tru.html']


link_all = []
for i in lst_r:
    driver.get(i)
    # links = driver.find_elements_by_xpath('//div[@class="product-item-info"]')  # link không cố định chỉ 1 link, nhiều link lặp lại
    # # links = wait.until(EC.visibility_of_element_located((By.XPATH, '//div[@class="product-item-info"]')))
    sleep(5)
    
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
    
    element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div//button[@class='action-close']"))) # REMOVE POP_UP
    driver.execute_script("arguments[0].click();", element) #OK
    i = 0
    
    while i in range(0, 8):
        i += 1
        if i <=8:
            try:
                element = driver.find_element_by_xpath("(//div[contains(@id, 'ias_trigger_')])")  # CLICK SPAN
                driver.execute_script("arguments[0].click();", element)  # OK
                
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
    links = driver.find_elements_by_xpath('//li[@class="item product product-item"]')  
    sleep(1)
    for item in links:   
        sub_link = sub_link = item.find_element_by_xpath('.//a').get_attribute('href')  
        link_p.append(sub_link)
    link_all.append(link_p)
driver.quit()  
   

result = list(itertools.chain.from_iterable(link_all))

result = list(dict.fromkeys(result))

df = pd.DataFrame(data= result, columns = ['link_p'])



#%%
#%%
gc = gspread.service_account(filename= current_path + '/data/project-aftersale-moho-cd1338f28ec9.json')
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

worksheet = sh.get_worksheet(7)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

# df_1 = df.loc[df.iloc[:,-1] == 'X']
# last_col = (df.columns)[-1]
# check = df.loc[df.iloc[:,-1] != 'X']
link_old = df_ggs['link_p'].tolist()

lst_sku = list(dict.fromkeys(df_ggs['SKU'].tolist()))

link_new = list(dict.fromkeys(sorted(set(result) - set(link_old))))

print('''---------------NUMBER NEW LINK COME_HOME:  {}'''.format(len(link_new)))

if len(link_new) == 0:
    print('''NO NEED TO UPDATE NEW PRODUCT IN COME_HOME''')
    
print('--------------------------------------------------------------')
print('--------------------------------------------------------------')
print('---------------- COMPLETED PHASE I ---------------')
print('--------------------------------------------------------------')
print('--------------------------------------------------------------')

df = pd.DataFrame(data = link_new, columns = ['link_p'])
df.to_excel(current_path + '''/data/Link_product_Come_Home.xls''')         
#%% CRAWL DESCRIPTION:

df_links = pd.read_excel(current_path + '''/data/Link_product_Come_Home.xls''')  
Productlinks = df_links['link_p'].tolist()           

options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)
set_speed = 1


wait = WebDriverWait(driver, set_speed)
data = []

n_p = len(Productlinks) # Productlinks # link_new
c = 0
c_1 = 0
e = 0



link_test = ['https://comehome.com.vn/wroclaw-ghe-sofa-3-cho.html'] # sp đơn lẻ

Link_error = []
for link in link_test:  #Productlinks
    try:
        driver.get(link)
        sleep(set_speed)
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
    
        try:
            Collection = driver.find_element(By.XPATH,"//span[@itemprop='name']").get_attribute("innerText")  # ok
        except:
            Collection = None
        try:
            price = driver.find_element_by_xpath('//meta[@itemprop="price"]').get_attribute('content')
        except:
            price = None

        try:
            types = driver.find_element(By.XPATH,"//div[@itemprop='product-type']").get_attribute("innerText") #ok
        except:
            types = None
            
        try:
            
            sku = driver.find_element_by_xpath('//div[@class="product-add-form"]//form').get_attribute('data-product-sku')
        except:
            sku = None
            
            
        try:
            material = driver.find_element(By.XPATH,"//div[@data-thumb-height='90']").get_attribute("innerText")  # OK
        except:
            material = None

        try:
            dimension = driver.find_element(By.XPATH,"//div[@class='swatch-attribute dimension']//div[@data-thumb-height='90']").get_attribute("innerText") # ok
        except:
            dimension = None
            
            
        lst_dimension = []
        for d in driver.find_elements_by_xpath('//div[@class="swatch-attribute dimension"]//div[@class="swatch-option text"]'):
            try:
                d = d.get_attribute("data-option-label")
            except:
                d = None
            lst_dimension.append(d)
        lst_color = []
        for c in driver.find_elements_by_xpath('//div[@class="swatch-attribute colour"]//div[@class="swatch-option image"]'): # ok
            try:
                c = c.get_attribute("data-option-label")
            except:
                c = None
            lst_color.append(c)
   
        
           
        dict_p = {'SKU': sku, 'Collection': Collection, 'Price': price, 'Product_Types': types, 'Material': material, 'Dimension': lst_dimension, 'Color': lst_color,
                  'link_p': link}  #  
        data.append(dict_p)
        c+=1 
        c_1+= float(1*100/n_p) 
        print('- Product completed: {}, {}'.format(c, c_1))
    except:
        # None
        e += 1
        print('- Link error: {}, ({})'.format(link, e))
        Link_error.append(link)


driver.quit()
df = pd.DataFrame(data)            
print(df)


df = df.loc[~(df['SKU'].isnull() & df['Collection'].isnull() & df['Price'].isnull())]

df['SKU'].fillna(df['Collection'], inplace = True)


sku_check = df['SKU'].tolist()

sku_new = list(dict.fromkeys(sorted(set(sku_check) - set(lst_sku))))

print('''---------------NUMBER NEW SKU COME_HOME:  {}'''.format(len(sku_new)))

df = df.loc[df['SKU'].isin(sku_new)]

df.to_excel(current_path + '/data/Data_Comehome_Web_Phase_I.xlsx', index = False)
#%% ETL DATA
df = pd.read_excel(current_path + '/data/Data_Comehome_Web_Phase_I.xlsx')
                   
                 
 #FILL TYPES
df_sp_etl = pd.read_excel(current_path + '''/data/Sp_ETL_Types_COMEHOME.xlsx''')
# df_sp_etl = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Sp_ETL_Types_BAYA (BACK-UP).xlsx''') # BACKUP               
   
df_sp_etl = df_sp_etl[['Types', 'SKU']].drop_duplicates()

df_sp_etl['SKU'] = df_sp_etl['SKU'].astype(str)

test = df.merge(df_sp_etl, how = 'left', on = 'SKU')
       
check_types_NULL = test.loc[test['Types'].isnull()][['SKU', 'Types', 'Product_Types', 'link_p']].drop_duplicates(subset = 'SKU')
                
print('--------------------------')
print('--------------------------')
print('TYPES IS NULL: {}'.format(check_types_NULL))
print('--------------------------')
print('--------------------------')



if check_types_NULL.size == 0:
    df = df.merge(df_sp_etl, how = 'left', on = 'SKU').drop(['Product_Types'], axis = 1)
    
#%% FIX PRICE

df['Price'] = df['Price'].astype(str)

def fix_price(r):
    lst_special = "['.đ|,']"
    temp = r['Price']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Price'] = df.apply(fix_price, axis = 1)



print('---------------------------------------------------------')
print('------------------Final Test after ETL:------------------')



#%% ADD Room 
df['Room'] = ''

dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room', 'Pallet Bed', 'Set Bed Room', 'Storage Bed', 'Bed Accessories'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room', 'set 4 chairs', 'Drop-Leaf Table'], #ok
            'Kitchen': ['Kitchen storage', 'Kitchen cabinet', 'Set Kitchen cabinet'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Set TV Cabinet', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa Bed', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater', 
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Set Storage Cabinet', 'Set Living Room', 'Set Sofa Table', 'Shoes racks',
                            'Rocking Chair', 'Bench With Storage', 'Children Armchair', 'Set sofa table & chair', 'Chaise Lounge', 'Bean Bag', 'Multi-Function Table',
                            'Sofa Accessories', 'Glass Storage', 'Living Room Set Cabinet', 'Sofa 4 seaters', 'Student table',
                            'Children desk chair'],
            'Office': ['Desk', 'Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room','Book shelf', 'Desk Accessories'],
            # 'Other': ['None', 'Gift Voucher', 'Other'],    
            'Outdoor': ['Set Outdoor Table', 'Outdoor Table', 'Folding Chair - Outdoor', 'Swing chair', 'Outdoor Chair', 'Outdoor stool'],
            
            
            'Bath Room': ['Bath Storage', 'Set Bath Room'],
            'Decoration/ Accessory': ['Blanket/ Pillow', 'Decoration Mirror', 'Decoration Accessories', 'Kitchen Accessory', 
                                      'Painting', 'Carpet', 'Candle', 'Pot', 'Lamp', 'Folding Chair', 
                                      'Other', 'Seat Cushions', 'Decoration/ Accessory', 'Accessories', 'Wall shelf', 'Toy', 'Folding Table'],
            'Children Room': ['Children table', 'Children chair','Changing Table', 'Children Desk Set', 'Children Book Store','Cot', 'Children Bed',
                              'Set Children table']} #, 'Table Accessory'

df_room = pd.DataFrame(dict_all.items(), columns=['Room', 'Types'])

df_room.to_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Sp_ETL_ALL_Competitors.xlsx''', index = False)



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


df['Brand'] = 'Comehome'
df['Date_updated'] = current_date

df.rename({'Size': 'Dimension'}, axis = 1, inplace = True)

# df['Product_name'] = df['Product_name'].str.title()


df = df.drop_duplicates(subset = 'SKU')



df.rename({'Sku':'SKU', 'Product_description': 'Description'}, axis = 1, inplace = True)

month_save = now1.strftime("%B-%Y")
df[month_save] = 'X'



df['Dimension'] =  df['Dimension'].astype(str)

for col in ['Dimension']:
    df['Dimension'] = df['Dimension'].str[1:-1]

def fix_d(r):
    lst_special = "['.đ|']"
    temp = r['Dimension']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Dimension'] = df.apply(fix_d, axis = 1)
df['Dimension'] = df['Dimension'].str.replace('centimetre', ' cm')

def fix_c(r):
    lst_special = "['.đ|']"
    temp = r['Color']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Color'] = df.apply(fix_c, axis = 1)

df['Color'] = df['Color'].str.title()

df['Types'] = df['Types'].str.title()

df['Product_name'] = df['Types'] + ' ' + df['Collection']
df['Date_get_new_product'] = current_date





lst_col = list(df_ggs.columns)
col_new = set(lst_col) - set(df.columns)

for i in col_new:
    df[i] = np.nan



df = df[lst_col]


df.to_excel(current_path + '''/data/Data_''' + Brand + '_FINAL_' + today +'.xlsx', index = False)





         
#%%
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











