# -*- coding: utf-8 -*-
"""
Created on Thu Apr 28 10:58:18 2022

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
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC

import os
current_path = os.get_cwd()

now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")

df_old = pd.read_excel(current_path + "/data/Data_IndexLivingMall_Web_Phase_I.xlsx")
                 
                       
print('------------------------------------------')
print('------------------------------------------')
print('-----------Number of Product_link OLD: {}'.format(len(list(dict.fromkeys(df_old['link_p'].tolist())))))
print('------------------------------------------')
print('------------------------------------------')


#%% 1. GET link Room

options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

Brand = 'Index_LivingMall'
data = []
Categories = []
Productlinks = []

driver.get('https://indexlivingmallvn.com/rooms')
# links = 

link_room = []
data_room = []
for i in driver.find_elements_by_xpath('//a[@class="item"]'):
    link = link = i.get_attribute('href')  # ok
    link_room.append(link)
driver.quit()


room = []
for i in link_room:  
    room.append(i.replace('https://indexlivingmallvn.com/rooms/', ''))

res = {room[i]: link_room[i] for i in range(len(room))}

df_r = pd.DataFrame(list(res.items()),columns = ['Room','link_r']) 

#%% 2. GET link Types
options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

link_types = []
data_types = []
for i in link_room:
    driver.get(i)
    sleep(1)
    links = driver.find_elements_by_xpath('//a[@class="item"]') 
    for types in links:
        item = item = types.get_attribute('href')  # ok
        link_types.append(item)
        dict_types = {i: item}
        data_types.append(dict_types)
driver.quit()
   
df_t = pd.DataFrame([(k, v) for d in data_types for k, v in d.items()], 
                  columns=['link_r','link_types'])      

df_1 = df_r.merge(df_t, how = 'left', on ='link_r')
         

 # remove link for types:
lst_types= []
data_t = []
for i1 in link_room:
    for i2 in link_types:
        if i1 in i2:
            temp = i2.replace(str(i1) + '/', '')
            lst_types.append(temp)
            dict_types = {i2: temp}
            data_t.append(dict_types)

df_types = pd.DataFrame([(k, v) for d in data_t for k, v in d.items()], 
                  columns=['link_types','Types'])    
            
df = df_1.merge(df_types, how = 'left', on = 'link_types')

def fill_null_types(r):
    temp1 = r['link_types']
    temp2 = r['Types']
    if pd.isna(temp2) and temp1 == 'https://indexlivingmallvn.com/theo-phong/living-room/coffee-side-table':
        return 'coffee-side-table'
    elif pd.isna(temp2) and temp1 == 'https://indexlivingmallvn.com/catalog/category/view/s/bathroom-accessories/id/382/':
        return 'bathroom-accessories'
    else:
        return temp2

df['Types'] = df.apply(fill_null_types, axis = 1)

df.info()   

#%% 3. Get Product link:
options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

link_p = []
data = []


c = 0
link_test = ['https://indexlivingmallvn.com/rooms/living-room/sofa']
for i in link_types: # link_types
    driver.get(i)
    sleep(1)
    link_product = driver.find_elements_by_xpath('//div[@class="swiper-slide product-list-item item product"]') 
    for p in link_product:
        item = item = p.find_element_by_xpath('.//a').get_attribute('href')  # ok
        link_p.append(item)
        dict_types = {i: item}
        data.append(dict_types)
        c+=1
        print('- Product link completed: {}'.format(c))
driver.quit()

df_p = pd.DataFrame([(k, v) for d in data for k, v in d.items()], 
                  columns=['link_types','link_p'])  



df = df.merge(df_p, how = 'left', on = 'link_types').drop(['link_r', 'link_types'], axis = 1)

print('------------------')
print('------------------')
print('------------- \ Check lỗi df_Room_Types_Product null / -------------')  
df.info()
#%%
gc = gspread.service_account(filename=current_path + "/data/project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4')

worksheet = sh.get_worksheet(3)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

# df_1 = df.loc[df.iloc[:,-1] == 'X']
# last_col = (df.columns)[-1]
# check = df.loc[df.iloc[:,-1] != 'X']
link_old = df_ggs['link_p'].tolist()



link_new = list(dict.fromkeys(sorted(set(Productlinks) - set(link_old))))
print('                                                   ')

print('------------------NEW LINK PRODUCT: {}-------------'.format(len(link_new)))

if len(link_new) == 0:
    print('                                                   ')
    print('         NO NEED CONTINUE PHASE II   ')


print('----------------------------')
print('----------------------------')
print('-----COMPLETED PHASE I------')
print('----------------------------')
print('----------------------------')
#%% 4. GET Info product:
options = Options()
options.binary_location = current_path + "/data/chrome.exe" # 
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

c = 0
c_1 = 0
n_p = len(link_p)
data_product = []
link_test = ['https://indexlivingmallvn.com/ghe-thu-gian-riley-mau-nau-370000062', 'https://indexlivingmallvn.com/rorial-area-rug-60x90cm-dbl-170112218',
             'https://indexlivingmallvn.com/carpenter-triangle-side-table-76-cm-wn-110026441', 'https://indexlivingmallvn.com/sofa-goc-l-trai-moretto-mau-nau-nhat-110030248']
for i in link_p: #link_p
    driver.get(i)
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
    driver.implicitly_wait(1)
    
    try:
        sku = sku = driver.find_elements_by_xpath('.//div[@itemprop="sku"]')[0].text
    except:
        sku = None

    try:
        Product_name = Product_name =  driver.find_elements_by_xpath('.//span[@itemprop="name"]')[0].text # ok
    except:
        Product_name = None
    # lst_price = []    
    # try:
    #     price = driver.find_element(By.XPATH,"//span[@class='price']").text  # ok    
    #     lst_price.append(price)                   
    # except:
    #     price = None
    try:
        price = driver.find_element(By.XPATH,"//div[@class='product-info-price']//span[@data-price-type='finalPrice']").text
    except:
        price = None
        try:
            price = driver.find_element(By.XPATH,"//div[@class='product-info-price']//span[@class='finalprice finalprice-price']").text
        except:
            price = None
            print('MISSING PRICE')
            print('Link Missing price: {}'.format(i))

    try:
        material = driver.find_element(By.XPATH,"//div[@data-th='Chất liệu']").text 
    except:
        material = None
    try:
        color = driver.find_element(By.XPATH,"//div[@data-th='Màu sắc']").text 
    except:
        color = None
    try:
        dimension = driver.find_element(By.XPATH,"//div[@data-th='Kích thước']").text 
    except:
        dimension = None
    try:
        collection = driver.find_element(By.XPATH,"//div[@data-th='Bộ sưu tập']").text 
    except:
        collection = None
    try:
        style = driver.find_element(By.XPATH,"//div[@data-th='Phong cách']").text 
    except:
        style = None
        
    elems = driver.find_elements_by_xpath(('//div[@class="product media"]'))    # GET IMAGE TO HDD
    for elem in elems:
        time.sleep(2)
        try:
            sub_link = sub_link = elem.find_element_by_xpath('.//img').get_attribute('src')   
            urllib.request.urlretrieve(sub_link, current_path + "/data/Index_Living_Mall_" + str(sku) +  '_' + str(current_date) + '.jpg') # save image
        except:
            print('Link error: {}'.format(i))
        
    dict_p = {'SKU': sku, 'Product_name': Product_name, 'Price': price, 'Material': material, 'Color': color, 
              'Dimension': dimension, 'Collection': collection, 'Style': style, 
              'link_p': i} # 'Product_description': product_description
    data_product.append(dict_p)
    c+=1
    c_1+= float(1*100/n_p) 
    print('Product completed: {}, ({:.2f}%)'.format(c, c_1))
driver.quit()
    
df_product = pd.DataFrame(data_product)
       
df_product.info()
    

df = df.merge(df_product, how = 'left', on = 'link_p')
#%% #%% ETL Room
df['Brand'] = Brand
df['Date_updated'] = current_date
def fix_room(r):
    temp = r['Room']
    if 'living-room' in temp:
        return 'Living Room'
    elif 'bedroom' in temp:
        return 'Bed Room'
    elif 'dining-room' in temp:
        return 'Dining Room'
    elif 'bathroom' in temp:
        return 'Bath Room'  
    elif 'kitchen' in temp:
        return 'Kitchen'    
    elif 'outdoor' in temp:
        return 'Outdoor'
    else:
        return temp
df['Room'] = df.apply(fix_room, axis = 1)

df.info()

#%% FILL NULL PRICE: 
# adjust_time_wait = 1  
    
    
# null_price = df.loc[df['Price'].isnull()][['link_p']]
# link_fill_price = list(dict.fromkeys(null_price['link_p'].tolist()))

# PATH = "C:/Users/aston/Documents/playground_python/chromedriver.exe" # trang update: https://chromedriver.chromium.org/ -> last version -> chromedriver_win32.zip
# driver = webdriver.Chrome(PATH)  
# c = 0
# c_1 = 0
# n_p = len(link_fill_price)
# data_product = []
# link_test = ['https://indexlivingmallvn.com/ghe-thu-gian-lemma-mau-den-370000063']
# data_price = []
# for i in link_fill_price: # link_fill_price
#     driver.get(i)
#     sleep(adjust_time_wait)
#     SCROLL_PAUSE_TIME = adjust_time_wait
#     last_height = driver.execute_script("return document.body.scrollHeight")
    
#     while True:
#         driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#         sleep(SCROLL_PAUSE_TIME)
#         new_height = driver.execute_script("return document.body.scrollHeight")
#         if new_height == last_height:
#             break
#         last_height = new_height
#     driver.implicitly_wait(adjust_time_wait)
#     try:
#         price = driver.find_element(By.XPATH,"//div[@class='product-info-price']//span[@data-price-type='finalPrice']").text
#     except:
#         price = None
#         try:
#             price = driver.find_element(By.XPATH,"//div[@class='product-info-price']//span[@class='finalprice finalprice-price']").text
#         except:
#             price = None
#             print('MISSING PRICE')
#             print('Link Missing price: {}'.format(i))


            
#     dict_p = {'link_p': i, 'Price': price} # 'Product_description': product_description
#     data_price.append(dict_p)
#     c+=1
#     c_1+= float(1*100/n_p) 
#     print('Product completed: {}, ({:.2f}%)'.format(c, c_1))
# driver.quit()
    
# df_fill_price = pd.DataFrame(data_price)

           

df.to_excel(current_path + "/data/Data_IndexLivingMall_Web_Phase_I.xlsx", 
            index = False) 

print('----------------------------------------')
print('----------------------------------------')
print('-----COMPLETED CRAWL DESCRIPTION--------')
print('----------------------------------------')
print('----------------------------------------')

#%% ETL DATA
import ast
import pandas as pd
import numpy as np
from datetime import datetime
Brand = 'Index LivingMall'
today = pd.to_datetime('now').strftime("%d.%m.%y")

df = pd.read_excel(current_path + "/data/Data_IndexLivingMall_Web_Phase_I.xlsx")   
                         
df = df.loc[df['link_p'].notnull()]   
df.drop_duplicates(subset = 'link_p', inplace =True)                
# FIX PRICE
        
def fix_price(r):
    lst_special = "['₫|.']"
    temp = r['Price']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Price'] = df.apply(fix_price, axis = 1)


#%% FIX TYPES:  

check_room = df['Room'].value_counts()                
check_types = df['Types'].value_counts()                   
  
               
def get_str(r):
    temp = r['Product_name'].split(' ')[:3]
    return ' '.join(temp)
df['Check_Name'] = df.apply(get_str, axis = 1)

check_name = df['Check_Name'].value_counts()                 
    

check = df[['Product_name', 'Check_Name']].drop_duplicates(subset = 'Check_Name')


check = check.sort_values(by = 'Check_Name')
# check.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Sp_ETL_Types_Beyous.xlsx", index = False)
   
df_fix_Types = pd.read_excel(current_path + "/data/Sp_ETL_Types_IndexLivingMall.xlsx")    
df_fix_Types = df_fix_Types[['Check_Name', 'Types']]
                             
df = df.merge(df_fix_Types, how = 'left', on = 'Check_Name')
print('-----------------------------------------------------------')
print('Số lượng Types null: {}'.format(df['Types_y'].isnull().sum()))
print('-----------------------------------------------------------')
check_Types_null = df.loc[df['Types_y'].isnull()][['Check_Name', 'Product_name', 'link_p']]

#%% Add Room
df['Room'] = ''

dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room', 'Pallet Bed', 'Dressing Chair'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 'Kitchen cabinet', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room'], #ok
            'Kitchen': ['Kitchen storage'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Set TV Cabinet','Book shelf', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa Bed', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater', 
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Corner table', 'Console table'
                            , 'Glass Cabinet', 'Shoes Racks'],
            'Office': ['Desk', 'Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room'],
            # 'Other': ['None', 'Gift Voucher', 'Other'],    
            'Outdoor': ['Set Outdoor Table', 'Outdoor Table', 'Folding Chair - Outdoor', 'Swing chair', 'Outdoor Chair', 'Outdoor Desk', 'Outdoor Bench'],
            
            
            'Bath Room': ['Bath Storage'],
            'Decoration/ Accessory': ['Blanket/ Pillow', 'Decoration Mirror', 'Decoration Accessories', 'Kitchen Accessory', 'Painting', 'Carpet', 'Candle', 'Pot', 'Lamp', 
                                      'Other', 'Seat Cushions', 'Decoration/ Accessory', 'Accessories', 'Wall shelf']} #, 'Table Accessory'

def fix_room(r):
    temp = r['Room']
    temp_1 = r['Types_y'].lower()
    for key, values in dict_all.items():
        for i in values:
            if temp_1 == i.lower():
                return key
            continue
        continue
    return temp
df['Room'] = df.apply(fix_room, axis = 1)  

print('-----------------------------------------------------------')
print('Số lượng Room null: {}'.format(df['Room'].isnull().sum()))
print('-----------------------------------------------------------')
check_Room_null = df.loc[df['Room'].isnull()][['Types_y', 'Room', 'link_p']]


df.rename({'Types_y': 'Types'}, axis = 1, inplace = True)

df.drop(['Types_x'], axis = 1, inplace = True)


df['Brand'] = 'Index Living Mall'

df.to_excel(current_path + "/data/Data_IndexLivingMall_FINAL.xlsx", index = False)

#%%
df.rename({'Sku':'SKU', 'Product_description': 'Description'}, axis = 1)

# month_save = now1.strftime("%B-%Y")
# df[month_save] = 'X'

new_product = df.copy()

lst_n = list(set(list(df_ggs.columns.tolist())) - set(list(df.columns.tolist())))
print(lst_n)
for i in lst_n:
    new_product[i] = ''

new_product.columns
new_product = new_product[df_ggs.columns]
new_product['Date_get_new_product'] = current_date 











