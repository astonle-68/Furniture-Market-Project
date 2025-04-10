# -*- coding: utf-8 -*-
"""
Created on Thu May 12 13:53:37 2022

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
Brand = 'Make My Home'
now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")
#%% 1. GET link Room

options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

Brand = 'Make My Home'
data = []
Categories = []
Productlinks = []

#%% 1. GET link Room
driver.get('https://makemyhomevn.com/')
links = driver.find_elements_by_xpath('//div[@class="content-sub-menu"]//li')  

link_room = []
data_room = []
for i in links:
    item = item = i.find_element_by_xpath('.//a').get_attribute('href')  # ok
    link_room.append(item)
driver.quit()


lst_room = []
for i in link_room:
    if 'room' in i:
        lst_room.append(i)
lst_room = list(dict.fromkeys(lst_room))

print(lst_room)


room = []
for i in lst_room:  
    room.append(i.replace('https://makemyhomevn.com/collections/', ''))

res = {room[i]: lst_room[i] for i in range(len(room))}

df_r = pd.DataFrame(list(res.items()),columns = ['Room','link_r']) 

#%% 2. GET link Types
options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

link_types = []
data_types = []

link_test = ['https://makemyhomevn.com/collections/phong-khach-living-room']
for i in lst_room: #lst_room
    driver.get(i)
    sleep(1)
    links = driver.find_elements_by_xpath('//ul[@class="top-categories clearfix hidden-xs"]/li')  # ok
    for types in links:
        item = item = types.find_element_by_xpath('.//a').get_attribute('href')  # ok
        link_types.append(item)
        dict_types = {i: item} # item
        data_types.append(dict_types)
driver.quit()
   
df_t = pd.DataFrame([(k, v) for d in data_types for k, v in d.items()], 
                  columns=['link_r','link_types'])      

df_1 = df_r.merge(df_t, how = 'left', on ='link_r')

df_1.drop('link_r', axis = 1, inplace = True)

#%% 3. GET link Product
options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

link_p = []
data = []

c = 0
link_test = ['https://makemyhomevn.com/collections/combo-phong-khach'] # link_types
for i in link_types:   #link_types
    driver.get(i)
    sleep(1)
    link_product = driver.find_elements_by_xpath('//div[@class="col-md-3 col-sm-4 col-xs-6 product-wrap product-wrap1"]') # ok
    for p in link_product:
        item = item = p.find_element_by_xpath('.//a').get_attribute('href') 
        c+=1

        print('- Link Product completed: {}'.format(c))
        link_p.append(item)
        dict_types = {i: item}
        data.append(dict_types)
driver.quit()

df_p = pd.DataFrame([(k, v) for d in data for k, v in d.items()], 
                  columns=['link_types','link_p'])  



df_1 = df_1.merge(df_p, how = 'left', on = 'link_types')

#%% 3.1 GET  LINK_P TRONG LINK_P
lst_null_link = list(dict.fromkeys(df_1.loc[df_1['link_p'].isnull()]['link_types'].tolist()))

options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

link_p_1 = []
data = []
c = 0

link_test = ['https://makemyhomevn.com/collections/sofa',
             'https://makemyhomevn.com/collections/ban-desks']
for i in lst_null_link:   #lst_null_link
    driver.get(i)
    sleep(1)
    link_product = driver.find_elements_by_xpath('//ul[@class="parents-categories clearfix"]/li')  # ok
    for p in link_product:
        item = item = p.find_element_by_xpath('.//a').get_attribute('href') 
        
        link_p_1.append(item)
        dict_types = {i: item}
        data.append(dict_types)
        # print('-Product completed: {}'.format(c+= 1))
driver.quit()

df_p_1 = pd.DataFrame([(k, v) for d in data for k, v in d.items()], 
                  columns=['link_types','link_p_1'])  

#%% 3.2 GET LINK_P TRONG LINK_P_1:
options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

link_p_2 = []
data = []

link_test = ['https://makemyhomevn.com/collections/sofa-bang']
for i in link_p_1:   #lst_null_link
    driver.get(i)
    sleep(1)
    link_product = driver.find_elements_by_xpath('//div[@class="col-md-3 col-sm-4 col-xs-6 product-wrap product-wrap1"]')  # ok
    for p in link_product:
        item = item = p.find_element_by_xpath('.//a').get_attribute('href') 
        # print('Link crawl: {}'.format(item))
        link_p_2.append(item)
        dict_types = {i: item}
        data.append(dict_types)
driver.quit()

df_p_2 = pd.DataFrame([(k, v) for d in data for k, v in d.items()], 
                  columns=['link_p_1','link_p_2'])   

df_p_final = df_p_1.merge(df_p_2, how = 'left', on = 'link_p_1')


df_2 = df_1.merge(df_p_final, how = 'left', on = 'link_types') 

df_2['link_p_2'] = df_2['link_p_2'].fillna(df_2['link_p_1'])

df_2['link_p_2'] = df_2['link_p_2'].fillna(df_2['link_p'])


link_p = list(dict.fromkeys(df_2['link_p_2'].tolist()))


link_p = [x for x in link_p if str(x) != 'nan']

# GET 20 first element in link_p

link_test = link_p[0:5]

#%%
gc = gspread.service_account(filename=current_path + "/data/project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

worksheet = sh.get_worksheet(5)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

# df_1 = df.loc[df.iloc[:,-1] == 'X']
# last_col = (df.columns)[-1]
# check = df.loc[df.iloc[:,-1] != 'X']
link_old = df_ggs['link_p'].tolist()



link_new = list(dict.fromkeys(sorted(set(link_p) - set(link_old))))

print("---------------NUMBER LINK NEW MAKE MY HOME:  {}".format(len(link_new)))

if len(link_new) == 0:
    print("NO NEED TO UPDATE NEW PRODUCT IN MAKE MY HOME")
    
print('--------------------------------------------------------------')
print('--------------------------------------------------------------')
print('---------------- COMPLETED PHASE I ---------------')
print('--------------------------------------------------------------')
print('--------------------------------------------------------------')

#%% 4. GET INFO Product
n_p = len(link_new)

options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)
wait = WebDriverWait(driver, 2)
Set_speed = 0.00001

data_product = []
c = 0 
c_1 = 0

link_test = ['https://makemyhomevn.com/collections/ghe-an-cafe/products/shape',
             'https://makemyhomevn.com/collections/ghe-van-phong-office-chairs/products/shape'
            ] # link_types
n_p = len(link_new) # link_new

for i in link_new: # link_new
    try:
        driver.get(i)
        sleep(Set_speed)
        try:
            Product_name = Product_name = driver.find_elements_by_xpath('.//h2[@itemprop="name"]/a')[0].text 
        except:
            Product_name = None
        try:
            SKU = driver.find_element(By.XPATH, "//div[@class='row no-margin']/div").get_attribute('id')
        except:
            SKU = None        
            
    
        try:
            price = driver.find_element(By.XPATH,"//label[@id='ProductPrice']").text
        except:
            price = None
            print('MISSING PRICE')
            print('Link Missing price: {}'.format(i))
    # Get Info product:
        try:
            Collection = Collection =  driver.find_elements_by_xpath('.//h2[@itemprop="name"]/a')[0].text 
        except:
            Collection = None
            
            
        product_description = []
        for table in driver.find_elements_by_xpath('//div[@class="product-item-description"]'):   
            des = [item.text for item in table.find_elements(By.XPATH,"./p")]  # p
            if des != []:
                product_description.append(des)  
            else:
                break
            
        product_description2 = []
        for table in driver.find_elements_by_xpath('//div[@class="product--description has-img"]'):   
            des = [item.text for item in table.find_elements(By.XPATH,"./p")]  # p
            if des != []:
                product_description2.append(des)  
            else:
                break
        
        try:
            des_0 = driver.find_elements(By.XPATH, '//div[@class="product-item-description"]')[0].get_attribute('innerText')
        except:
            des_0 = None  
            
        try:
            des_1 = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Chất liệu:']/.."))).get_attribute('innerText').split(":")[1:]
        except:
            des_1 = None                                               
            for table in driver.find_elements_by_xpath('//div[@class="product-item-description"]'):   
                des = [item.text for item in table.find_elements_by_xpath(".//*[self::span or self::p or self::strong]")] # or self::p
                if des != []:
                    product_description.append(des)  
                else:
                    break
        try:
            des_2 = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Màu sắc:']/.."))).get_attribute('innerText').split(":")[1:]
        except:
            des_2 = None
        try:
            des_3 = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Kích thước:']/.."))).get_attribute('innerText').split(":")[1:]
        except:
            des_3 = None
        
        dict_p = {'SKU': SKU, 'Collection': Collection, 'Product_name': Product_name, 'Price': price,  # 'Material': des_1, 'Color': des_2, 'Dimension': des_3,
                   
                  'Product_description': product_description, 'Product_description_2': product_description2, 'Product_description_3': des_0,'Material': des_1, 'Color': des_2, 'Dimension': des_3,
                  'link_p': i}  
        elems = driver.find_elements_by_xpath(('//div[@class="item"]'))    
        for elem in elems:
            time.sleep(2)
            try:
                sub_link = sub_link = elem.find_element_by_xpath('.//img').get_attribute('src')   
                urllib.request.urlretrieve(sub_link, current_path + "/data/Make_My_Home_" + str(SKU) +  '_' + str(current_date) + '.jpg') # save image
            except:
                None
                print('Link error: {}'.format(i))
        data_product.append(dict_p)
        c+=1
        c_1+= float(1*100/n_p) 
        print('Product completed: {}, ({:.2f}%)'.format(c, c_1))
    except:
        None
driver.quit()
    
df_product = pd.DataFrame(data_product)    
# df_product = df_product.drop_duplicates()
 
df_product.info()
    

# df = df_2.merge(df_product, how = 'left', left_on = 'link_p_2',right_on = 'link_p') 

#%% 
df_product = df_product.loc[~(df_product['SKU'].isnull())]

def fix_sku(r):
    temp = r['SKU']
    return temp.replace('product-', '')

df_product['SKU'] = df_product.apply(fix_sku, axis = 1)




#%% CHECKING NEW SKU:



new_sku = list(set(df_product['SKU'].tolist()) - set(df_ggs['SKU'].tolist()))
print("                          ")
print('--------------NUMBER OF NEW PRODUCT MAKE MY HOME: {}-------------'.format(len(new_sku)))
print("                          ")


df_product = df_product.loc[~df_product['Product_name'].isnull()]
print(len(new_sku))
df_product = df_product.loc[df_product['SKU'].isin(new_sku)]


df_product.to_excel(current_path + "/data/Data_MakeMyHome_Web_Phase_I'.xlsx", index = False)


#%%
#%% FILL TYPES
df = pd.read_excel(current_path + "/data/Data_MakeMyHome_Web_Phase_I'.xlsx")




def fix_types(r):
    temp = r['link_p']
    temp_1 = list(temp.split("/"))[-1]
    temp_1_1 = temp_1.split("-")[:3]
    return ' '.join([str(item) for item in temp_1_1])
    
    
df['Check_name'] = df.apply(fix_types, axis = 1)
check_types = df[['Check_name', 'link_p']].drop_duplicates(subset = 'Check_name')

# Fix Types BY HAND:
df_types_ETL = pd.read_excel(current_path + "/data/Sp_ETL_Types_MakeMyHome.xlsx")
df_types_ETL = df_types_ETL[['Check_name', 'Types']]
               
df = df.merge(df_types_ETL, how = 'left', on = 'Check_name')

check_types_NULL = df.loc[df['Types'].isnull()][['Check_name', 'Types', 'link_p']].drop_duplicates(subset = 'Check_name').sort_values(by = 'Check_name')
print('---------------------------------------------------')
print('---------------------------------------------------')
print('Product có Types null: {}'.format(check_types_NULL))
print('                                                   ')
print('                                                   ')


#%% Add Room
df['Room'] = ''

dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room', 'Pallet Bed', 'Dressing Chair', 'Folding Bed', 'Kid Bunk'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 'Kitchen cabinet', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room'], #ok
            'Kitchen': ['Kitchen storage', 'Kitchen cabinet'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Set TV Cabinet','Book shelf', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater',
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Laptop table', 'Set Sofa 1 seater', 'Swing armchair', 'Corner table',
                            'Sofa Bed', 'Sofa Bed 1 seater', 'Sofa Bed 2 seater', 'Sofa Bed 3 seater'],
            'Office': ['Desk', 'Folding Desk','Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room', 'Desk Accessories'],
            # 'Other': ['None', 'Gift Voucher', 'Other'],    
            'Outdoor': ['Set Outdoor Table', 'Outdoor Table', 'Folding Chair - Outdoor', 'Swing chair', 'Outdoor Chair', 'Folding Chair'],
            
            
            'Bath Room': ['Bath Storage'],
            'Decoration/ Accessory': ['Blanket/ Pillow', 'Mattress','Decoration Mirror', 'Decoration Accessories', 'Kitchen Accessory', 'Painting', 'Carpet', 'Candle', 'Pot', 'Lamp', 
                                      'Other', 'Seat Cushions', 'Decoration/ Accessory', 'Accessories', 'Wall shelf', 'Clock']} #, 'Table Accessory'

def fix_room(r):
    temp = r['Room']
    temp_1 = r['Types'].lower()
    for key, values in dict_all.items():
        for i in values:
            if temp_1 == i.lower():
                return key
            continue
        continue
    return temp
df['Room'] = df.apply(fix_room, axis = 1)  

check_Room_NULL = df.loc[df['Room'].isnull()][['Room', 'Types', 'link_p']].drop_duplicates(subset = 'Types').sort_values(by = 'Types')


#%% Fix Price

df['Price'] = df['Price'].str.replace('₫', '')
df['Price'] = df['Price'].str.replace(',', '')

df['Price'] = pd.to_numeric(df['Price'], errors='ignore')

print(df.columns)

#%% Sync Room, Types with all Competitors
dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 'Kitchen cabinet', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room'], #ok
            'Kitchen': ['Kitchen storage'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Book shelf', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa Bed', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater', 
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Eames Chair'],
            'Office': ['Desk', 'Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room'],
            # 'Other': ['None', 'Gift Voucher', 'Other'],    
            'Outdoor': ['Set Outdoor Table', 'Outdoor Table', 'Folding Chair - Outdoor', 'Swing chair', 'Outdoor Chair'],
            
            
            'Bath Room': ['Bath Storage'],
            'Decoration/ Accessory': ['Blanket/ Pillow', 'Decoration Mirror', 'Decoration Accessories', 'Kitchen Accessory', 'Painting', 'Carpet', 'Candle', 'Pot', 'Lamp', 
                                      'Other', 'Seat Cushions', 'Decoration/ Accessory', 'Accessories', 'Wall shelf'] #, 'Table Accessory'
            }
def fix_room(r):
    temp = r['Room']
    temp_1 = r['Types'].lower()
    for key, values in dict_all.items():
        for i in values:
            if temp_1 == i.lower():
                return key
            continue
        continue
    return temp
df['Room'] = df.apply(fix_room, axis = 1)   

df['Types'] = df['Types'].replace('Decoration Accessories', 'Decoration/ Accessory')


# check = check.groupby('Room')['Types'].apply(list).to_dict()

df['Types'] = df['Types'].str.title()
df['Room'] = df['Room'].str.title()  

print(df.columns)
df['Brand'] = 'Make My Home'

# df.rename({'link_p_2': 'link_p'},axis = 1, inplace = True)
#%% ADD DIMENSION, COLOR, MATERIAL:

    
import ast
df["Product_description"] = df["Product_description"].apply(ast.literal_eval) 

def convert_flat(r):
    temp = r['Product_description']
    return [item for sublist in temp for item in sublist]
df['Product_description'] = df.apply(convert_flat, axis = 1)


# def fix_p(r):
#     temp = r['Product_description']
#     return ''.join(temp).split()

# df['Product_description'] = df.apply(fix_p, axis = 1)


def add_m(r):
    temp = r['Product_description']
    for i in temp:
        if 'Chất liệu' in i:
            return i.replace('Chất liệu', '').replace(':', '').strip()
        continue
df['Material'] = df.apply(add_m, axis = 1)

def add_c(r):
    temp = r['Product_description']
    for i in temp:
        if 'Màu sắc' in i:
            return i.replace('Màu sắc', '').replace(':', '').strip()
        continue
df['Color'] = df.apply(add_c, axis = 1)

def add_d(r):
    temp = r['Product_description']
    for i in temp:
        if 'Kích thước' in i:
            return i.replace('Kích thước', '').replace(':', '').strip()
        continue
df['Dimension'] = df.apply(add_d, axis = 1)


def add_n(r):
    temp = r['Product_description_3']
    try:
        return temp.split(',')[0] + ' ' + r['Product_name']
    except:
        None
    
df['Product_name'] = df.apply(add_n, axis = 1)



df.rename({'Product_description': 'Description'}, axis = 1, inplace = True)



df.to_excel(current_path + "/data/Data_MakeMyHome_FINAL.xlsx", index = False)



#%%
lst_sku_old = list(dict.fromkeys(df_ggs['SKU'].tolist()))


new_product = df.copy()
new_product['SKU'] = new_product['SKU'].astype(str)
new_product = new_product.loc[~new_product['SKU'].isin(lst_sku_old)]


lst_n = list(set(list(df_ggs.columns.tolist())) - set(list(df.columns.tolist())))
print(lst_n)
for i in lst_n:
    new_product[i] = ''

new_product.columns
new_product = new_product[df_ggs.columns]
new_product['Date_get_new_product'] = current_date # 
            
new_product.iloc[: , -1] = 'X'
            
new_product.drop_duplicates(subset = ['SKU'], inplace = True)

