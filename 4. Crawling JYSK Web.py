# -*- coding: utf-8 -*-
"""
Created on Wed Apr 13 10:17:16 2022

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
#%% 

options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)


Brand = 'JYSK'
data = []
Categories = []
Productlinks = []

'https://jysk.vn/phong-khach'

list_room = ['ban-cong-san-vuon', 'phong-an', 'phong-khach', 'phong-lam-viec', 'phong-ngu', 'phong-tam', 'sanh-va-loi-vao']

# 1. Get link_Types
link_test = ['phong-lam-viec']


room = []
link_Types = []
for i in list_room: # list_room
    driver.get('https://jysk.vn/'+ str(i))
    links = driver.find_elements_by_xpath('//div[@class="w-100"]')  
    sleep(1)
    for item in links:   
        sub_link = sub_link = item.find_element_by_xpath('.//a').get_attribute('href')
        link_Types.append(sub_link)
        dict_room = {i: sub_link}
        room.append(dict_room)
driver.quit()

df = pd.DataFrame([(k, v) for d in room for k, v in d.items()], 
                  columns=['Room','link_types'])   
    
df['Types'] = df['link_types'].str.replace('https://jysk.vn/',  '')

#%% 2. Get link_product:
options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

link_p = []
types = []
c = 0

link_test = ['https://jysk.vn/ban-lam-viec']
for i in link_Types: # link_Types
    driver.get(i)
    links = driver.find_elements_by_xpath('//div[@class="w-100"]')  
    sleep(1)
    for item in links:   
        sub_link = sub_link = item.find_element_by_xpath('.//a').get_attribute('href')
        link_p.append(sub_link)
        dict_types = {i: sub_link}
        types.append(dict_types)
        c+=1
        print('- Product completed: {}'.format(c))
driver.quit()      
df2 = pd.DataFrame([(k, v) for d in types for k, v in d.items()], 
                  columns=['link_types','link_p'])   
 
df = df.merge(df2, how = 'left', on = 'link_types')
  
df = df[['Room', 'Types', 'link_p']]

#%%
#%%
gc = gspread.service_account(filename=current_path + "/data/project-aftersale-moho-cd1338f28ec9.json")
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



link_new = list(dict.fromkeys(sorted(set(Productlinks) - set(link_old))))

print("---------------NUMBER LINK NEW JYSK {}".format(len(link_new)))

if len(link_new) == 0:
    print('No NEED TO CONTINUE PHASE II')


print('-------------COMPLETED PHASE I----------------')

#%% 3. Get info_product
n_p = len(link_p)

options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path="I:/Chromedriver/BETA/chromedriver.exe", )
driver.set_window_size(1920, 1080)

c = 0
c_1 = 0
data = []
link_test = ['https://jysk.vn/tu-ho-so-abbetved-go-cong-nghiep-soitrang-d40xr40xc56cm']
for link in link_p:  # link_p
    driver.get(link)
 
    driver.implicitly_wait(5)
    try:
        Product_name = Product_name =  driver.find_elements_by_xpath('.//h2[@class="product-detail-name"]')[0].text # ok
    except:
        Product_name = None
    try:
        price = driver.find_element(By.XPATH,"//div[@class='price']").text  # ok                       
    except:
        price = None
    product_description = [] 
    for table in driver.find_elements_by_xpath('//div[@class="product-detail-spec-content"]'):   # ok nhưng bị dup '[['
        des = [item.text for item in table.find_elements_by_xpath(".//*[self::td or self::th or self::p]")]
        product_description.append(des)  
    elems = driver.find_element_by_xpath(('//div[@class="product-detail-images-wrap mt-5"]'))     # //div[@class="img"]
    
    time.sleep(2)
    sub_link = sub_link = elems.find_element_by_xpath('.//img').get_attribute('src')   
    try:
        urllib.request.urlretrieve(sub_link, current_path + "/data/JYSK_" + str(Product_name) +  ' ' + str(current_date) + '.jpg') # save image
    except:
        None

             
    dict_p = {'Product_name': Product_name, 'Price': price , 'Product_description': product_description,'link_p': link} #'description': des
    data.append(dict_p)
    c+=1
    c_1+= float(1*100/n_p) 
    print('Product completed: {}, ({:.2f}%)'.format(c, c_1))
    # driver.stop_client()
    # driver.close()
    # driver.quit()
driver.quit()
df3 = pd.DataFrame(data)

def fix_des(r):
    temp = r['Product_description']
    try:
        return temp[0]
    except:
        return temp

df3['Product_description'] = df3.apply(fix_des, axis = 1)

#%%
df = df.merge(df3, how = 'right', on = 'link_p')
print(list(df.columns))


df['Brand'] = Brand
df['Date_updated'] = current_date
df = df[['Brand', 'Room', 'Types', 'Product_name', 'Price', 'Product_description', 'Date_updated', 'link_p']]

df = df.loc[df['Product_name'].notnull()]

df['Product_name'] = df['Product_name'].replace(' | ', ' ')

df.to_excel(current_path + "/data/Data_JYSK_web_Phase_I.xlsx", # Done 2/11/22 
            index = False)

print('-------------COMPLETED PHASE II----------------')
#%% ETL DATA
Brand = 'JYSK'
import pandas as pd

df = pd.read_excel(current_path + "/data/Data_JYSK_web_Phase_I.xlsx")

# Tách description từ 'Product name'
df.columns

# def get_des(r):
#     temp = .split('|')
#     return ','.join(temp)
# df['Check_Name'] = df.apply(get_des, axis = 1)               

def get_des(r):
    listRes = list(r['Product_name'].split("|"))
    return listRes[0]
df['Check_Name'] = df.apply(get_des, axis = 1) 


def get_des2(r):
    listRes = list(r['Check_Name'].split(","))
    return listRes[0]
df['Check_Name'] = df.apply(get_des2, axis = 1) 

def get_len(r):
    temp = r['Check_Name']     
    return len(temp)
df['Len_name'] = df.apply(get_len, axis = 1)        

check = df[['Check_Name', 'Len_name']]

check_Types= df[['Check_Name', 'Price','link_p']].drop_duplicates(subset = 'Check_Name')


check = check_Types.sort_values(by = 'Check_Name')
check['Types'] = ''
check.columns

check = check[['Check_Name', 'Price', 'Types', 'link_p']]

# check.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Sp_ETL_Types_JYSK.xlsx", index = False)
   
df_fix_Types = pd.read_excel(current_path + "/data/Sp_ETL_Types_JYSK.xlsx") # điền sp mới vào đây
df_fix_Types = df_fix_Types[['Check_Name', 'Types']]

df.drop(['Types'], axis = 1, inplace = True)                       
df = df.merge(df_fix_Types, how = 'left', on = 'Check_Name')
print('-----------------------------------------------------------')
print('Số lượng Types null: {}'.format(df['Types'].isnull().sum()))
print('-----------------------------------------------------------')
check_Types_null = df.loc[df['Types'].isnull()][['Check_Name', 'Price','link_p']].drop_duplicates(subset = 'Check_Name')

#%% Add Room
df['Room'] = ''

dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room', 'Pallet Bed', 'Dressing Chair', 'Folding Bed', 'Kid Bunk'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 'Kitchen cabinet', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room'], #ok
            'Kitchen': ['Kitchen storage'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Set TV Cabinet','Book shelf', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater',
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Laptop table', 'Set Sofa 1 seater', 'Swing armchair',
                            'Sofa Bed', 'Sofa Bed 1 seater', 'Sofa Bed 2 seater', 'Sofa Bed 3 seater', 'Glass Cabinet'],
            'Office': ['Desk', 'Folding Desk','Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room', 'Set Desk + Chair', 'Set Desk + Cabinet'],
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

print('-----------------------------------------------------------')
print('Số lượng Room null: {}'.format(df['Room'].isnull().sum()))
print('-----------------------------------------------------------')
check_Room_null = df.loc[df['Room'].isnull()][['Types', 'link_p']].drop_duplicates(subset = 'Types')

#%% ADD Description:
    
def get_des(r):
    listRes = list(r['Product_name'].split("|"))
    return listRes
df['Description'] = df.apply(get_des, axis = 1)  

def get_len(r):
    temp = r['Description']     
    return len(temp)
df['Len_name'] = df.apply(get_len, axis = 1) 



def fix_des(r):
    temp = r['Len_name']
    temp_1 = r['Description']
    if temp == 1:
        return list(str(temp_1).split(","))[1:]
    elif temp >= 5:
        return temp_1[2:]
    else:
        return temp_1[1:]
    
df['Description 2'] = df.apply(fix_des, axis = 1)     
    
def get_Dimension(r):
    temp = r['Description 2']
    for i in temp:
        if 'cm' in i:
            return i
        continue
df['Dimension'] = df.apply(get_Dimension, axis = 1)
check = df[['Description 2', 'Dimension']]

lst_color = ['xanh', 'đen', 'hồng', 'nâu', 'màu', 'vàng', 'đỏ', 'trắng', 'be', 'trong suốt', 'xám', 'bạc', 'ghi', 'tím', 'cam', ' xám ', 'Xám']
def get_Color(r):
    temp = r['Description 2']
    for i in temp:
        for c in lst_color:
            if c in i:
                return i
            continue
        continue
df['Color'] = df.apply(get_Color, axis = 1)

check = df[['Description 2', 'Color']]

lst_mat = ['kim loại', 'plastic', 'gỗ', 'nhựa', 'polyester','gốm', 'sứ', 'xi măng', 'tre', 'pp', 'thủy tinh', 'đá', 'gỗ thông', 'bạch đàn', 'sắt', 'thép',
           'aluminium', 'abs', 'polyethylene', 'paraffin', 'stearin', 'inox', 'pvc/pp', 'pvc', 'eva', 'viscose', 'linen', 'cotton', 'acrylic', 'viscose',
           'pu', 'giấy sợi', 'gỗ công nghiệp', 'nylon', 'polypropylene', 'đá cẩm thạch', 'dolomite', 'mdf', 'kính cường lực', 'tần bì', 'veneer', 
           'microfiber', 'acrylic', 'mây', 'gỗ thông', 'cây liễu', 'sồi/veneer sồi', 'gỗ sồi', 'nhôm', 'composite', 'sáp', 'nan lá', 'in canvas',
           'sợi đay', 'polyethylen', 'microfibre', 'bamboo', 'cao su', 'xơ dừa', 'Polypropylen', 'sợi cói', 'plywood', 'alu', 'thuỷ tinh', 'giấy', 'gốm']

def get_Material(r):
    temp = r['Description 2']
    for i in temp:
        for c in lst_mat:
            if c in i.lower():
                return i
            continue
        continue
df['Material'] = df.apply(get_Material, axis = 1)

check = df[['Description 2', 'Material']]

df.drop(['Description 2', 'Len_name', 'Check_Name'], axis = 1, inplace = True)

#%% FIX PRICE
df['Price'] = df['Price'].astype(str)

def fix_price(r):
    lst_special = "['đ|.']"
    temp = r['Price']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Price'] = df.apply(fix_price, axis = 1)


for i in ['Material', 'Color', 'Dimension']:
    df[i] = df[i].str.strip().str.title()

df.info()



# df.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_JYSK_FINAL.xlsx", index =  False)

df.rename({'Sku':'SKU', 'Product_description': 'Description'}, axis = 1)


#
new_product = df.copy()

lst_n = list(set(list(df_ggs.columns.tolist())) - set(list(df.columns.tolist())))
print(lst_n)
for i in lst_n:
    new_product[i] = ''

new_product.columns
new_product = new_product[df_ggs.columns]
new_product['Date_get_new_product'] = current_date # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251



















