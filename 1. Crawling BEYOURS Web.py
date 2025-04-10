# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 10:11:52 2022

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


now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
current_date_save = now1.strftime("%Y-%m-%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")
Brand = 'Beyours'

import os
current_path = os.getcwd()

#%% 1. GET ALL LINK
options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

Brand = 'Beyours'
data = []
Categories = []
Productlinks = []
c = 0
# list_room = ['phong-khach', 'phong-an', 'phong-ngu', 'phong-lam-viec', 'hanh-lang', 'trang-tri-nha-cua']
for i in range(0, 25):
    driver.get('https://beyours.vn/collections/tat-ca-san-pham?page='+ str(i))
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
    options = Options()
    options.add_argument("--disable-notifications")

    items = driver.find_elements_by_xpath('//div[@class="grid__item product--loop product--grid-item large--one-quarter medium--one-third small--one-half pro-loop"]') #items = driver.find_elements_by_xpath('//*[@id="main"]/div/div[3]/div/div[2]/div/div[2]/div')
    for item in items:   
        
        sleep(1)
        link = link = item.find_element_by_xpath('.//a').get_attribute('href')
        Productlinks.append(link)
        dict_p = {i: link}
        data.append(dict_p)
        c+=1
        print('- Product link compeleted: {}'.format(c))
    # if len(Productlinks) != len(set(Productlinks)):
    #     break    
Productlinks = list(dict.fromkeys(Productlinks))
driver.quit()

n_p = len(Productlinks)


#%%
gc = gspread.service_account(filename= current_path + "/data/project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

worksheet = sh.get_worksheet(1)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

df_ggs = df_ggs.drop_duplicates()

# df_1 = df.loc[df.iloc[:,-1] == 'X']
# last_col = (df.columns)[-1]
# check = df.loc[df.iloc[:,-1] != 'X']
link_old = df_ggs['link_p'].tolist()



link_new = list(dict.fromkeys(sorted(set(Productlinks) - set(link_old))))
print('------------------NEW LINK PRODUCT: {}'.format(len(link_new)))

print('--------------------------------------------------')
print('--------------------------------------------------')
print('-----------COMPLETED PHASE I: CRAWL DATA BEYOURS----------')
print('--------------------------------------------------')
print('--------------------------------------------------')
#%% 
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


link_test = ['https://beyours.vn/products/sofa-bang-beyours-3-seat-violet-sofa-orange']

chrome_options = Options()
chrome_options.add_argument("--disable-notifications")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--remote-debugging-port=9230")
 
options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path="I:/Chromedriver/BETA/chromedriver.exe", )
driver.set_window_size(1920, 1080)



c = 0
c_1 = 0
data = []

link_test = ['https://beyours.vn/products/quay-bep-beyours-mody-kitchen-storage-cafe-head-01-natural', 'https://beyours.vn/products/sofa-bang-beyours-moonlight-sofa-grey']
for link in link_new:  # link_new #
    driver.get(link)
 
    driver.implicitly_wait(5)
    try:
        Product_name = Product_name =  driver.find_elements_by_xpath('.//h1[@itemprop="name"]')[0].text # ok
    except:
        Product_name = None
    try:
        sku = sku =  driver.find_elements(By.XPATH, '//div[@class="product__id hide"]')[0].get_attribute('innerText')
    except:
        sku = None
        
        
    try:
        price = driver.find_element(By.XPATH,"//span[@id='ProductPrice']").text  # ok                       
    except:
        price = None
    product_description = [] 

    for table in driver.find_elements_by_xpath('//*[contains(@class,"pro-tabcontent")]//table'):  # '//*[contains(@class,"zmotadangcapvuottroi")]//table'
        des = [item.text for item in table.find_elements_by_xpath(".//*[self::td or self::th]")]
        product_description.append(des)      
    elems = driver.find_elements_by_xpath(('//div[@class="product-single__photos"]'))    
    for elem in elems:  # DOWLOAD IMAGE
        time.sleep(2)
        sub_link = sub_link = elem.find_element_by_xpath('.//img').get_attribute('src')        
     
        urllib.request.urlretrieve(sub_link, "C:/DATA_D/CRAWLING DATA/Data Competitor/Beyours/" + str(sku) +  '_' + str(current_date_save) + '.jpg') # save image



         
    dict_p = {'SKU': sku, 'Product_name': Product_name, 'Price': price , 'Product_description': product_description,'link_p': link} #'description': des
    data.append(dict_p)
    c+=1
    c_1+= float(1*100/n_p) 
    print('Product completed: {}, ({:.2f}%)'.format(c, c_1))

driver.quit()


df_2 = pd.DataFrame(data)

df_2.info()

df_2.to_excel(current_path + "/data//Data_BEYOURS_Web_Phase_I.xlsx", 
              index = False) 
print('--------------------------------------------------')
print('--------------------------------------------------')
print('--------COMPLETED PHASE II: CRAWL DESCRIPTION - BEYOURS -----')
print('--------------------------------------------------')
print('--------------------------------------------------')



#%%
#%% ETL DATA
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

now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")
Brand = 'Beyours'

df = pd.read_excel(current_path + "/data/Data_BEYOURS_Web_Phase_I.xlsx")

gc = gspread.service_account(filename= current_path + "/data/project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

worksheet = sh.get_worksheet(1)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]   
df_ggs = df_ggs.drop_duplicates()                
#%% Add Types                   
df['Product_name'] = df['Product_name'].str.title()

df['Check_Name'] = df['Product_name'][:5]

def get_str(r):
    temp = r['Product_name'].split(' ')[:5]
    return ' '.join(temp)
df['Check_Name'] = df.apply(get_str, axis = 1)

check = df[['Product_name', 'Check_Name']].drop_duplicates(subset = 'Check_Name')

check = check.sort_values(by = 'Check_Name')
# check.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Sp_ETL_Types_Beyous.xlsx", index = False)
   
df_fix_Types = pd.read_excel(current_path + "/data//Sp_ETL_Types_Beyous.xlsx")    
df_fix_Types = df_fix_Types[['Check_Name', 'Types']]
                             
df = df.merge(df_fix_Types, how = 'left', on = 'Check_Name')
print('-----------------------------------------------------------')
print('Number of Types null: {}'.format(df['Types'].isnull().sum()))
print('-----------------------------------------------------------')
check_Types_null = df.loc[df['Types'].isnull()][['Product_name', 'Check_Name', 'link_p']].drop_duplicates(subset = 'Check_Name')
     


#%% Add Room
df['Room'] = ''

dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room', 'Pallet Bed', 'Dressing Chair', 'Shelf Mirror'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 'Kitchen cabinet', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room'], #ok
            'Kitchen': ['Kitchen storage', 'Kitchen Island'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Set TV Cabinet','Book shelf', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa Bed', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater', 
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Shoes racks'],
            'Office': ['Desk', 'Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room'],
            # 'Other': ['None', 'Gift Voucher', 'Other'],    
            'Outdoor': ['Set Outdoor Table', 'Outdoor Table', 'Folding Chair - Outdoor', 'Swing chair', 'Outdoor Chair'],
            
            
            'Bath Room': ['Bath Storage'],
            'Decoration/ Accessory': ['Blanket/ Pillow', 'Decoration Mirror', 'Decoration Accessories', 'Kitchen Accessory', 'Painting', 'Carpet', 'Candle', 'Pot', 'Lamp', 
                                      'Other', 'Seat Cushions', 'Decoration/ Accessory', 'Accessories', 'Wall shelf']} #, 'Table Accessory'

df['Types'] = df['Types'].astype(str)
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
 
df['Types'] = df['Types'].replace('nan', np.nan)


check_Room_null = df.loc[df['Room'].isnull()][['Product_name', 'Check_Name', 'link_p', 'Types']].drop_duplicates(subset = 'Check_Name')



#%% ETL "Product_description"            

import ast
df["Product_description"] = df["Product_description"].apply(ast.literal_eval) 

def convert_flat(r):
    temp = r['Product_description']
    return [item for sublist in temp for item in sublist]
df['Product_description'] = df.apply(convert_flat, axis = 1)

def remove_blank(r):
    temp = r['Product_description']
    return [i for i in temp if i]
df['Product_description'] = df.apply(remove_blank, axis = 1)

# def check_len(r):
#     temp = r['Product_description']
#     return len(temp)
# df['Check_LEN'] = df.apply(check_len, axis = 1)


def Convert(r):
    lst = r['Product_description'][:6]
    res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
    return res_dct
    
df['Product_description'] = df.apply(Convert, axis = 1)

import json
df3 = pd.json_normalize(df['Product_description'])

df3.rename({'Kích thước:': 'Dimension', 'Chất liệu:': 'Material', 'Màu sắc:': 'Color', 'Xuất xứ:': 'Made in'}, axis = 1, inplace = True)

df = df.merge(df3, how = 'left', left_index= True, right_index = True)

#%% FIX PRICE

df['Price'] = df['Price'].astype(str)

def fix_price(r):
    lst_special = "['.₫|,']"
    temp = r['Price']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Price'] = df.apply(fix_price, axis = 1)

df.drop(['Product_description'], axis = 1, inplace = True)

df['Product_name'] = df['Product_name'].str.title()

lst_cols = list(df_ggs.columns)
print(lst_cols)
df['Brand'] = Brand

month_save = now1.strftime("%B-%Y")
df[month_save] = 'X'


#
new_product = df.copy()

lst_n = list(set(list(df_ggs.columns.tolist())) - set(list(df.columns.tolist())))
print(lst_n)
for i in lst_n:
    new_product[i] = ''

print(new_product)

#%% REMOVE product BY sku:
lst_sku = list(dict.fromkeys(df_ggs['SKU'].tolist()))
new_product = new_product.loc[~new_product['SKU'].isin([lst_sku])] 



new_product.columns
new_product = new_product[df_ggs.columns]
new_product['Date_get_new_product'] = current_date # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251





















#%%
# df_2.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data Crawl/Data_" + Brand + '_Web_' + today +'.xlsx', index = False)

# #%% ETL ALL 
# df = pd.read_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data Crawl/Data_" + Brand + '_Web_' + today +'.xlsx')
# # df = pd.read_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data Crawl/Data_Beyours_Web_12.08.22.xlsx")                






#%% Add tính năng Giảm số lượng product theo Collection: 'Count_Product'
lst_collection = ['Violet', 'Helio', 'Vesta', 'Pansy', 'Mimosa', 'Lantana', 'Katrina', 'Poppy',
                  'Enyo', 'Andes', 'Melia', 'Calorina', 'Hera', 'Dione', 'KTV', 'Kiri', 'Sena',
                  'Bonny', 'Bumbee', 'Pin Stool', 'A Table','B Table', 'D Table', 'Pure Table', 'Cubo Table',
                  'Chubby', 'Bee', 'Aimee', 'Dambi', 'Charming', 'Dra', 'Acep', 'Freezing Fog',
                  'Bernie', 'Funky', 'Nancy', 'Elsa', 'Ruby', 'Glass', 'Bey', 'Mandy',
                  'Mina', 'BLV', 'Simple', 'Square', 'Neuly', 'Cabin', 'Be.He', 'Uni', 'Pebal',
                  'Ori', 'Athena', 'Bey', 'ks', '3f', '2fm', 'Be.01', 'tg0', 'A Mirror', 'Soi Toàn Thân O',
                  'A Case', '2f', '3f', '4f']



def add_classify(r):
    temp = r['Product_name']
    temp_1 = r['Types']
    for i in lst_collection:
        if i.lower() in temp.lower() and temp_1 != 'Decoration/ Accessory':
            return temp_1.title() + ' ' + i.upper()
        continue
    return temp
df['Count_Product'] = df.apply(add_classify, axis = 1)
    
df.columns
    
# df.drop(['Đặc tính:', 'Check_Name', 'Bảo quản:'], axis = 1, inplace = True)    # , 'Thành phần:' , 'Xuất xứ' , ' ' ,  'Mẫu mã thiết kế:'
    
df['Brand'] = Brand
df['Date_get_new_product'] = current_date

df['SKU'] = df['Product_name'].copy()

month_save = now1.strftime("%B-%Y")
df[month_save] = 'X'

df.rename({'link': 'link_p'}, axis = 1, inplace = True)

lst_cols_ggs = list(df_ggs.columns)
print(list(df_ggs.columns))
df.columns
#%% Bổ sung thêm Columns df ko có:
lst_spec = sorted(set(list(df_ggs.columns)) - set(list(df.columns)))


final_result = pd.concat([df_ggs, df])
final_result = final_result[list(df_ggs.columns)]

final_result = final_result[['Room', 'Types', 'Collection', 'Date_get_new_product', 'SKU', 'Product_name',
       'Price', 'Color', 'Description', 'Dimension', 'Material', 'link_p',
       'Style', 'Made in', 'Xuất xứ', 'Mẫu mã thiết kế:', 'Thành phần:',
       'Đặc điểm nổi bật:', 'Made In', 'Unit', 'September-2022',
       'October-2022', 'November-2022', 'December-2022', month_save]]



# df.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_Beyours_FINAL.xlsx", index = False)

#%% 
worksheet.clear()
set_with_dataframe(worksheet, final_result) # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

print('-----------------------------------------------------------')
print('---------Update Beyours Product Status COMPLETED-----------')
print('-----------------------------------------------------------')
print('-----------------------------------------------------------')






#%%
#%%
#%% CRAWL SKU BEYOURS
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

chrome_options = Options()
chrome_options.add_argument("--disable-notifications")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--remote-debugging-port=9230")
 
options = Options()
options.binary_location = "C:/Program Files/Google/Chrome Beta/Application/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path="I:/Chromedriver/BETA/chromedriver.exe", )
driver.set_window_size(1920, 1080)

c = 0
c_1 = 0
data = []

link_test = ['https://beyours.vn/products/quay-bep-beyours-mody-kitchen-storage-cafe-head-01-natural', 'https://beyours.vn/products/sofa-bang-beyours-moonlight-sofa-grey']
for link in Productlinks:  # link_test #
    driver.get(link)
 
    driver.implicitly_wait(5)
    try:
        Product_name = Product_name =  driver.find_elements_by_xpath('.//h1[@itemprop="name"]')[0].text # ok
    except:
        Product_name = None
    try:
        sku = sku =  driver.find_elements(By.XPATH, '//div[@class="product__id hide"]')[0].get_attribute('innerText')
    except:
        sku = None
        

         
    dict_p = {'SKU': sku, 'Product_name': Product_name, 'link_p': link} #'description': des
    data.append(dict_p)
    c+=1
    c_1+= float(1*100/n_p) 
    print('Product completed: {}, ({:.2f}%)'.format(c, c_1))

driver.quit()


df_all_sku = pd.DataFrame(data)

df_all_sku.to_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data Crawl/Data_BEYOURS_ALL_SKU.xlsx", index = False)

# FIX SKU ON GGS
df_all_sku = pd.read_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data Crawl/Data_BEYOURS_ALL_SKU.xlsx")


gc = gspread.service_account(filename="I:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/0. ETL's CODING To Export Data Studio/project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251
worksheet = sh.get_worksheet(1)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

df_ggs = df_ggs.drop_duplicates()

cols_df = list(df_ggs.columns) 
#
df_all_sku = df_all_sku[['SKU', 'link_p']]

test = df_ggs.merge(df_all_sku, how = 'left', on = ['link_p'])

test['SKU_y'].fillna(test['SKU_x'], inplace = True)

test.drop(['SKU_x'], axis = 1, inplace = True)
test.rename({'SKU_y': 'SKU'}, axis = 1, inplace = True)

test = test[cols_df]

test['SKU'] = test['SKU'].astype(str)


worksheet.clear()
set_with_dataframe(worksheet, test)



            
#%%
# #%% CRAWLING PRODUCT IMAGE:
    

# import pandas as pd
# import numpy as np
# import datetime as dt
# import re
# import gspread
# import csv
# import time
# import urllib.request
# from gspread_dataframe import set_with_dataframe
# from datetime import datetime, timedelta
# from time import sleep
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support import expected_conditions as EC

# Brand = 'Baya'
# now1 = datetime.now()
# current_time = now1.strftime("%H:%M:%S")
# current_date = now1.strftime("%Y/%m/%d")
# today = pd.to_datetime('now').strftime("%d.%m.%y")


# df = pd.read_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_Beyours_FINAL.xlsx")
# df.columns


# Productlinks = list(dict.fromkeys(df['link'].tolist()))


# c = 0
# c_1 = 0
# n_p = len(Productlinks)

# data = []
# options = Options()
# options.binary_location = "C:/Program Files/Google/Chrome Beta/Application/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
# driver = webdriver.Chrome(chrome_options=options, executable_path="I:/Chromedriver/BETA/chromedriver.exe", )
# driver.set_window_size(1920, 1080)

# link_image = []

# link_test = ['https://beyours.vn/products/giuong-ngu-beyours-cristal-bed']
# for i in Productlinks: #Productlinks
#     driver.get(i)
#     try:
#         Product_name = Product_name =  driver.find_elements_by_xpath('.//h1[@itemprop="name"]')[0].text # ok
#     except:
#         Product_name = None
#     elems = driver.find_elements_by_xpath(('//div[@class="product-single__photos"]'))    
#     for elem in elems:
#         time.sleep(2)
#         sub_link = sub_link = elem.find_element_by_xpath('.//img').get_attribute('src')        
     
#         urllib.request.urlretrieve(sub_link, "D:/CRAWLING DATA/Data Competitor/Beyours/Product Image/" + str(Product_name) + '.jpg') # save image
        
        
#     #     link_image.append(sub_link)
#     # dict_p = {'Image_Product': link_image, 'SKU': sku} #'description': des
#     # data.append(dict_p)
#     c += 1
#     c_1+= float(1*100/n_p) 
#     print('Product completed: {}, ({:.2f}%)'.format(c, c_1))
# driver.quit()
# # df_image = pd.DataFrame(data)








    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    



