# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 10:25:28 2022

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
from ast import literal_eval 

import os
current_path = os.get_cwd()

Brand = 'Modern House'
now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")
# PATH = "I:/Chromedriver/chromedriver.exe" # trang update: https://chromedriver.chromium.org/ -> last version -> chromedriver_win32.zip # My PC

#%% 1. GET ALL LINK
Categories = []
Productlinks = []

options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

# driver = webdriver.Chrome(PATH)
driver.set_window_size(1920, 1080)
c = 0
for i in range(1, 10):
    driver.get('https://modernhousevn.com/collections/new-arrival?page='+ str(i)) 
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
    items = driver.find_elements_by_xpath('//div[@class="text"]') #items = driver.find_elements_by_xpath('//*[@id="main"]/div/div[3]/div/div[2]/div/div[2]/div')
    for item in items:   
        link = link = item.find_element_by_xpath('.//a').get_attribute('href')
        Productlinks.append(link)
        c+= 1
        print('- Link_product completed: {}'.format(c))
        sleep(1)
    # if len(Productlinks) != len(set(Productlinks)):
    #     break    
driver.quit()    
    
Productlinks = list(dict.fromkeys(Productlinks))    

# save data link
df_links = pd.DataFrame(data = Productlinks, columns= ['Product_links'])

df_links.to_excel(current_path + "/data/Link_product_Modern_House.xls", index= False)
                  
#%%
gc = gspread.service_account(filename=current_path + "/data/project-aftersale-moho-cd1338f28ec9.json")
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

worksheet = sh.get_worksheet(6)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)  #'Unnamed: 0' 
df_ggs = df_ggs.iloc[1: , :]

# df_1 = df.loc[df.iloc[:,-1] == 'X']
# last_col = (df.columns)[-1]
# check = df.loc[df.iloc[:,-1] != 'X']
link_old = df_ggs['link_p'].tolist()




#%%

link_new = list(dict.fromkeys(sorted(set(Productlinks) - set(link_old))))
print('                                                                   ')       
print('                                                                   ')
print("---------------NUMBER LINK NEW MODERN HOUSE {}".format(len(link_new)))
print('                                                                   ')       
print('                                                                   ')
if len(link_new) == 0:
    print("NO NEED TO UPDATE NEW PRODUCT IN MODERN HOUSE")        
print('                                                                   ')       
print('                                                                   ')
print('-----------------COMPLETED PHASE I - PRODUCT LINK -----------')         
print('                                                                   ')
print('                                                                   ')   

#%%                       
#%% 2.1 PHASE I: Get Price, Types, Stocks
df_links = pd.read_excel(current_path + "/data/Link_product_Modern_House.xls")
Productlinks = df_links['Product_links'].tolist()           

options = Options()
options.binary_location = current_path + "/data/chrome.exe" 
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)
set_speed = 1


wait = WebDriverWait(driver, set_speed)
data = []

n_p = len(link_new) # Productlinks
c = 0
c_1 = 0
e = 0



Link_error = []
for link in link_new:  #Productlinks
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

        driver.implicitly_wait(1)
        options = Options()
        options.add_argument("--disable-notifications")
    
        try:
            Product_name = Product_name = driver.find_elements_by_xpath('.//div[@class="product-details"]/h1')[0].text
        except:
            Product_name = None
        try:
            price = driver.find_element(By.XPATH,"//span[@class='product-price on-sale']").get_attribute("innerText") #ok
        except:
            # None
            try:
                price = driver.find_element(By.XPATH,"//span[@class='product-price']").get_attribute("innerText") #ok
            except:
                price = None
        try:
            types = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Loại sản phẩm:']/.."))).get_attribute('innerText').split(":")[1].strip()
        except:
            types = None
        try:
            stock = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Hàng có sẵn:']/.."))).get_attribute('innerText').split(":")[1].strip()
        except:
            stock = None
            
#%% 2.2 PHASE II: Get Dimension, Color, Material: 
            
    # Info  Lỗi khi crawling info nhiều LINK    
        try:
            material = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Chất liệu']/.."))).get_attribute('innerText').split(":")[1].strip() #ok
        except:
            # None
            try:
                material = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Chất liệu :']/.."))).get_attribute('innerText').split(":")[1].strip()
            except:
                material = None
        try:
            color = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Màu sắc']/.."))).get_attribute('innerText').split(":")[1].strip() #ok
        except:
            color = None
        try:
            dimension = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Kích thước :']/.."))).get_attribute('innerText').split(":")[1].strip() #ok
        except:
            # None 
            try:
                dimension = wait.until(EC.visibility_of_element_located((By.XPATH, "//strong[text()='Kích thước']/.."))).get_attribute('innerText').split(":")
            except:
                dimension = None
        elems = driver.find_elements_by_xpath(('//div[@class="product-image-big"]'))    
        for elem in elems:
            time.sleep(2)
            try:
                sub_link = sub_link = elem.find_element_by_xpath('.//img').get_attribute('src')   
                urllib.request.urlretrieve(sub_link, current_path + "/data/Modern_House_" + str(Product_name) + ' ' + str(current_date) +'.jpg') # save image
            except:
                None
                print('Link error: {}'.format(i))
        
        
        dict_p = {'Product_name': Product_name, 'Price': price, 'Types': types, 'Stock': stock,
                  'Material': material, 'Color': color, 'Dimension': dimension,
                  'link_p': link}  #  
        data.append(dict_p)
        c+=1
        c_1+= float(1*100/n_p) 
        print('- Product completed: {}, ({:.0f}%)'.format(c, c_1))
    except:
        # None
        e += 1
        print('- Link error: {}, ({})'.format(link, e))
        Link_error.append(link)

driver.quit()
df = pd.DataFrame(data)

Link_error = list(dict.fromkeys(Link_error))

df_error_link = pd.DataFrame(data = Link_error, columns= ['link_p'])

df['SKU'] = df['Product_name'].copy()
#%% CHECK SKU DUP:
new_sku = list(set(df['SKU'].tolist()) - set(df_ggs['SKU'].tolist()))
print("                          ")
print('--------------NUMBER OF NEW PRODUCT MODERN HOUSE: {}-------------'.format(len(new_sku)))
print("                          ")    
    


df.to_excel(current_path + "/data/Data_Modern House_Phase_I.xlsx", 
            index = False) # done ngày 2/11/22
            

print('                                      ')
print('---------COMPLETED PHASE II - DESCRIPTION -----------')
print('                                      ')
print('                                      ')
#%% FILL DESCRIPTION PHASE I
df = pd.read_excel(current_path + "/data/Data_Modern House_Phase_I.xlsx")

def fix(r):
    temp = r['Dimension']
    try:
        return literal_eval(temp)
    except:
        return ['']

df['Dimension'] = df.apply(fix, axis = 1)
    
def add_dimension(r):
    temp = r['Dimension']
    for i in temp:
        if '0' in i.lower() and 'x' in i.lower():
            return i
df['Dimension 2'] = df.apply(add_dimension, axis = 1)

def add_dimension2(r):
    temp = r['Dimension']
    temp_1 = r['Dimension 2']
    if temp_1 == '':
        for i in temp:
            if 'mm' in i and 'x' in i:
                return i
    else:
        return temp_1
df['Dimension 2'] = df.apply(add_dimension2, axis = 1)

df['Dimension'] = df['Dimension 2'].copy()



link_fill_dimension = list(dict.fromkeys(df.loc[df['Dimension'].isnull()]['link_p'].tolist()))   
link_fill_des = list(dict.fromkeys(df.loc[df['Material'].isnull()]['link_p'].tolist()))    

link_fill_des += link_fill_dimension    
      
link_fill_des = list(dict.fromkeys(link_fill_des))


options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

wait = WebDriverWait(driver, 3)

data = []  
for link in link_fill_des:   # Link_error
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

        driver.implicitly_wait(1)
        options = Options()
        options.add_argument("--disable-notifications")
            
        try:
            Product_name = Product_name = driver.find_elements_by_xpath('.//div[@class="product-details"]/h1')[0].text
        except:
            Product_name = None
            # None
        description = [] 
        des = []
        for t in driver.find_elements_by_xpath('//li[@data-mce-fragment="1"]'): #ul
            try:
                t = t.get_attribute("innerText")
            except:
                t = None
            des.append(t)


         
        dict_p = {'Product_name': Product_name, 
                  'description': des,
                  'link_p': link}  #  
        data.append(dict_p)
        # c+=1
        # c_1+= float(1*100/n_p) 
        # print('Product completed: {}, ({:.0f}%)'.format(c, c_1))
    except:
        None

        print('Link error: {}'.format(link))
        # Link_error_2.append(link)

driver.quit()
df_2 = pd.DataFrame(data) 

           
df_sp = pd.read_excel(current_path + "/data/Data_Modern House_Phase_I.xlsx")                         
df_2 = df_2.merge(df_sp, how = 'left', on = 'link_p')                   
   
                  
df_2.rename({'Product_name_x': 'Product_name'}, axis = 1, inplace = True)
df_2 = df_2[['Product_name', 'Price', 'Types', 'Stock', 'description', 'link_p']]

df_2.to_excel(current_path + "/data/Data_Modern House_Phase_II.xlsx", index = False)
   
df_2.info()
print('                                      ')
print('---------COMPLETED PHASE III - FILL DESCRIPTION -----------')
print('                                      ')
print('                                      ')                    
#%% FILL DESCRIPTION PHASE II
df = pd.read_excel(current_path + "/data/Data_Modern House_Phase_II.xlsx")

check = df['description'].value_counts()                    

df['description'] = df['description'].astype(str)

df['description'] = df['description'].replace('[]', np.nan)       

link_fill_des = list(dict.fromkeys(df.loc[df['description'].isnull()]['link_p'].tolist()))                 



options = Options()
options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
driver = webdriver.Chrome(chrome_options=options, executable_path=current_path + "/data/chromedriver.exe", )
driver.set_window_size(1920, 1080)

wait = WebDriverWait(driver, 3)
c = 0
c_1 = 0

n_p = len(link_fill_des)
data = []  
for link in link_fill_des:   # Link_error
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

    driver.implicitly_wait(1)
    options = Options()
    options.add_argument("--disable-notifications")
        
    description = [] 
    des = []
    for table in driver.find_elements_by_xpath('//div[@class="panel-body"]'):
        des = [item.text for item in table.find_elements_by_xpath(".//*[self::ul or self::li or self::span]")]
        description.append(des) 

                          

     
    dict_p = { 
      'description': des,
      'link_p': link}  #  
    data.append(dict_p)
    c+=1
    c_1+= float(1*100/n_p) 
    print('Product completed: {}, ({:.0f}%)'.format(c, c_1))

driver.quit()
df_2 = pd.DataFrame(data) 


def remove_space(r):
    temp = r['description']
    for i in temp:
        if i == ' ' or i == '':
            temp.remove(i)
        continue
    return temp
df_2['description'] = df_2.apply(remove_space, axis = 1)

df_2.to_excel(current_path + "/data/Data_Modern House_Phase_II_SP.xlsx", index = False) 

#%% ETL DESCRIPTION
df_2 = pd.read_excel(current_path + "/data/Data_Modern House_Phase_II_SP.xlsx")
df = pd.read_excel(current_path + "/data/Data_Modern House_Phase_II.xlsx")                     
                                        
df = df.merge(df_2, how = 'left', on = 'link_p')

df_p1 = pd.read_excel(current_path + "/data/Data_Modern House_Phase_I.xlsx")
                     
df_all = pd.concat([df, df_p1])                      

df = df_all.groupby('link_p').apply(lambda group: group.dropna(subset=['description_x'])).reset_index(drop=True)

df_color = df_all[['link_p', 'Color']].loc[df_all['Color'].notnull()].drop_duplicates(subset = 'link_p')

df.drop(['Color'], axis = 1, inplace = True)

df = df.merge(df_color, how = 'left', on = 'link_p')


def fix_des(r):
    temp1 = r['description_x']
    temp2 = r['description_y']
    if temp1 == '[]':
        return temp2
    else:
        return temp1
df['description_x'] = df.apply(fix_des, axis = 1)

df.rename({'description_x': 'description'}, axis = 1, inplace = True)
df.drop(['description_y'], axis = 1, inplace = True)


check = df['link_p'].value_counts()

check_1 = df.loc[df['link_p'] == 'https://modernhousevn.com/collections/new-arrival/products/sofa-bang-rena-kem-hrw']

                     

df.loc[:,'description'] = df.loc[:,'description'].apply(lambda x: literal_eval(x)) 


def add_dimension(r):
    temp = r['description']
    for i in temp:
        if '0' in i.lower() and 'x' in i.lower():
            return i
df['Dimension'] = df.apply(add_dimension, axis = 1)
     
check = df.loc[df['Dimension'] == '0 x ']

def fix_dimension(r):
    temp = r['Dimension']
    if temp == '0 x ':
        return ' '.join(map(str, r['description']))
    else:
        return r['Dimension']
df['Dimension'] = df.apply(fix_dimension, axis = 1)
   


def add_material2(r):
    temp = r['Material']
    temp_1 = r['description']
    lst_m = ['mdf', 'mfc','cao su', 'acrylic', 'simili', 'sồi', 'polyester', 'canvas', 'cotton', 'gỗ tràm', ' gỗ ash', 'khung']
    lst = []
    if pd.isna(temp):
        for i in temp_1:
            for j in lst_m:
                if j in i.lower():
                    lst.append(i)
        return lst

    else:
        return r['Material']
df['Material'] = df.apply(add_material2, axis = 1)

check = df[['description', 'Material']]


def add_dimension2(r):
    temp = r['description']
    temp_1 = r['Dimension']
    if pd.isna(temp_1):
        try:
            for i in temp:
                if 'mm' in i.lower() and 'x' in i.lower():
                    return i
        except:
            None
    else:
        return temp_1
df['Dimension'] = df.apply(add_dimension2, axis = 1)
        


#%%


df.to_excel(current_path + "/data/Data_Modern House_Phase_III.xlsx", index = False)

print('----------------COMPLETED PHASE II: CRAWL DESCRIPTION-----------------')
#%% ETL DATA
import pandas as pd
import numpy as np


df = pd.read_excel(current_path + "/data/Data_Modern House_Phase_III.xlsx")
                   
df = pd.read_excel(current_path + "/data/Data_Modern House_Phase_I.xlsx")                   
df.drop(['Types'], axis = 1, inplace = True)
                   
def add_cols(r):
    temp = r['Product_name']
    result = temp.split(' ')[:2]
    return ' '.join(result)
df['Check_name'] = df.apply(add_cols, axis = 1)                
       
                   
#%% FILL = HAND

df_sp_etl = pd.read_excel(current_path + "/data/Sp_ETL_Types_Modern_House.xlsx")
# df_sp_etl = pd.read_excel("I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Sp_ETL_Types_BAYA (BACK-UP).xlsx") # BACKUP               
   
df_sp_etl = df_sp_etl[['Check_name', 'Types']].drop_duplicates()



df = df.merge(df_sp_etl, how = 'left', on = 'Check_name')
       
check_types_NULL = df.loc[df['Types'].isnull()][['Check_name', 'Types', 'link_p']].drop_duplicates(subset = 'Check_name')
                
print('--------------------------')
print('--------------------------')
print('TYPES IS NULL: {}'.format(check_types_NULL))
print('--------------------------')
print('--------------------------')




#%%
check = df.loc[df['Product_name'] == 'Sofa góc Buri AX17-3G']     
        
df['Price'] = df['Price'].astype(str)

def fix_price(r):
    lst_special = "['.₫|,']"
    temp = r['Price']
    for i in lst_special:
        temp = temp.replace(i, '')  
    return temp
df['Price'] = df.apply(fix_price, axis = 1)

# df = df.loc[(df['Price'] != '0') & (df['Price'].notnull())]

df['Stock'] = df['Stock'].str.replace('in stock.', '')


df['Stock'] = df['Stock'].str.replace('Hết hàng', '0')

# check_sp = df.loc[df['Types'] == 'tủ kệ'][['Product_name', 'Price', 'link_p']]     
# Add Room

dict_all = {
            'Bed Room': ['Set Wardrobe', 'Set Dresser', 'Wardrobe', 'Dresser', 'set dresser', 'Mirror', 'Bed Normal', 'Bedside', 'Bunk', 'Clothes racks',
                         'Folding Bed', 'set bed room', 'Pallet Bed', 'Set Bed Room'],
            'Dining Room': ['Set Dining table', 'Set Dining bench', 'Set Dining chair', 'Dining table', 'Dining chair', 'Dining bench', 
                            'Dining Table Set', 'Bar Table', 'Bar Chair', 'Dining cabinet', 'set dining room', 'set 4 chairs'], #ok
            'Kitchen Room': ['Kitchen storage', 'Kitchen cabinet'],            #ok
            'Living Room': ['Set Sofa table', 'Sofa table', 'TV cabinet', 'Set TV Cabinet','Book shelf', 'Shoes shelf', 'Sofa', 'Sofa arm chair', 'Stool', 
                            'Chest of drawer', 'Decoration shelf', 'Sofa Bed', 'Sofa 2 seaters', 'Sofa Arm Chair', 'Sofa Corner', 'Sofa 1 seater', 
                            'Relaxed Chair', 'Sofa 3 seaters', 'Bench', 'cabinet 1 door iconico', 'cabinet 2 doors iconico', 'Armchair', 'Decoration desk',
                            'set living room', 'Shoes bench', 'Set Storage Cabinet', 'Set Living Room', 'Set Sofa Table',
                            'Rocking Chair', 'Bench With Storage', 'Children Armchair', 'Set sofa table & chair'],
            'Office': ['Desk', 'Storage Cabinet', 'Desk chair', 'Reception Desk', 'set office room'],
            # 'Other': ['None', 'Gift Voucher', 'Other'],    
            'Outdoor': ['Set Outdoor Table', 'Outdoor Table', 'Folding Chair - Outdoor', 'Swing chair', 'Outdoor Chair'],
            
            
            'Bath Room': ['Bath Storage', 'Set Bath Room'],
            'Decoration/ Accessory': ['Blanket/ Pillow', 'Decoration Mirror', 'Decoration Accessories', 'Kitchen Accessory', 
                                      'Painting', 'Carpet', 'Candle', 'Pot', 'Lamp', 
                                      'Other', 'Seat Cushions', 'Decoration/ Accessory', 'Accessories', 'Wall shelf', 'Toy']} #, 'Table Accessory'
df['Room'] = ''

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



df['Brand'] = Brand
df['Date_updated'] = current_date

for i in ['Types', 'Product_name']:
    df[i] = df[i].str.title()


#
new_product = df.copy()

lst_n = list(set(list(df_ggs.columns.tolist())) - set(list(df.columns.tolist())))
print(lst_n)
for i in lst_n:
    new_product[i] = ''



new_product.columns
new_product = new_product[df_ggs.columns]
new_product['Date_get_new_product'] = current_date # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251
new_product.iloc[: , -1] = 'X'















                
            
