# -*- coding: utf-8 -*-
"""
Created on Thu Dec  8 08:00:34 2022

@author: aston
"""
from IPython import get_ipython
get_ipython().magic('reset -sf') 
import pandas as pd
import numpy as np
import datetime as dt
import re
import gspread
import csv
import time
import urllib.request
import glob
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from collections import namedtuple

now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")

gc = gspread.service_account(filename='''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/0. ETL's CODING To Export Data Studio/project-aftersale-moho-cd1338f28ec9.json''')
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251


#%% Update PRICE MOHO
# lst_file = []
# for file in glob.glob('''D:/MOHO - DOANH SỐ\FILE SẢN PHẨM MOHO 2/*.csv'''): # RIVAL PRICE
#     lst_file.append(file)
# num_df = len(lst_file)    
# lst_df = []
# for i in range(1, len(lst_file) + 1):
#     lst_df.append('df' + str(i))
# df = namedtuple('Cdfs',
#                 lst_df
#                )(*[pd.read_csv(file) for file in lst_file])
# lst_df = []
# for i in df:
#     lst_df.append(i)
# df_moho = pd.concat(lst_df, axis=0)  

# df_moho = df_moho[['Mã phiên bản sản phẩm', 'Giá']].drop_duplicates(subset = 'Mã phiên bản sản phẩm', keep = 'last')
# df_moho = df_moho.loc[~(df_moho['Mã phiên bản sản phẩm'].isnull()) & ~(df_moho['Giá'].isnull())]


# check = df_moho['Mã phiên bản sản phẩm'].value_counts()

# df_moho.rename({'Mã phiên bản sản phẩm': 'SKU', 'Giá': 'Price'}, axis = 1, inplace = True)

# # df_moho['Date_Update'] = '12.09.23'
# df_moho['Date_Update'] = today
# df_moho['Brand'] = 'Moho'

# df_moho.to_excel('''D:/MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web\Data Crawl\Price_Rival/Data_Price_Moho_Web_12.09.23.xlsx''', index = False)


# #%% COMEHOME
# worksheet = sh.get_worksheet(7)
# df = worksheet.get_all_values()
# cols = df[0]
# df = pd.DataFrame(df, columns = cols)  #'Unnamed: 0' 
# df_comehome = df.iloc[1: , :]

# ids = df_comehome['SKU']
# df_dup = df_comehome[ids.isin(ids[ids.duplicated()])].sort_values("SKU")
# lst_sku_dup = list(dict.fromkeys(df_dup['SKU'].tolist()))
# df_dup['SKU'] = df_dup['SKU'] + '_' + df_dup['Dimension']
# df_dup['SKU'] = df_dup['SKU'].apply(lambda x: x.replace('cm', ''))

# df_single = df_comehome.loc[~df_comehome['SKU'].isin(lst_sku_dup)]

# df_comehome = pd.concat([df_dup, df_single])

# check = df_comehome['SKU'].value_counts().reset_index()
# for i in list(dict.fromkeys(check['SKU'].tolist())):
#     if i >= 2:
#         print('DUPLICATED SKU')
#     else:
#         print('NO DUPLICATED SKU')
   
# df_comehome.columns    
   

# df_comehome['Date_Update'] = today
# df_comehome = df_comehome[['Brand', 'SKU', 'Price', 'Date_Update']]      
   
    
# df_comehome.to_excel('''D:/MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web\Data Crawl\Price_Rival/Data_Price_ComeHome_Web_12.09.23.xlsx''', index = False)

#%% CONCAT ALL PRICE 

lst_file = []
for file in glob.glob('''D:/MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web\Data Crawl\Price_Rival/*.xlsx'''): # RIVAL PRICE
    lst_file.append(file)
num_df = len(lst_file)    
lst_df = []
for i in range(1, len(lst_file) + 1):
    lst_df.append('df' + str(i))
df = namedtuple('Cdfs',
                lst_df
               )(*[pd.read_excel(file) for file in lst_file])

lst_df = []
for i in df:
    lst_df.append(i)
df_all_p = pd.concat(lst_df, axis=0)  

df_all_p.columns


check = df_all_p[['SKU', 'Price', 'Date_Update']].drop_duplicates()
check['SKU'] = check['SKU'].astype(str)  
    



# df_all_p['Date_Update'] = df_all_p['Date_Update'].dt.strftime('%d.%m.%y')
df_all_p['Date_Update'] = pd.to_datetime(df_all_p['Date_Update'], errors='coerce')
df_all_p['Month_Update'] = df_all_p['Date_Update'].dt.strftime('%B-%Y')
df_all_p = df_all_p.groupby(['Brand', 'SKU', 'Month_Update']).median().reset_index()


for i in ['Brand', 'SKU']:
    df_all_p[i] = df_all_p[i].astype(str)
df_all_p.rename({'Price': 'Price_NEW'}, axis = 1, inplace = True)


from datetime import datetime
current_month_1 = datetime.now()
current_month = current_month_1.strftime('%B-%Y')

import datetime
import dateutil.relativedelta
date_now = datetime.datetime.now() 
last_month = date_now + dateutil.relativedelta.relativedelta(months=-1)
last_month = last_month.strftime('%B-%Y')

lst_month = list(dict.fromkeys(df_all_p['Month_Update'].tolist()))

if current_month in lst_month:
    df_all_p = df_all_p.loc[df_all_p['Month_Update'] == current_month]
else:
    df_all_p = df_all_p.loc[df_all_p['Month_Update'] == lst_month]




for i in ['Brand', 'SKU', 'Month_Update']:
    df_all_p[i] = df_all_p[i].astype(str)

df_all_p.drop_duplicates(subset = ['Brand', 'SKU', 'Month_Update'], keep = 'last', inplace = True)


check = df_all_p[['Brand', 'SKU', 'Month_Update']].value_counts()


print('Number of Brand: {}'.format(df_all_p['Brand'].nunique()))
print('List: {}'.format(list(dict.fromkeys(df_all_p['Brand'].tolist()))))


#%% CONCAT ALL RIVAL
lst_wh = []
for i in range(0,8):
    worksheet = sh.get_worksheet(i)
    lst_wh.append(worksheet)

list_of_dfs = [pd.DataFrame(ws.get_all_records()) for ws in lst_wh]  # number of column must equal between df
df = pd.concat(list_of_dfs)


lst_col = list(df.columns)
df = df.reset_index()
df.drop(['index'], axis = 1, inplace = True)
df2 = df.iloc[:, 21:].replace('X', pd.Series(df.columns, df.columns))
lst_date = list(df.iloc[: , 21:])
df.drop(lst_date, axis = 1, inplace = True)
df = df.join(df2)

df.columns

# check = df.loc[(df['Brand'] == 'Beyours') & (df['September-2023'] == 'September-2023')]
# check1 = check.drop_duplicates(subset = ['SKU'])


df['lst_date'] = df.iloc[:, 21:].values.tolist()
def remove_blank(r):
    temp = r['lst_date']
    return list(filter(None, temp))
df['lst_date'] = df.apply(remove_blank, axis = 1)

# Exploding the list
df = df.explode('lst_date')  

len_lst = len(list(df.columns)) -1
df = df.drop(df.columns[20:len_lst], axis=1)

df.rename({'lst_date': 'Month_Update', 'Date_updated': 'Date_get_new_product'}, axis = 1, inplace = True)
df.columns
df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

df.drop_duplicates(subset = ['SKU', 'Month_Update'], inplace = True)

for i in ['Brand', 'SKU', 'Month_Update']:
    df[i] = df[i].astype(str)
df = df.merge(df_all_p, how = 'left', on = ['Brand', 'SKU', 'Month_Update'])

df['Price_NEW'].fillna(df['Price'], inplace = True)


check = df.loc[(df['Brand'] == 'Beyours') & (df['Month_Update'] == 'December-2023')]
check1 = check.drop_duplicates(subset = ['SKU'])

test=  df.copy()

check = test['Price_NEW'].value_counts()

check = test['Room'].value_counts()

check = test.loc[(test['Room'] != 'Decoration/ Accessory') & (test['Month_Update'] == 'December-2023')]
check = check.loc[check['Price_NEW'].isnull()][['Brand', 'Price', 'Price_NEW', 'link_p']]


check.info()

import datetime
today2 = datetime.date.today()
first = today2.replace(day=1)
last_month = first - datetime.timedelta(days=1)


last_month = last_month.strftime("%B-%Y")

df.columns

check = df.loc[df['Month_Update'] == last_month]

lst_brand = list(dict.fromkeys(df['Brand'].tolist()))

print('                    ')
print('DATA Competitor ' + last_month)
print('                    ')

for i1 in  lst_brand:
    check = df.loc[(df['Brand'] ==i1) & (df['Month_Update'] == last_month)]['SKU'].nunique()    
    print('Number SKU of {}: {}'.format(i1,check))

check = df['Room'].value_counts()


def fix_r(r):
    temp = r['Room']
    lst_kitchen = ['Kitchen', 'Kitchen Room', 'Kitchen ']
    for i in lst_kitchen:
        if i in temp:
            return 'Kitchen Room'
        continue
    return temp
df['Room'] = df.apply(fix_r, axis = 1)

df.columns


check = df[['Brand', 'Room', 'Types', 'SKU','Product_name', 'Price', 'Price_NEW']].drop_duplicates()



# df = df_rival.merge(df_all_p, how = 'left', on = ['Brand', 'SKU'])
# df['Price_NEW'] = df['Price_NEW'].fillna(df['Price'])


# df.drop(['Price'], axis =1, inplace = True)
# df.rename({'Price_NEW': 'Price'}, axis = 1, inplace = True)

# df = df[lst_col]

# df2 = df[['Brand', 'SKU', 'Product_name', 'Price', 'link_p']]

# test = df2.merge(df_all_p, how = 'left', on = ['SKU'])
# test = test.loc[test['Price_NEW'].notnull()][['Brand_x', 'Room', 'Types', 'Product_name_x', 'Price', 'Price_NEW']]

# print('All Rival Number: {}'.format(df_rival['Brand'].nunique()))
# print('Rival: {}'.format(list(dict.fromkeys(df_rival['Brand'].tolist()))))



# print(test['Brand_x'].nunique())
# print('Rival: {}'.format(list(dict.fromkeys(test['Brand_x'].tolist()))))



  
#%%
# lst_wh = []
# for i in range(0,7):
#     worksheet = sh.get_worksheet(i)
#     lst_wh.append(worksheet)

# list_of_dfs = [pd.DataFrame(ws.get_all_records()) for ws in lst_wh]  # number of column must equal between df
# df = pd.concat(list_of_dfs)
  
# df.columns  

# check = df['Brand'].value_counts()








# df.columns
# check = df.loc[df['Brand'] == 'Make My Home']

# check_1 = check['link_p'].value_counts()

#%% FIX PRICE BY MONTH
df.columns

check_m = df['Month_Update'].value_counts()






#%% RE-ROOM FOR ALL TYPES:
df_tableau= pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_Competitors_Tableau.xlsx''')
 
# df_tableau['Types'] = df_tableau['Types'].replace('Armchair', 'Sofa 1 Seater')                          
# df_tableau['Types'] = df_tableau['Types'].replace('Bench', 'Dining Bench') 
      
# df.loc[df.my_channel > 20000, 'my_channel'] = 0  
# df_tableau.loc[df_tableau.Types == 'Dining Bench', 'Room'] = 'Dining Room'
            
# check = df_tableau['Types'].value_counts()

# check = df_tableau.loc[df_tableau['Types'] == 'Dining Bench'][['Types', 'Room']].drop_duplicates()
        
 
 
 





# df_tableau.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_Competitors_Tableau.xlsx''', index=  False)



 

 
df_sp = df_tableau[['Room', 'Types']].drop_duplicates()
 
df_sp = df_sp.sort_values(by = ['Room'])

df_sp.to_excel('''I:\MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web/Sp_Room_Types.xlsx''', index = False)





# df_room = pd.DataFrame(dict_room.items(), columns=['Room', 'Types'])# SAVE ROOM

# df_room.to_excel('''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/ROOM_MEANING.xlsx''', index = False)





#%% BACKUP BEFORE SAVE NEW ONE:
current_date = now1.strftime("%Y-%m-%d")
    
df_old = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_Competitors_Tableau.xlsx''')
df_old.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/BACKUP/Data_Competitors_Tableau_''' + str(current_date) + '.xlsx', index = False)

                
check = df[['Room', 'Types']].drop_duplicates()
check_1 = check.loc[check['Room'] == 'Living Room']             

fix_types = {'Sofa': ['Sofa 3 Seaters'], 'Sofa 1 Seater': ['Armchair']}

def fix_type(r):
    temp = r['Types'].lower()
    for key, values in fix_types.items():
        for i in values:
            if temp == i.lower():
                return key
            continue
        continue
    return temp
df['Types'] = df.apply(fix_type, axis = 1)  


df.columns
for i in ['Brand', 'Room', 'Types']:
    df[i] = df[i].str.title()




df.drop(['Price'], axis = 1, inplace = True)

df.rename({'Month_Update': 'Date_Updated',
           'Price_NEW': 'Price'}, axis =1 , inplace = True)




df['Types'] = df['Types'].replace('Sofa 2 Seater', 'Sofa 2 Seaters')  
df['Types'] = df['Types'].replace('Sofa', 'Sofa 3 Seaters')     
     
df['Types'] = df['Types'].replace('Sofa 3 Seaters 60', 'Sofa 3 Seaters 160')    
              
df.columns                  
                   
check = df['Types'].value_counts()
                   
                   
check = df['Types_2'].value_counts()
                   
check = df['Room'].value_counts()         
            

df['Types_2'] =df['Types_2'].astype(str)

df['Types_2_split'] = df['Types_2'].apply(lambda x: x.split(' ')[-1])

def fix_types_2(r):
    temp = r['Types_2_split']
    if len(temp) >=4:
        return temp[:3]
    else:
        return temp
df['Types_2_split'] = df.apply(fix_types_2, axis = 1)

check = df.loc[df['Types'].isin(['Sofa 3 Seaters'])][['Types', 'Types_2', 'Types_2_split']]

standard_sizes = [60, 100, 140, 160, 180, 200, 220, 240, 300]
def fix_size(value):
    try:
        # Extract the number from the string
        number = int(re.findall(r'\d+', value)[-1])
        # Find the closest standard size within +/- 20
        closest_size = min(standard_sizes, key=lambda x: abs(x - number))
        return closest_size if abs(closest_size - number) <= 20 else number
    except IndexError:
        # Return the original value if no number is found
        return value
df['Types_2_split'] = df['Types_2_split'].apply(lambda x: re.sub(r'\d+', str(fix_size(x)), x))

check = df.loc[df['Types'].isin(['Sofa 3 Seaters', 'Bed Normal'])][['Types', 'Types_2', 'Types_2_split']].drop_duplicates()



df['Types_2_split'] = df['Types_2_split'].replace('nan', np.nan)

df['Types_2'] = df['Types'] + ' ' + df['Types_2_split']
df['Types_2'].fillna(df['Types'], inplace = True)     

df.drop(['Types_2_split'], axis = 1,inplace = True)


df.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_Competitors_Tableau.xlsx''', index = False)

df_performance = pd.read_csv('''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/0. ETL's CODING To Export Data Studio/Data_Sales_For_Moho_Performance.csv''')            

df_performance.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data After ETL/Data_Sales_For_Moho_Performance.xlsx''', index = False)                             

            
#%% CHECK DUP

# df = pd.read_excel('''I:\MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web\Data After ETL\BACKUP/Data_Competitors_Tableau_2023-01-05.xlsx''')

# df = pd.read_excel('''I:\MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web\Data After ETL\Data_Competitors_Tableau.xlsx''')
                   

check = df.loc[df['Brand'] == 'Make My Home']
check = check['link_p'].value_counts()


#%%
#%%
#%%% ANALYZE DATA  COMPETITORS

import pandas as pd
import numpy as np
import datetime as dt
import unidecode
import glob
from datetime import datetime, timedelta
from collections import namedtuple
import datetime
today2 = datetime.date.today()
first = today2.replace(day=1)
last_month = first - datetime.timedelta(days=1)

from datetime import datetime
current_month_1 = datetime.now()
current_month = current_month_1.strftime('%B-%Y')




df_all = pd.read_excel('''I:\MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web\Data After ETL/Data_Competitors_Tableau.xlsx''')
        
df_all.columns                     
        
check_Types_NULL = df_all.loc[df_all['Types'].isnull()][['SKU', 'Product_name', 'Brand', 'link_p']]


                           
                                        
df_moho = pd.read_csv('''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/0. ETL's CODING To Export Data Studio/Data_Sales_For_Moho_Performance.csv''')                  

df_all.columns                        

#%% Filter SKU NOT PRODUCE
first = today2.replace(day=1)
current_month_update = first.strftime("%B")
Month_Update = current_month_update #  <<<<====================== IMPORTANCE  



lst_moho = pd.read_excel('''I:\MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web/SKU_NOT_PRODUCE.xlsx''')

lst_remove_SKU = list(dict.fromkeys(lst_moho['SKU'].tolist()))
df_moho = df_moho.loc[~df_moho['product_code'].isin(lst_remove_SKU)]
                       
         
                        
df = df_all.copy()               
df.columns
check = df['Date_Updated'].value_counts()
check = df_all.loc[df_all['Types'] == 'Chest Of Drawer']
check = df_moho.loc[df_moho['product_code'] == 'MWBWCBN01.K04']


df = df.loc[df['Date_Updated'] ==  str(Month_Update) + '-2025']   # change date_update # 

df['Date_Updated'] = pd.to_datetime(df['Date_Updated']).dt.date



df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

check = df['Price'].value_counts().reset_index()
check = df['Date_Updated'].value_counts()

# PRICE STATISTICS BY TYPES:
df2 = df_all.copy()
# df2 = df2.loc[~df2['Brand'].isin(['Moho'])]    
    
df2 = df2.groupby(['Room','Types']).describe().reset_index()
df2.columns


df2 = df2[[( 'Room',      ''),
            ('Types',      ''), ('Price',   '25%'),('Price',   '50%'), ('Price',   '75%')]]

df2.columns = df2.columns.droplevel(1)  

df2.columns = ['Room', 'Types', 'Price_Low','Price_Median','Price_High']


#%%
df_room = df_all[['Room', 'Types']].drop_duplicates(subset = ['Types'])

df_types = df_all[['Types','Price']]

def fix_v(r):
    temp = r['Types']
    return temp.replace(' ', '_')
df_types['Types'] = df_types.apply(fix_v, axis = 1)
    
print(df_types.shape)
    
dfs = {f'df_{n}': df_types[df_types['Types'] == n] for n in df_types['Types'].unique()}



lst_df = []
for key, value in dfs.items():
    temp = [key,value]
    lst_df.append(value)

lst_df2 = []
for i in lst_df:
    
    Q1 = np.percentile(i['Price'], 25, interpolation = 'midpoint')
    Q3 = np.percentile(i['Price'], 75, interpolation = 'midpoint')
    IQR = Q3 - Q1
    
    upper = i.loc[i['Price'] >= (Q3+1.5*IQR)]['Price'].tolist()
    lower = i.loc[i['Price'] <= (Q1-1.5*IQR)]['Price'].tolist() 
    
    i = i.loc[~(i['Price'].isin(upper)) & ~i['Price'].isin(lower)]
 
    lst_df2.append(i)

df2 = pd.concat(lst_df2)
    
def fix_v(r):
    temp = r['Types']
    return temp.replace('_', ' ')
df2['Types'] = df2.apply(fix_v, axis = 1)

df2 = df2.merge(df_room, how = 'left', on = ['Types'])



df2 = df2.groupby(['Room','Types']).describe().reset_index()


df2 = df2[[( 'Room',      ''),
            ('Types',      ''), ('Price',   '25%'),('Price',   '50%'), ('Price',   '75%')]]

df2.columns = df2.columns.droplevel(1)  

df2.columns = ['Room', 'Types', 'Price_Low','Price_Median','Price_High']



# df2 = df2.loc[df2['Room'].isin(['Bed Room', 'Dining Room', 'Living Room', 'Office'])]

#%%
df2.to_excel('''D:/REPORT MONTHLY/Attractive_Price_''' + str(Month_Update) + '_2025.xlsx', index = False)

# df2.to_excel('''D:/REPORT MONTHLY/Attractive_Price_August_2023.xlsx''', index = False) # REPLACE "NEW MONTH" BEFORE SAVE

#%%
# Comparing with 
import datetime
today2 = datetime.date.today()
first = today2.replace(day=1)
last_month_1 = first - datetime.timedelta(days=1)


last_month = last_month_1.strftime("%B")
# last_month = 'January'

df_spec = pd.read_excel('''D:/REPORT MONTHLY/Attractive_Price_''' + str(last_month)  +'''_2025.xlsx''')  # REPLACE "MONTH" BEFORE SAVE
df2 = pd.read_excel('''D:/REPORT MONTHLY/Attractive_Price_''' + str(Month_Update) + '_2025.xlsx')


df2.columns


test = df2.merge(df_spec, how = 'left', on = ['Room', 'Types'])

test.columns
def different_low(r):
    temp1 = r['Price_Low_x']
    temp2 = r['Price_Low_y']
    return (temp1 - temp2)*100/temp2
test['Different_LOW'] = test.apply(different_low, axis = 1)


def different_median(r):
    temp1 = r['Price_Median_x']
    temp2 = r['Price_Median_y']
    return (temp1 - temp2)*100/temp2
test['Different_MEDIAN'] = test.apply(different_median, axis = 1)

def different_high(r):
    temp1 = r['Price_High_x']
    temp2 = r['Price_High_y']
    return (temp1 - temp2)*100/temp2
test['Different_HIGH'] = test.apply(different_high, axis = 1)

for i in ['Different_LOW', 'Different_MEDIAN', 'Different_HIGH']:
    test[i] = test[i].apply(lambda x: np.round(x, 2))

# test.drop(['Price_Low_y', 'Price_Median_y', 'Price_High_y'], axis = 1, inplace = True)


# test['Price_Low'] = str(test['Price_Low_x']) + str(test['Different_LOW'])



    

def add_str(r):
    temp1 = r['Price_Low_x']
    temp2 = r['Different_LOW']
    if temp2 > 0:
        return str(int(float(temp1))) + '+'+ str(temp2) + '%'
    elif temp2 == 0:
        return temp1
    elif temp2 <0:
        return str(int(float(temp1))) + str(temp2) + '%'
test['Price_Low'] = test.apply(add_str, axis = 1)   
    
def add_str(r):
    temp1 = r['Price_High_x']
    temp2 = r['Different_HIGH']
    if temp2 > 0:
        return str(int(float(temp1))) + '+'+ str(temp2) + '%'
    elif temp2 == 0:
        return temp1
    
    elif temp2 <0:
        return str(int(float(temp1))) + str(temp2) + '%'
test['Price_High'] = test.apply(add_str, axis = 1) 

def add_str(r):
    temp1 = r['Price_Median_x']
    temp2 = r['Different_MEDIAN']
    if temp2 > 0:
        return str(int(float(temp1))) + '+'+ str(temp2) + '%'
    elif temp2 == 0:
        return temp1
    elif temp2 <0:
        return str(int(float(temp1))) + str(temp2) + '%'
test['Price_Median'] = test.apply(add_str, axis = 1) 

test2 = test.dropna()

df_sp_price = test2[['Room', 'Types', 'Price_Low_x', 'Price_Median_x', 'Price_High_x']]

df_sp_price.rename({'Price_Low_x': 'Price_Low',
              'Price_Median_x': 'Price_Median',
              'Price_High_x': 'Price_High'}, axis = 1, inplace = True)

today2 = datetime.date.today()
first = today2.replace(day=1)
last_month = first - datetime.timedelta(days=1)


last_month = last_month.strftime("%m")


df_sp_price.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/SKU Competitors/Sp_Competed_PRICE_T''' + str(last_month) + '''_2025.xlsx''', index = False)

#%%
#%%
#%% LIST PRODUCT IS EXISTED AND NOT 
# df_p = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/SKU Competitors/Sp_Competed_PRICE_T2_2023.xlsx''') # Price_Median_High

# df_p = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/SKU Competitors/Sp_Competed_PRICE_T11_2023.xlsx''') # Price_Median_High

      
df_p = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/SKU Competitors/Sp_Competed_PRICE_T''' + str(last_month) + '''_2025.xlsx''') # Price_Median_High

      

# df_sp_price.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/SKU Competitors/Sp_Competed_PRICE_T''' + str(last_month) + '''_2023.xlsx''', index = False)

               
                 
lst_wh = []
for i in range(0,7):
    worksheet = sh.get_worksheet(i)
    lst_wh.append(worksheet)

list_of_dfs = [pd.DataFrame(ws.get_all_records()) for ws in lst_wh]  # number of column must equal between df
df = pd.concat(list_of_dfs) 
  
df.columns

df = df.loc[df['Brand'] != 'Moho'] # 5609 SKU

lst_date = list(df.iloc[: , 21:])

print(lst_date)

# for i in lst_date:
#     df2 = df.loc[df[i] == 'X']

#%%
#%%
#%% FILTER TOP SP CÒN SỐNG  VÀ GIÁ THẤP HƠN Price_High
# df_top = df.loc[(df['February-2023'] == 'X') & (df['January-2023'] == 'X')] # DATA  <= T2/2023 


if current_month in lst_month:
    month_2 = current_month_1
else:
    month_2 = last_month_1




import datetime
# today2 = datetime.date.today()
# first = today2.replace(day=1)
previous_month = month_2 + dateutil.relativedelta.relativedelta(months=-1)
month_2 = month_2.strftime("%B-%Y")

month_1 = previous_month.strftime("%B-%Y")


# month_1 = 'December-2023'
# month_2 = 'January-2023'

df.columns

df_top = df.loc[(df[month_1] == 'X') & (df[month_2] == 'X')] # DATA  <= T5/2023 




df_p.columns
df_top = df_top.merge(df_p, how = 'left', on = ['Types', 'Room'])
df_top = df_top.loc[~(df_top['Price_Low'].isnull()) & ~(df_top['Price_Median'].isnull()) & ~(df_top['Price_High'].isnull())]


df_top.columns


df_top['Price'] = pd.to_numeric(df_top['Price'], errors='coerce')
df_top = df_top.loc[df_top['Price'] <= df_top['Price_High']]


#%%
worksheet = sh.worksheet('Top Product With Attractive- Price Exist')   # sh.get_worksheet(10)

df_old = worksheet.get_all_values()
cols = df_old[0]
df_old = pd.DataFrame(df_old, columns = cols)  #'Unnamed: 0' 
df_old = df_old.iloc[1:, :]


df_old['SKU'] = df_old['SKU'].astype(str)
df_top['SKU'] = df_top['SKU'].astype(str)

df_old.columns

df_top.columns



lst_new = list(set(df_top['SKU'].tolist()) - set(df_old['SKU'].tolist())) # 11 NEW SKU

lst_old = list(set(df_old['SKU'].tolist()) - set(df_top['SKU'].tolist())) 

result = pd.concat([df_old, df_top], ignore_index = True)

result.drop_duplicates(subset = ['SKU'], keep = 'last', inplace = True)

def add_d(r):
    temp = r['SKU']
    if temp in lst_new:
        return 'New'
    else:
        return 'Old' 
    
    
result['Check_New_Updated'] = result.apply(add_d, axis = 1)


result.columns

result = result[['Brand', 'Room', 'Types', 'Collection', 'Date_get_new_product', 'SKU',
       'Product_name', 'Price', 'Color', 'Description', 'Dimension',
       'Material', 'link_p', 'Style', 'Made In', 'Xuất xứ', 'Mẫu mã thiết kế:',
       'Thành phần:', 'Đặc điểm nổi bật:', 'Unit', 'September-2022',
       'October-2022', 'November-2022', 'December-2022', 
       'January-2023', 'February-2023', 'March-2023', 'April-2023', 'May-2023', 'June-2023', 'July-2023','August-2023', 'September-2023', 
       'October-2023', 'November-2023', 'December-2023', 'January-2024', 'February-2024', 'March-2024', 
       'April-2024', 'May-2024', 'June-2024', 'July-2024','August-2024', 'September-2024', 'October-2024', 
       'November-2024', 'December-2024',# -> ADD NEW MONTH HERE
       'Price_Low', 'Price_Median', 'Price_High',
       'Check_New_Updated']]

result = result.loc[result['Brand'] != 'Moho']

# result = result.loc[result['Room'] == 'Decoration/ Accessory']

test = result.copy()
test = test.loc[~test['Room'].isin(['', 'Brand', 'Baya'])]

gc = gspread.service_account(filename='''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/0. ETL's CODING To Export Data Studio/project-aftersale-moho-cd1338f28ec9.json''')
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251
worksheet = sh.worksheet('Top Product With Attractive- Price Exist')
worksheet.clear()

# APPEND DATA TO SHEET
set_with_dataframe(worksheet, test) 



#%%
#%%
#%% FILTER TOP SP CÒN BỊ BỎ ĐI VÀ thấp hơn Price_High

df_top = df.loc[~(df[month_1] == 'X') & ~(df[month_2] == 'X')]

df_p.columns
df_top = df_top.merge(df_p, how = 'left', on = ['Types', 'Room'])
df_top = df_top.loc[~(df_top['Price_Low'].isnull()) & ~(df_top['Price_Median'].isnull()) & ~(df_top['Price_High'].isnull())]



df_top['Price'] = pd.to_numeric(df_top['Price'], errors='coerce')
df_top = df_top.loc[df_top['Price'] <= df_top['Price_High']]

lst_new = list(set(df_top['SKU'].tolist()) - set(df_old['SKU'].tolist())) # 321 NEW SKU

lst_old = list(set(df_old['SKU'].tolist()) - set(df_top['SKU'].tolist())) # 1327

df_old = df_old.reset_index(drop = True)
df_top = df_top.reset_index(drop = True)


result = pd.concat([df_old, df_top])

result.drop_duplicates(subset = ['SKU'], keep = 'last', inplace = True)

def add_d(r):
    temp = r['SKU']
    if temp in lst_new:
        return 'New'
    else:
        return 'Old'
    
    
result['Check_New_Updated'] = result.apply(add_d, axis = 1)


result.columns

result = result[['Brand', 'Room', 'Types', 'Collection', 'Date_get_new_product', 'SKU',
       'Product_name', 'Price', 'Color', 'Description', 'Dimension',
       'Material', 'link_p', 'Style', 'Made In', 'Xuất xứ', 'Mẫu mã thiết kế:',
       'Thành phần:', 'Đặc điểm nổi bật:', 'Unit', 'September-2022',
       'October-2022', 'November-2022', 'December-2022', 
       'January-2023', 'February-2023', 'March-2023', 'April-2023', 'May-2023', 'June-2023', 'July-2023','August-2023', 'September-2023', 
       'October-2023', 'November-2023', 'December-2023', 'January-2024', 'February-2024', 'March-2024', 'April-2024', 
       'May-2024', 'June-2024', 'July-2024','August-2024', 'September-2024', 'October-2024', 'November-2024', 'December-2024',           # -> ADD NEW MONTH HERE
       'Price_Low', 'Price_Median', 'Price_High',
       'Check_New_Updated']]
result = result.loc[result['Brand'] != 'Moho']

test = result.copy()
test = test.loc[~test['Room'].isin(['', 'Brand', 'Baya'])]


worksheet = sh.worksheet('Top Product With Attractive-Price NONE- Exists')
worksheet.clear()

# APPEND DATA TO SHEET
set_with_dataframe(worksheet, test) 


                     

                     










                     

                     

                     

                     
#%%
#%%
#%%   ANALYZE ANOTHER           
# df3 = df_moho.copy()
# df3['order_date'] = pd.to_datetime(df3['order_date']).dt.date
# df3 = df3.loc[df3['order_date'] >= dt.date(2022, 1,1)]
# check = df3['room'].value_counts()


# #
# df_sku = df3.loc[~df3['room'].isin(['PROJECT', 'SAMPLE', 'Decoration/ Accessory', 'Kitchen Room', 'Other'])]
# df_sku = df_sku.loc[~df_sku['product_category'].isin(['Bed Accessories', 'Mirror', 'Moho Blanket/ Pillow', 'Sofa Accessories', 'Tv Cabinet Accessories',
#                                                       ])]


# lst_sku_moho = list(dict.fromkeys(df_sku['product_code'].tolist()))  # 346 SKU MOHO
# df3.columns
# # THỐNG KÊ SKU:
# df_room = df_sku[['room', 'product_category', 'product_code']].drop_duplicates().groupby(['room', 'product_category']).count()



 
# df3 = df3.groupby(['product_code'])['quantity'].sum().reset_index()

# check_qty_sold = df3['quantity'].describe()





# df0 = df[['SKU', 'Types', 'Room','Price']].drop_duplicates()
# test = df0.merge(df3, how = 'left', left_on = ['SKU'], right_on = ['product_code'])
# test = test.merge(df2, how = 'left', on = ['Room', 'Types'])
# test = test.loc[test['Room'] != 'Decoration/ Accessory']


# test = test.loc[test['product_code'].isin(lst_sku_moho)]





# test_all = df[['Brand', 'Room', 'Types', 'SKU']].groupby(['Brand', 'Room', 'Types']).count().reset_index()
# test_all = test_all.loc[~test_all['Room'].isin(['PROJECT', 'SAMPLE', 'Decoration/ Accessory', 'Kitchen Room', 'Other'])]
# test_all = test_all.loc[~test_all['Types'].isin(['Bed Accessories', 'Mirror', 'Moho Blanket/ Pillow', 'Sofa Accessories', 'Tv Cabinet Accessories'])]


# #%% TÍNH SỐ LƯỢNG SKU NẰM TRONG VÙNG AN TOÀN:

# lst_qty = test.loc[test['quantity'].notnull()]['quantity'].tolist()

# des_qty = test.loc[test['quantity'].notnull()]['quantity'].describe() # 15 - 32 - 73

# from scipy.stats import shapiro
# def get_num(lst):
#     if len(lst) < 3:
#         return round(np.mean(lst),2)
#     else:
#         stat, p = shapiro(lst)
#         alpha = 0.05
#         if p > alpha:
#             return round(np.mean(lst),2)
#         elif p <= alpha:
#             return round(np.median(lst),2)

# print(get_num(lst_qty))  # 32

# #%% TÍNH TOÁN TỶ LỆ SKU ĐẠT HIGH PERFORMANCE khi nằm trong top SKU


# test_2 = test.loc[(test['quantity'].notnull()) &  
#                   (test['Price'] <= test['Price_Low'])].drop_duplicates(subset = ['SKU'])    # 71 SKU

# counts_sku_each_types_low = test_2[['Types', 'SKU']].groupby(['Types']).count()



# test_2 = test.loc[(test['quantity'].notnull()) &  
#                   (test['Price'] <= test['Price_Median'])].drop_duplicates(subset = ['SKU']) # 153 SKU


# counts_sku_each_types_median = test_2[['Types', 'SKU']].groupby(['Types']).count()



# test_2 = test.loc[(test['quantity'].notnull()) &  
#                   (test['Price'] <= test['Price_High'])].drop_duplicates(subset = ['SKU']) # 212 SKU

# test_2 = test.loc[(test['quantity'].notnull()) &  (test['Price'] >= test['Price_Low']) &
#                   (test['Price'] <= test['Price_Median'])].drop_duplicates(subset = ['SKU'])    # 96 SKU




    
    
# test_2 = test.loc[(test['quantity'].notnull()) & (test['Price'] > test['Price_Median']) 
#                   & (test['Price'] <= test['Price_High'])].drop_duplicates(subset = ['SKU']) # 59 SKU
    
# test_2 = test.loc[(test['quantity'].notnull()) 
#                   & (test['Price'] > test['Price_High'])].drop_duplicates(subset = ['SKU'])  # 8 SKU


# #%% TÍNH SỐ LƯỢNG SẢN PHẨM BÁN CHẠY NẰM TRONG TOP 3 CỦA MỖI PRODUCT CATEGORY:
# # test =test.loc[test['quantity'] >=50]


# check_top3 = test.groupby(['Types'])['quantity'].nlargest(3).reset_index()    # 54 SKU
# test2 = test[['SKU', 'Price', 'Price_Low','Price_Median', 'Price_High']] 
# test2 = test2.loc[test2['Price_Median'].notnull()] # 220 SKU



# # Tính dựa trên 49 SKU ĐẠT TOP 3 
# check_top3 = pd.merge(check_top3, test2, left_on = ['level_1'], right_index=True) # 


# check1 = check_top3.loc[check_top3['Price'] <= check_top3['Price_Low']]    # 13




# check2 = check_top3.loc[check_top3['Price'] <= check_top3['Price_Median']] # 39

# check3 = check_top3.loc[(check_top3['Price'] <= check_top3['Price_High'])] # 48

# check4 = check_top3.loc[(check_top3['Price'] > check_top3['Price_Median']) & # 9
#                         (check_top3['Price'] <= check_top3['Price_High'])]

# check5 = check_top3.loc[(check_top3['Price'] > check_top3['Price_Low']) & # 26
#                         (check_top3['Price'] <= check_top3['Price_Median'])]



# check6 = check_top3.loc[(check_top3['Price'] > check_top3['Price_Low']) & # 35
#                         (check_top3['Price'] <= check_top3['Price_High'])]




# check7 = check_top3.loc[(check_top3['Price'] > check_top3['Price_High'])] # 1


























