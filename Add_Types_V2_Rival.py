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

gc = gspread.service_account(filename='''I:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/0. ETL's CODING To Export Data Studio/project-aftersale-moho-cd1338f28ec9.json''')
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

#%% CONCAT ALL PRICE 

lst_file = []
for file in glob.glob('''I:/MOHO's OTHER PROJECT\Crawling DATA BY PYTHON\Crawl Competitor Web\Data Crawl\Price_Rival/*.xlsx'''): # RIVAL PRICE
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
# lst_wh = []
# for i in range(0,8):
#     worksheet = sh.get_worksheet(i)
#     lst_wh.append(worksheet)

# list_of_dfs = [pd.DataFrame(ws.get_all_records()) for ws in lst_wh]  # number of column must equal between df
# df = pd.concat(list_of_dfs)

##
import pandas as pd
import gspread
from gspread.exceptions import GSpreadException

lst_wh = []
for i in range(0, 8):
    worksheet = sh.get_worksheet(i)
    lst_wh.append(worksheet)

list_of_dfs = []
expected_headers = ['Brand', 'Room', 'Types', 'Types_2','Collection', 'Date_get_new_product', 'SKU',
        'Product_name', 'Price', 'Color', 'Description', 'Dimension',
        'Material', 'link_p', 'Style', 'Made In', 'Xuất xứ', 'Mẫu mã thiết kế:',
        'Thành phần:', 'Đặc điểm nổi bật:', 'Unit', 'September-2022',
        'October-2022', 'November-2022', 'December-2022', 'January-2023',
        'February-2023', 'March-2023', 'April-2023', 'May-2023', 'June-2023',
        'July-2023', 'August-2023', 'September-2023', 'October-2023',
        'November-2023', 'December-2023', 'January-2024', 'February-2024',
        'March-2024', 'April-2024', 'May-2024', 'June-2024', 'July-2024',
        'August-2024', 'September-2024', 'October-2024', 'November-2024',
        'December-2024', 'January-2025', 'February-2025', 'March-2025']  # replace with your actual headers

# Iterate through worksheets, handle errors, and collect valid DataFrames
for ws in lst_wh:
    try:
        df = pd.DataFrame(ws.get_all_records(expected_headers=expected_headers))
        list_of_dfs.append(df)
    except GSpreadException as e:
        print(f"Error processing worksheet {ws.title}: {e}")
        continue

# Concatenate all DataFrames, ignoring index for a clean merge
if list_of_dfs:
    final_df = pd.concat(list_of_dfs, ignore_index=True)
else:
    final_df = pd.DataFrame()  # Fallback in case no DataFrames were processed

# Check the result
print(final_df)



##
df = final_df.copy()
lst_col = list(df.columns)
df = df.reset_index()
df.drop(['index'], axis = 1, inplace = True)
df2 = df.iloc[:, 21:].replace('X', pd.Series(df.columns, df.columns))
lst_date = list(df.iloc[: , 21:])
df.drop(lst_date, axis = 1, inplace = True)
df = df.join(df2)

#%%
df_all = df.copy()
df_all.columns

df = df_all[['Brand', 'Room', 'SKU', 'Product_name','Types', 'Description', 'Dimension']].drop_duplicates(subset = ['SKU'])

check = df['Room'].value_counts()


df = df.loc[~df['Room'].isin(['Decoration/ Accessory', 'Outdoor', 'Kitchen', 'Other_Room', 'Kitchen Room', ''])]

check_types = df[['SKU', 'Product_name', 'Types']]



## CHECK DATA
# types_2_all = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''')

                            
# types_2_all['Dimension'] = types_2_all['Types 2'].split()
                            

                            
# types_2_all = types_2_all[['Types 2']]
                            

                 
#%% STANDARD MOHO
types_2_all = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''')



worksheet = sh.worksheet('Moho')
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns= cols)

df_ggs = df_ggs.iloc[1:, ]


moho = df_ggs.loc[(df_ggs['Brand'] == 'Moho') & ~(df_ggs['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                           'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]


moho = moho[['Brand', 'SKU', 'Types', 'Dimension']]

moho = moho.merge(types_2_all, how = 'left', on = ['Brand', 'SKU'])

moho = moho.loc[(moho['Types_2'].notnull()) & (moho['Dimension'] != '')]
moho['Dimension_List'] = moho['Dimension'].apply(lambda x: [int(num) for num in re.findall(r'\d+', x)])

moho = moho[['Types', 'Dimension_List', 'Types_2']]
check_moho = moho.groupby(['Types']).head(5).reset_index()




#%% MOHO # DONE
moho = df.loc[(df['Brand'] == 'Moho')]

check = moho['Types'].value_counts()



moho = df.loc[(df['Brand'] == 'Moho') & ~(df['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                            'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]
moho['Types_2'] = ''

lst_types_need_fill = list(set(moho['Types'].tolist()))

check = moho[['Types', 'Product_name']].drop_duplicates()
moho = moho[['SKU', 'Product_name', 'Dimension', 'Types','Types_2']]
moho['Product_name'] = moho['Product_name'].str.lower()     

def extract_dimension(product_name):
    match = re.search(r'(\d+m\d*)', product_name)
    return match.group(0) if match else None

# Apply the function and create a new column 'Dimension'
moho['Dimension_2'] = moho['Product_name'].apply(extract_dimension)


def extract_dimension2(product_name):
    match = re.search(r'Dài (\d+)', product_name) # match = re.search(r'Dài (\d+cm)', product_name)
    return match.group(1) if match else None

# Apply the function and create a new column 'Dimension'
moho['Dimension_3'] = moho['Dimension'].apply(extract_dimension2)


moho['Final_Dimesnion'] = np.nan
moho['Final_Dimesnion'].fillna(moho['Dimension_2'], inplace = True)
moho['Final_Dimesnion'].fillna(moho['Dimension_3'], inplace = True)


def convert_dimension(value):
    # Check if the value contains 'm' (for meters)
    if pd.notna(value):
        if 'm' in value:
            # Extract the number before 'm' and after 'm', if any
            parts = re.findall(r'\d+', value)
            if len(parts) == 2:
                # Combine the two parts into a centimeter value (e.g., 1m8 -> 180)
                return str(int(parts[0]) * 100 + int(parts[1]) * 10)
            elif len(parts) == 1:
                # Handle cases like '2m' -> 200
                return str(int(parts[0]) * 100)
        return value
    else:
        return value
# Apply the conversion function
moho['Final_Dimesnion'] = moho['Final_Dimesnion'].apply(convert_dimension)


check = moho.loc[moho['Final_Dimesnion'].isnull()]

moho_2 = moho.loc[moho['Final_Dimesnion'].isnull()]

def extract_dimension(r):
    # Extract the second part after the dot (e.g., 'B10', 'N14', etc.)
    temp1 = r['SKU']
    temp2 = r['Final_Dimesnion']
    try:
        if temp2 == None:
            second_part = temp1.split('.')[1]
            
            # Extract the numeric part from the second part
            dimension = re.findall(r'\d+', second_part)[0]
            
            return str(int(dimension)*10)
        else:
            return temp2
    except:
        pass

# Apply the function to create a 'Dimension' column
moho_2['Final_Dimesnion'] = moho_2.apply(extract_dimension, axis = 1)


moho = pd.concat([moho, moho_2])   #.dropna(subset = ['Final_Dimesnion']).drop_duplicates(subset = ['SKU'])



moho = moho.groupby('SKU').filter(lambda x: x['Final_Dimesnion'].notna().any() or x['Final_Dimesnion'].isna().all())

# Drop duplicates based on 'SKU' after filtering
moho = moho.dropna(subset=['Final_Dimesnion']).drop_duplicates(subset=['SKU'], keep='first')


moho['Final_Dimesnion'] = pd.to_numeric(moho['Final_Dimesnion'])


moho = moho.loc[moho['Final_Dimesnion'] <= 500]

moho['Types_2'] = moho['Types'] + ' ' + moho['Final_Dimesnion'].astype(str)

moho['Brand'] = 'Moho'

moho = moho[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU'])

# moho.to_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''',
#               index = False)


#%% JYSK # DONE

dfi = df.loc[(df['Brand'] == 'JYSK')]

check = dfi['Types'].value_counts()



dfi = dfi.loc[(dfi['Brand'] == 'JYSK') & ~(dfi['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                            'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]
dfi['Types_2'] = ''

lst_types_need_fill = list(set(dfi['Types'].tolist()))


def extract_output(dimension):
    match = re.search(r'[dØ](\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'd' or 'Ø'
    if match:
        return match.group(1)
    else:
        match_R = re.search(r'R(\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'R'
        return match_R.group(1) if match_R else None

# Apply the function to the 'Dimension' column to create a new 'output' column
dfi['Dimension_2'] = dfi['Dimension'].apply(extract_output)
check = dfi[['Types', 'Dimension', 'Dimension_2']].drop_duplicates()



dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Dimension_2'].astype(str)
dfi['Types_2'] = dfi['Types_2'].str.title()

dfi['Brand'] = 'JYSK'

dfi = dfi[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU'])

# types_2_all = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''')

# types_2_all = pd.concat([types_2_all, dfi])

# types_2_all.to_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''',
#                      index = False)

#%%
dfi = df.loc[(df['Brand'] == 'Modern House')]

check = dfi['Types'].value_counts()
#%% INDEX LIVING MALL # DONE

dfi = df.loc[df['Brand'] == 'Index Living Mall']

dfi = dfi[['SKU', 'Types', 'Dimension']]

dfi['Dimension_List'] = dfi['Dimension'].apply(lambda x: [int(num) for num in re.findall(r'\d+', x)])
dfi['Final_Dimension'] = dfi['Dimension_List'].apply(lambda x: x[0] if len(x) > 0 else '')

dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi['Brand'] = 'Index Living Mall'
dfi = dfi.drop_duplicates(subset = ['SKU'])


# types_2_all = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''')

# types_2_all = pd.concat([types_2_all, dfi])

# types_2_all.to_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''',
#                      index = False)


























#%% MAKE MY HOME # DONE
dfi = df.loc[(df['Brand'] == 'Make My Home') & ~(df['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                            'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]


def extract_output(dimension):
    match = re.search(r'[dØ](\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'd' or 'Ø'
    if match:
        return match.group(1)
    else:
        match_R = re.search(r'R(\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'R'
        return match_R.group(1) if match_R else None

# Apply the function to the 'Dimension' column to create a new 'output' column
dfi['Dimension_2'] = dfi['Dimension'].apply(extract_output)

dfi['Dimension_List'] = dfi['Dimension'].apply(lambda x: [int(num) for num in re.findall(r'\d+', x)])
dfi['Dimension_3'] = dfi['Dimension_List'].apply(lambda x: max(x) if len(x) > 0 else '')

dfi['Final_Dimension'] = np.nan
dfi['Final_Dimension'].fillna(dfi['Dimension_2'], inplace = True)
dfi['Final_Dimension'].fillna(dfi['Dimension_3'], inplace = True)

dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi['Brand'] = 'Make My Home'
dfi = dfi.drop_duplicates(subset = ['SKU'])

check = dfi[['Types', 'Dimension', 'Dimension_2', 'Dimension_3', 'Final_Dimension']].drop_duplicates()

dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)


dfi = dfi[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU'])


#%% MODERN HOUSE # DONE

dfi = df.loc[(df['Brand'] == 'Modern House') & ~(df['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                            'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]
dfi = dfi[['SKU', 'Types', 'Dimension']]

def extract_output(dimension):
    match = re.search(r'[dØ](\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'd' or 'Ø'
    if match:
        return match.group(1)
    else:
        match_R = re.search(r'R(\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'R'
        return match_R.group(1) if match_R else None

# Apply the function to the 'Dimension' column to create a new 'output' column
dfi['Dimension_2'] = dfi['Dimension'].apply(extract_output)

dfi['Dimension_List'] = dfi['Dimension'].apply(lambda x: [int(num) for num in re.findall(r'\d+', x)])
dfi['Dimension_3'] = dfi['Dimension_List'].apply(lambda x: max(x) if len(x) > 0 else '')

dfi['Final_Dimension'] = np.nan
dfi['Final_Dimension'].fillna(dfi['Dimension_2'], inplace = True)
dfi['Final_Dimension'].fillna(dfi['Dimension_3'], inplace = True)


dfi = dfi.loc[dfi['Final_Dimension'] != '40']


dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi['Brand'] = 'Modern House'
dfi = dfi.drop_duplicates(subset = ['SKU'])

check = dfi[['Types', 'Dimension', 'Dimension_2', 'Dimension_3', 'Final_Dimension']].drop_duplicates()


dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi = dfi[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU'])


dfi['Types_2'] = dfi['Types_2'].apply(lambda x: x if any(char.isdigit() for char in x) else np.nan)
dfi = dfi.loc[dfi['Types_2'].notnull()]




#%% COMEHOME # DONE

dfi = df.loc[(df['Brand'] == 'Comehome') & ~(df['Room'].isin(['Decoration/ Accessory', 'Kitchen']))
             & ~(df['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                            'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]
dfi = dfi[['SKU', 'Types', 'Dimension']]

def extract_output(dimension):
    match = re.search(r'[dØ](\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'd' or 'Ø'
    if match:
        return match.group(1)
    else:
        match_R = re.search(r'R(\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'R'
        return match_R.group(1) if match_R else None

# Apply the function to the 'Dimension' column to create a new 'output' column
dfi['Dimension_2'] = dfi['Dimension'].apply(extract_output)

dfi['Dimension_List'] = dfi['Dimension'].apply(lambda x: [int(num) for num in re.findall(r'\d+', x)])
dfi['Dimension_3'] = dfi['Dimension_List'].apply(lambda x: max(x) if len(x) > 0 else '')

dfi['Final_Dimension'] = np.nan
dfi['Final_Dimension'].fillna(dfi['Dimension_2'], inplace = True)
dfi['Final_Dimension'].fillna(dfi['Dimension_3'], inplace = True)


# dfi = dfi.loc[dfi['Final_Dimension'] != '40']
dfi = dfi.loc[dfi['Final_Dimension'] != '']


dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi['Brand'] = 'Comehome'
dfi = dfi.drop_duplicates(subset = ['SKU'])

check = dfi[['Types', 'Dimension', 'Dimension_2', 'Dimension_3', 'Final_Dimension']].drop_duplicates()


dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi = dfi[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU'])


#%% Beyours # DONE
dfi = df.loc[(df['Brand'] == 'Beyours') & ~(df['Room'].isin(['Decoration/ Accessory', 'Kitchen']))
             & ~(df['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                            'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]
dfi = dfi[['SKU', 'Types', 'Dimension']]

def extract_output(dimension):
    match = re.search(r'[dØ](\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'd' or 'Ø'
    if match:
        return match.group(1)
    else:
        match_R = re.search(r'R(\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'R'
        return match_R.group(1) if match_R else None

# Apply the function to the 'Dimension' column to create a new 'output' column
dfi['Dimension_2'] = dfi['Dimension'].apply(extract_output)

dfi['Dimension_List'] = dfi['Dimension'].apply(lambda x: [int(num) for num in re.findall(r'\d+', x)])
dfi['Dimension_3'] = dfi['Dimension_List'].apply(lambda x: max(x) if len(x) > 0 else '')

dfi['Final_Dimension'] = np.nan
dfi['Final_Dimension'].fillna(dfi['Dimension_2'], inplace = True)
dfi['Final_Dimension'].fillna(dfi['Dimension_3'], inplace = True)


# dfi = dfi.loc[dfi['Final_Dimension'] != '40']
dfi = dfi.loc[dfi['Final_Dimension'] != '']


dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi['Brand'] = 'Beyours'
dfi = dfi.drop_duplicates(subset = ['SKU'])

check = dfi[['Types', 'Dimension', 'Dimension_2', 'Dimension_3', 'Final_Dimension']].drop_duplicates()


dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi = dfi[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU'])


types_2_all = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''')
types_2_all = pd.concat([types_2_all, dfi])
types_2_all.to_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''',
                      index = False)



#%% Baya # DONE

dfi = df.loc[(df['Brand'] == 'Baya') & ~(df['Room'].isin(['Decoration/ Accessory', 'Kitchen']))
             & ~(df['Types'].isin(['Sofa Accessories', 'Set Bed Normal', 'Set Bed Storage',
                                                            'Set Bedside', 'Set Dining Bench', 'Set Dining Chair',
                                                            'Set Dining Table', 'Set Living Room', 'Set Sofa',
                                                            'Set Sofa Table', 'Set Stool', 'Set Tv Cabinet', 
                                                            'Full Combo Bed Room', 'PROJECT', 'SAMPLE', 'Combo Bed Room',
                                                            'Bed Accessories', 'Full House', 'Combo Living Room',
                                                            'Combo Dining Room', 'Full Combo Living Room', 'Set Sofa Corner',
                                                            'Set Sofa Table', 'Set Dining Chair', '']))]
dfi = dfi[['SKU', 'Types', 'Dimension']]


# dfi = dfi[['Types', 'Dimension']].groupby(['Types']).head(5).reset_index()




def extract_output(dimension):
    match = re.search(r'[lw](\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'd' or 'Ø'
    if match:
        return match.group(1)
    else:
        match_R = re.search(r'd(\d+)', dimension, re.IGNORECASE)  # Case-insensitive check for 'R'
        return match_R.group(1) if match_R else None

# Apply the function to the 'Dimension' column to create a new 'output' column
dfi['Dimension_2'] = dfi['Dimension'].apply(extract_output)

dfi['Dimension_List'] = dfi['Dimension'].apply(lambda x: [int(num) for num in re.findall(r'\d+', x)])
dfi['Dimension_3'] = dfi['Dimension_List'].apply(lambda x: max(x) if len(x) > 0 else '')

dfi['Final_Dimension'] = np.nan
dfi['Final_Dimension'].fillna(dfi['Dimension_2'], inplace = True)
dfi['Final_Dimension'].fillna(dfi['Dimension_3'], inplace = True)


# dfi = dfi.loc[dfi['Final_Dimension'] != '40']
dfi = dfi.loc[dfi['Final_Dimension'] != '']



dfi['Final_Dimension'] = dfi['Final_Dimension'].astype(str)

dfi['Len_Dimension'] = dfi['Final_Dimension'].apply(lambda x: len(x))


def fix_dimension(r):
    temp = r['Final_Dimension']
    if len(temp) == 4:
        # Combine the two parts into a centimeter value (e.g., 1m8 -> 180)
        return int(temp)/10
    else:
        return temp
dfi['Final_Dimension'] = dfi.apply(fix_dimension, axis = 1)

dfi['Final_Dimension']= dfi['Final_Dimension'].astype(int)

def fix_dimension2(r):
    temp = r['Final_Dimension']
    if temp >= 250:
        # Combine the two parts into a centimeter value (e.g., 1m8 -> 180)
        return int(temp)/10
    else:
        return temp
dfi['Final_Dimension'] = dfi.apply(fix_dimension2, axis = 1)

dfi['Final_Dimension'].fillna(dfi['Dimension_3'], inplace = True)


dfi['Brand'] = 'Baya'
dfi = dfi.drop_duplicates(subset = ['SKU'])

check = dfi[['Types', 'Dimension', 'Dimension_2', 'Dimension_3', 'Final_Dimension']].drop_duplicates()

dfi['Final_Dimension']= dfi['Final_Dimension'].astype(int)

dfi['Types_2'] = dfi['Types'] + ' ' + dfi['Final_Dimension'].astype(str)

dfi = dfi[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU'])


# types_2_all = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''')
# types_2_all = pd.concat([types_2_all, dfi])
# types_2_all.to_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''',
#                       index = False)


#%%
#%%
#%%
#%%% ADD TYPES 2 ON GGS

Brand = 'Moho'

df_all = pd.read_excel('''I:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Crawl Competitor Web/Data_Types_2/Data_Rival_Types_2_All.xlsx''')

                       
df_all = df_all[['Brand', 'SKU', 'Types_2']].drop_duplicates(subset = ['SKU', 'Brand'])

df_all['Types_2'] = df_all['Types_2'].apply(lambda x: x if any(char.isdigit() for char in x) else np.nan)
df_all = df_all.loc[df_all['Types_2'].notnull()]



df_all['SKU'] = df_all['SKU'].astype(str)

gc = gspread.service_account(filename='''I:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/0. ETL's CODING To Export Data Studio/project-aftersale-moho-cd1338f28ec9.json''')
sh = gc.open_by_key('1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4') # https://docs.google.com/spreadsheets/d/1OC-t2j3vmxXnkfRo9BeuzqO9kFhyl0EwtGhHOf0XNa4/edit#gid=1279399251

worksheet = sh.worksheet(Brand)
df_ggs = worksheet.get_all_values()
cols = df_ggs[0]
df_ggs = pd.DataFrame(df_ggs, columns = cols)

df_ggs = df_ggs.iloc[1:,]

# df_ggs.drop(['Types_2'], axis= 1, inplace = True)

df_ggs = df_ggs.merge(df_all, how = 'left', on = ['Brand', 'SKU'])

df_ggs.columns

# df_ggs = df_ggs[['Brand', 'Room', 'Types', 'Types_2','Collection', 'Date_get_new_product', 'SKU',
#         'Product_name', 'Price', 'Color', 'Description', 'Dimension',
#         'Material', 'link_p', 'Style', 'Made In', 'Xuất xứ', 'Mẫu mã thiết kế:',
#         'Thành phần:', 'Đặc điểm nổi bật:', 'Unit',
#         'October-2022', 'November-2022', 'December-2022', 'January-2023',
#         'February-2023', 'March-2023', 'April-2023', 'May-2023', 'June-2023',
#         'July-2023', 'August-2023', 'September-2023', 'October-2023',
#         'November-2023', 'December-2023', 'January-2024', 'February-2024',
#         'March-2024', 'April-2024', 'May-2024', 'June-2024', 'July-2024',
#         'August-2024', 'September-2024', 'October-2024', 'November-2024',
#         'December-2024', 'January-2025', 'February-2025', 'March-2025'
#         ]]


df_ggs = df_ggs[['Brand', 'Room', 'Types', 'Types_2','Collection', 'Date_get_new_product', 'SKU',
        'Product_name', 'Price', 'Color', 'Description', 'Dimension',
        'Material', 'link_p', 'Style', 'Made In', 'Xuất xứ', 'Mẫu mã thiết kế:',
        'Thành phần:', 'Đặc điểm nổi bật:', 'Unit', 'September-2022',
        'October-2022', 'November-2022', 'December-2022', 'January-2023',
        'February-2023', 'March-2023', 'April-2023', 'May-2023', 'June-2023',
        'July-2023', 'August-2023', 'September-2023', 'October-2023',
        'November-2023', 'December-2023', 'January-2024', 'February-2024',
        'March-2024', 'April-2024', 'May-2024', 'June-2024', 'July-2024',
        'August-2024', 'September-2024', 'October-2024', 'November-2024',
        'December-2024', 'January-2025', 'February-2025', 'March-2025'
        ]]



worksheet = sh.worksheet(Brand)
worksheet.clear()
set_with_dataframe(worksheet, df_ggs) 
























