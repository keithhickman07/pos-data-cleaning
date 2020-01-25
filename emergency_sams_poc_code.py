# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 09:37:39 2020

@author: khickman
"""
import pandas as pd

sams = pd.read_excel("P:\\BI\\POS Data from Paul\\customer files 20xx\\Sam's clubs W-E 12-28-2019.xlsx", 
                    encoding="latin", 
                    skiprows=12, 
#                       sheet_name="SAFETY 1st FISCAL WEEK 44", 
                    dtypes={"Item Nbr":"str"})


for row in sams:
    print(row)
    
cols = sams.columns[0:6]
sams = sams[cols]
sams = sams[pd.notnull(sams['Item Nbr'])]
sams = sams[sams['Item Nbr']!= "Inventory"]


sams_dtypes = sams[['Data Type',201901,201902]]
sams_dtypes_t = sams_dtypes.T

sams_indexes = sams[['Item Nbr', 'Item Desc 1', 'Vendor Stock Nbr']]
sams_indexes_group = sams_indexes.groupby(['Item Nbr', 'Item Desc 1', 'Vendor Stock Nbr'])

data = {'key':['foo', 'bar', 'baz','foo','bar'], 'val':['oranges', 'bananas', 'apples', 'grapes','kiwis']} 

df = pd.DataFrame(data)

df

#method1: 
df.pivot_table(values='val', index=df.index, columns='key', aggfunc='first')

df.set_index([df.index, 'key'])['val'].unstack()

item_nbr = 536140
dtypes = 'Total Units Sold'



test_1 = sams[201902][(sams['Item Nbr']==item_nbr) & (sams['Data Type']==dtypes)]



items_list = []
for value in sams['Item Nbr']:
    if value in items_list:
        pass 
    else:
        items_list.append(value)

dtypes_list = ['Total Units Sold', 'Total Sell Dollars', 'Avg On-Hand Qty', 'Avg On-Order Qty']
for value in sams['Data Type']:
    if value in dtypes_list:
        pass
    else:
        dtypes_list.append(value)
        
for item in items_list:
    for dtype in dtypes_list:
        print(item, dtype)


















# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 15:35:00 2020

@author: khickman
"""

headlines = ["India", "Asia", "Singapore", "Malaysia", "Nepal", "China"]
dict_list = []
with open("my_file.csv") as csv_file:
    for line in csv_file:
        dict_list.append(dict(zip(hl, [item.strip() for item in line.split(",")])))  
print(*dict_list, sep="\n")  # print one dictionary per line
