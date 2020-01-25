# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 09:37:39 2020

@author: khickman
"""
import pandas as pd
"""
reading in excel sheet names
"""
bbb1 = pd.read_excel(r"P:\BI\POS Data from Paul\bbb-mainline-2020-01-05.xlsx", None)
bbb1.keys()

for key in bbb1.keys():
    if 'Total' not in key:
        bbb1 = pd.read_excel(r"P:\BI\POS Data from Paul\bbb-mainline-2020-01-05.xlsx", 
                             sheet_name=key)



bbb2 = pd.read_excel("P:\BI\POS Data from Paul\Copy of VDS w-units registered for QUINNY DIV OF DOREL.xlsx",
                sheet_name="Total",
                skiprows=5, 
                converters={'UPC':str,'Sku':str})


"""
testing out using Paul's spreadsheet as Vendor Item Master
"""

bbb_master = pd.read_excel("P:\BI\POS Data from Paul\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Unnamed 16':str}).rename(columns=
                                        {"Unnamed 16":"Short UPC"})

bbb_master['VNDR STK NBR'] = bbb_master['12 Digit UPC']
bbb_master = amazon_master.drop(columns=['Unnamed: 0'])

"""
calculate quarter
"""
from fiscalyear import *

x = datetime.datetime.now()
#x = FiscalDate.today()
q = x.quarter
x.month



"""
Week Ending calculation:
"""
import pandas as pd
from pandas.tseries.offsets import Week
import datetime

'''
def previous_week_range(date):
    start_date = date + datetime.timedelta(-date.weekday(), weeks=-1)
    end_date = date + datetime.timedelta(-date.weekday() - 1)
    return start_date, end_date
'''

"""
Calculate last week/week ending offset
"""
rng = pd.date_range('2020-01-01', periods=21, freq='D')
df = pd.DataFrame({ 'DATE': rng, 'Val': np.random.randn(len(rng)) }) 
df['WEEK ENDING'] = df['DATE'].where( df['DATE'] == (( df['DATE'] + Week(weekday=5) ) - Week()), df['DATE'] + Week(weekday=5))


amazon = pd.read_excel("P:\\BI\\POS Data from Paul\\customer files 20XX\\amazon-01-05-2019.xlsx", skiprows=1).rename(columns=
                          {"ASIN":"VNDR STK NBR", 
                            "Product Title":"ITEM DESCRIPTION",
                            "Ordered Revenue":"WTD POS DOL",
                            "Ordered Units":"WTD POS Units"})
amazon = amazon[['VNDR STK NBR', 'ITEM DESCRIPTION', 'WTD POS DOL', 'WTD POS Units']]

lastweektoday = pd.to_datetime((datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%m/%d/%Y"))

amazon['lastweek'] = lastweektoday
#pd.to_datetime(datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%m-%d-%Y")
amazon['week_ending'] = amazon['lastweek'].where(amazon['lastweek'] == ((amazon['lastweek'] + Week(weekday=5)) - Week()), amazon['lastweek'] + Week(weekday=5))


x = datetime.datetime.now()
x = pd.to_datetime("05-01-2019".strftime("d%-m%-Y%"))
((x.month-1)//3)+1

pd.to_datetime('today')

today = datetime.now().strftime('%Y-%m-%d')


lastweektoday = (datetime.datetime.today().date() - datetime.timedelta(days=7))

week_ending = lastweektoday.where(lastweektoday == ((lastweektoday + Week(weekday=5) ) - Week()), lastweektoday + Week(weekday=5))


start_of_week = lastweektoday - datetime.timedelta(days=lastweektoday.weekday())  # Monday
end_of_week = start_of_week + datetime.timedelta(days=5)

#Use this in a dataframe
week_ending = lastweektoday.where(lastweektoday == ((lastweektoday + Week(weekday=5) ) - Week()), lastweektoday + Week(weekday=5))


lastweektoday = (datetime.datetime.today().date() - datetime.timedelta(days=7)) #one week ago today
start_of_week = lastweektoday - datetime.timedelta(days=lastweektoday.weekday()) #start of last week
end_of_week = start_of_week + datetime.timedelta(days=5) #week ending saturday
year = end_of_week.year
    
"""
Function dictionary example:
"""
import os
import pandas as pd

def move():
    print("move.")

def jump():
    print("jump.")

func_dict = {'move':move, 'jump':jump}
if __name__ == "__main__":
    input("Press enter to begin.")
    currentEnvironment = "room" #getNewEnvironment(environments)
    currentTimeOfDay = "1 A.M." #getTime(timeTicks, timeOfDay)
    print("You are standing in the {0}. It is {1}.".format(currentEnvironment, currentTimeOfDay))
    command = input("> ")
    func_dict[command]()

"""
#file iteration: 

"""
def read_data_excel():
    import re
    import os
    import glob
    

    list_of_files = glob.glob(r'P:\BI\POS Data from Paul\customer files 20XX\*') # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    print(latest_file)


    print("Reading Excel files Encounter Data")
    path = 'P:\BI\POS Data from Paul\customer files 20XX'

    customer_list = ["amazon"]
    all_files = []

    #this part works
    for subdir, dirs, files in os.walk(path):
        for file in files:
            for customer in customer_list:
                if customer in file.lower():
                    all_files.append(os.path.join(subdir,file))
                
                for file in all_files:
                    #list_of_files = glob.glob(r'P:\BI\POS Data from Paul\customer files 20XX\{}*'.format(customer)) # * means all if need specific format then *.csv
                    latest_file = max(all_files, key=os.path.getctime)

#Need to implement function dictionary next - take customer names as input parameters. 
    for customer in customer_list:
        cust_files = []
        for file in all_files:
            if customer in file.lower():
                print(customer)
                cust_files.append(file)
    
    for file in cust_file:


latest_file = []
    for customer in customer_list:
#        latest_file = []
        for file in all_files:
#       if customer in file:
        #list_of_files = glob.glob(r'P:\BI\POS Data from Paul\customer files 20XX\{}*'.format(customer)) # * means all if need specific format then *.csv
        #latest_file = max(list_of_files, key=os.path.getctime)
            if customer in file.lower():
                latest_file = max(all_files, key=os.path.getctime)
                print(latest_file)

        


#            all_files.append(os.path.join(subdir, file))
    
   
    all_files = all_files[0:5]

    amz_names = pd.DataFrame(columns=['filename'])





            file = file.split('\\')[4]
            file = re.findall(r"[\d]", file)
            
            amz_names = amz_names.append({'filename':file}, ignore_index=True)
            
        else:
            print(0)
    
    for row in amz_names['filename']:
        for item in row:
            item = row.join("")
            print(item)


    
    allencounters = []
    
    global notes
    allnotes = []
    
    for filename in all_files:
        df = pd.read_excel(filename,
                            sheet_name='Encounters',
                               header=0,
                               names=None,
                               index_col=None,
                               parse_cols=None,
                               usecols=None,
                               squeeze=False,
                               dtype=None,
                               engine=None,
                               converters=None,
                               true_values=None,
                               false_values=None,
                               skiprows=None,
                               nrows=None,
                               na_values=None,
                               keep_default_na=True,
                               verbose=False,
                               parse_dates=False,  ## change to true if issues w/date cols
                               date_parser=None,
                               thousands=None,
                               comment=None,
                               skipfooter=0,
                               convert_float=True,
                               mangle_dupe_cols=True)
        allencounters.append(df)
        
       










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



 meijer = pd.read_excel(r'P:\BI\POS Data from Paul\customer files 20XX\meijer-01-04-2020.xlsx', 
                       encoding="latin", 
                       skiprows=4, 
                       sheet_name = "LAST WEEK SALES", 
                       dtype={"Product ID":"str", 
                              "UPC":"str"}).rename(columns={"Unnamed: 1":"Vendor Category",
                                             "Unnamed: 3":"Product Description", 
                                             "Product ID":'VNDR STK NBR',
                                             "Manufacturer Item Code":"ITEM NBR", 
                                             "Sales: $":"WTD POS DOL", 
                                             "Sales: Qty":"WTD POS Units"})
    
    
     meijer_master = pd.read_excel("P:\BI\POS Data from Paul\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})














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
