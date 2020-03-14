# -*- coding: utf-8 -*-
"""
POS Data cleanup
Keith Hickman
khickman@djgusa.com
Dec 9 2019

This script ingests point of sale data from various customers in excel format, aggregates and outputs a unified flat file. 

To-do as of 12/13:
    [ ] create final columns across all datasets - rename columns in line with instructional document
    [ ] aggregate all dataframes together
    [ ] explore joins at the Infobright layer vs. in Pandas. 
    [x] rename "vendor_item" number to customer item in KMart dataset
    [ ] update master customer-item crossreference list. What if we do this in a flat file outside of IB, use python/manual to update
    , then push back into IB? What is the process for this? 
    [ ] 12.16.19 - Amazon vendor_item number missing - 300+ rows unable to merge (50%). 
"""
import pandas as pd
#import numpy as np
#import os
#from pandas.tseries.offsets import *

## KMART DATA: 
def kmart_import():
    kmart = pd.read_excel("P:\BI\POS Data from Paul\kmart-11.30.19.xlsx", 
                     sheet_name="LW SALES", 
                     skiprows=2, 
                     usecols="E:I, K:L, N:Q, R:Y, AA:AD, AF:AI", 
                     dtype={"Item ID":"str", "MODEL #":"str"})
    kmart['MODEL #'] = kmart['MODEL #'].str.strip()
    #Remove last row (totals)
    kmart = kmart[0:-1]
    
    print("Kmart - Total rows on read:", len(kmart))
    
    #splitting customer_item number from the "item Id" field:
    kmart['vendor_item'] = "00"+kmart['Item ID'].str.split(".").str[2].str.strip()
    
    print("Kmart - Removing zero sales rows")
    #filter TY Sales <= 0 and Item Ids = na (blank rows for subtotals)
    kmart = kmart[(kmart['Total Sales $ TY'] > 0) & (kmart['Item ID'].notna())]
    
    #Fill in account and time dimensions... time dimensions step is done on aggregated dataset
    kmart['account'] = "KMART"
    kmart['bricksNclicks'] = "Bricks"

    print("Total rows after cleaning:",len(kmart))
    print("Importing Item Master...")
    kmart_master = pd.read_excel("P:\BI\POS Data from Paul\dorel_master_vendor_item_crossref.xlsx", 
                             sheet_name="kmart", 
                             dtype={"vendor_item":"object"})

    # remove spaces:
    kmart_master['vendor_item'] = kmart_master['vendor_item'].str.strip()
        
    #First iteration of join on "vendor" item...need to rename this to customer item for clarity
    kmart_merged = pd.merge(kmart, kmart_master, how="left", on="vendor_item")
    
    if len(kmart) == len(kmart_merged):
        print("Kmart - No duplicate rows found")
    else:
        print("Kmart - Possible duplicates after initial merge")
    
    #Create first iteration of final merged dataset. 
    kmart_final = kmart_merged.loc[pd.notnull(kmart_merged['12 Digit UPC'])]
    kmart_errors = kmart_merged[pd.isna(kmart_merged['12 Digit UPC'])]
    
    merge1_success = len(kmart_final) + len(kmart_errors) == len(kmart_merged) 
    
    if merge1_success:
        print("First KMart join was successful")
    else:
        print("First Kmart join was not successful")
    
    kmart_errors.drop(columns=['vendor',
                               'Item',
                               'vendor_item', 
                               'unknown', 
                               '12 Digit UPC', 
                               'Dorel UPC', 
                               'Description', 
                               'Brand', 
                               'Category', 
                               'Short Brand', 
                               'Ultra', 
                               'Corp Brand', 
                               'Fashion'], inplace=True)
    kmart_errors = kmart_errors.rename(columns={'MODEL #':"Item"})   
    kmart_merge2 = pd.merge(kmart_errors, kmart_master, how="left", on="Item", suffixes=('_errors', '_master'))
    kmart_merge2_correct = kmart_merge2[pd.notna(kmart_merge2['12 Digit UPC'])]
    
    kmart_errors2 = kmart_merge2[pd.isna(kmart_merge2['Dorel UPC'])]
    #kmart_errors2['vendor_item'] = "00"+kmart_errors2['Item ID'].str.split(".").str[2].str.strip()
    #kmart_errors2.to_csv("kmart_errors_final.csv")
    
    kmart_merge2_correct.rename(columns={"Item":"MODEL #", "Item No. Short_master":"Item No. Short"}, inplace=True)
    kmart_final.drop(columns=['Item'], inplace=True)
    kmart_final=kmart_final.append(kmart_merge2_correct, sort=False)
    kmart_errors.to_csv("kmart_errors_final.csv")
    
    print("Kmart:",len(kmart_final),"rows successfully joined")
    if len(kmart_errors2) > 0:
        print(len(kmart_errors2),"rows unable to join. A flat file was placed in FILE_LOCATION\kmart_errors_final.csv. Please manually update the customer item master crossref file and rerun this script.")
    else:
        print("Kmart - All rows joined succesfully")
    #print(kmart_errors2[:,])
        
    return kmart


## Meijer Data Import 

def meijer_import():
    print("Importing Meijer Weekly POS Data")
    meijer = pd.read_excel("P:\BI\POS Data from Paul\Copy of MEIJER DJG 11.30.19.xlsx", 
                       encoding="latin", 
                       skiprows=4, 
                       sheet_name = "LAST WEEK SALES", 
                      dtype={"Product ID":"str", 
                            "UPC":"str"})
    
    meijer.rename(columns={"Unnamed: 1":"Vendor Category",
                           "Unnamed: 3":"Product Description", 
                           "Product ID":"vendor_item",
                          "Manufacturer Item Code":"Item"}, inplace=True)
    
    print("Meijer - Removing zero sales rows")
    meijer = meijer[(meijer['UPC'].notna()) & (meijer['Sales: $'] > 0)]
    
    print("Importing Meijer Item Master")
    meijer_master = pd.read_excel("P:\BI\POS Data from Paul\dorel_master_vendor_item_crossref.xlsx", 
                             sheet_name="meijer", 
                             dtype={"vendor_item":"object"})
    
    print("Meijer - Initial Join aka Vlookup for the Excel-ites")
    meijer_merged = pd.merge(meijer, meijer_master, how="left", on="vendor_item",suffixes=('_meijer', '_meijer_master') )
    meijer_merged.drop_duplicates(subset=['vendor_item',
                                          'Product Description',
                                          'Item_meijer',
                                          'Sales: $',
                                          'Sales: $ LY',
                                          'Sales: $ % Diff TYLY'], keep='first', inplace=True)
    
    meijer_final = meijer_merged.loc[pd.notnull(meijer_merged['12 Digit UPC'])]
    meijer_errors = meijer_merged[pd.isna(meijer_merged['12 Digit UPC'])]

    meijer_errors.drop(columns=['Item_meijer_master',
                            'vendor', 
                           'vendor_item', 
                           'unknown', 
                           '12 Digit UPC', 
                           'Dorel UPC', 
                           'Description', 
                           'Brand', 
                           'Category', 
                           'Short Brand', 
                           'Ultra', 
                           'Corp Brand', 
                           'Fashion'], inplace=True)

    meijer_errors.rename(columns={'Item_meijer':"Item"}, inplace=True)

    meijer_merge2 = pd.merge(meijer_errors, meijer_master, how="left", on="Item",suffixes=('_meijer', '_meijer_master'))
    meijer_merge2_correct = meijer_merge2[pd.notnull(meijer_merge2['12 Digit UPC'])]
    
    
    meijer_final = meijer_final.append(meijer_merge2_correct, sort=False)
    meijer_errors_final = meijer_merge2[meijer_merge2['12 Digit UPC'].isna()]
    
    meijer_final.to_csv("meijer_final_{}.csv".format(pd.datetime.now().date()))
    meijer_errors_final.to_csv("meijer_errors_final_{}.csv".format(pd.datetime.now().date()))
    
    print(len(meijer_final),"rows successfully joined")
    if len(meijer_errors_final) > 0:
        print(len(meijer_errors_final),"rows unable to join. A flat file was placed in FILE_LOCATION\meijer_errors_final.csv. Please manually update the customer item master crossref file and rerun this script.")
    else:
        print("All rows joined succesfully")
        
    return meijer_final


def bbb_import():
    #this will have to be changed to if: filename like... else error logic
    bbb = pd.read_excel("P:\BI\POS Data from Paul\Copy of VDS w-units registered for DOREL JUVENILE GROUP INC - 2019-12-04.xlsx",
                    sheet_name="Total",
                    skiprows=5,
#                    dtype={"UPC":"object"})
                    converters={"Sku":str, "UPC":str})
    
    bbb2 = pd.read_excel("P:\BI\POS Data from Paul\Copy of VDS w-units registered for QUINNY DIV OF DOREL.xlsx",
                    sheet_name="Total",
                    skiprows=5, 
                    converters={'UPC':str,'Sku':str})

    print("Merging BBB datasets")
    bbb_concat = bbb.append(bbb2)

    bbb_concat.rename(columns={"Department":"Dept Code",
                               "Unnamed: 4":"Department Desc",
                               "Unnamed: 7": "Product Desc",
                               "VPN": "Item",
                               "Sku Chain Retail":"Avg Retail", 
                               "Sales Units LW TY":"WTD POS Units", 
                               "UPC": "12 Digit UPC"}, inplace=True)
    print("Success")
    
    bbb_concat['WTD POS DOL'] = bbb_concat['WTD POS Units'] * bbb_concat['Avg Retail']
    bbb_concat['Item'] = bbb_concat['Item'].str.strip()
    #bbb_concat['12 Digit UPC'] = "0"+bbb_concat['12 Digit UPC'].str.strip()
    
    print("BBB - Removing zero sales rows")
    bbb_concat = bbb_concat[bbb_concat['WTD POS DOL'] > 0]
    bbb_concat['12 Digit UPC'] = bbb_concat['12 Digit UPC'].astype(str)
    bbb_concat['12 Digit UPC'] = bbb_concat['12 Digit UPC'].str.lstrip("0")
    
    print('Cleaning BBB UPC Codes that start with "19","44","52", "65", "72", "93"')
    bbb_concat['cleaned_upc'] = bbb_concat['12 Digit UPC'].apply(lambda x: "0"+x.strip() if x.startswith(("19","44","52", "65", "72", "93")) else x)

    print("Importing BBB Master Data")
    bbb_master = pd.read_excel("P:\BI\POS Data from Paul\dorel_master_vendor_item_crossref.xlsx"
                          , sheet_name="bbb"
                          , dtype={"vendor_item":"object", "12 Digit UPC":"object"})
    
    print("Joining BBB Weekly POS Data to BBB Master")
    bbb_merged = pd.merge(bbb_concat, bbb_master, how="left", on="12 Digit UPC")

    bbb_merged.drop_duplicates(subset=['Concept',
                                   'Format',
                                   'Sku',
                                   '12 Digit UPC',
                                   'Avg Retail',
                                   'WTD POS DOL'], keep='first', inplace=True)
    
    bbb_final = bbb_merged[pd.notnull(bbb_merged['Description'])]
    bbb_errors = bbb_merged[bbb_merged['Description'].isna()]
    
    bbb_errors.drop(columns=['Item_meijer_master',
                            'vendor', 
                            'vendor_item', 
                            'unknown', 
                            '12 Digit UPC', 
                            'Dorel UPC', 
                            'Description', 
                            'Brand', 
                            'Category', 
                            'Short Brand', 
                            'Ultra', 
                            'Corp Brand', 
                            'Fashion'], inplace=True)
    
    bbb_merge2 = pd.merge(bbb_errors, bbb_master, how="left", on="12 Digit UPC")
    bbb_errors_final = bbb_merge2[bbb_merge2['Description'].isna()]
    bbb_merge2 = bbb_merge2[pd.notnull(bbb_merge2['Description'])]    
    bbb_final = bbb_final.append(bbb_merge2)
    
    print(len(bbb_final),"rows successfully joined")
    if len(bbb_errors_final) > 0:
        print(len(bbb_errors_final),"rows unable to join. A flat file was placed in FILE_LOCATION\meijer_errors_final.csv. Please manually update the customer item master crossref file and rerun this script.")
    else:
        print("All rows joined succesfully")
        
    return bbb_final

#kmart = kmart_import()
#meijer = meijer_import()
bbb = bbb_import()



