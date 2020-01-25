# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

"""
latest_file = (r'P:\BI\POS Data from Paul\customer files 20XX\kmart-01-04-2020.xlsx')
"""

def kmart_import(latest_file):
    
    #Read in excel file: use only certain columns. 
    #Check dtype of Item ID and Model number. 
    print("Importing KMart POS Data file: {}".format(latest_file))
    kmart = pd.read_excel(latest_file, 
                     sheet_name="LW SALES", 
                     skiprows=2, 
                     usecols="E:I, K:L, N:Q, R:Y, AA:AD, AF:AI", 
                     dtype={"Item ID":"str", "MODEL #":"str"}).rename(columns=
                            {"Total Sales $ TY":"POSAmount",
                                  "Total Sales Units TY":"POSQuantity",
                                  "Ending Store On-Order Units TY":"On Order"}).rename(columns={"MODEL #":"CustVendStkNo"})
    
    

    kmart = kmart[pd.notnull(kmart['Item ID'])]
    kmart['CustVendStkNo'] = kmart['CustVendStkNo'].str.strip()
  
    #splitting customer_item number from the "item Id" field:
    kmart['CustItemNumber'] = "00"+kmart['Item ID'].str.split(".").str[2].str.strip()
    kmart['CustItemDesc'] = kmart['Item ID'].str.split(".").str[4]
    
    #filter TY Sales <= 0 and Item Ids = na (blank rows for subtotals)
    kmart = kmart[(kmart['POSAmount'] != 0)] #& (kmart['Item ID'].notna())]
    
    #subset columns - CustVendStkNo, Description POSAmount/Units, On Hand, On Order
    kmart = kmart[['CustVendStkNo',
                   'CustItemNumber',
                   'CustItemDesc',
                   'POSAmount',
                   'POSQuantity']]
    
    #Fill in account and time dimensions... time dimensions step is done on aggregated dataset
   
    print("Importing KMart Item Master...thank you for your patience")
    kmart_master = pd.read_excel(r"P:\BI\POS Data from Paul\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str}).rename(columns={"ITEM NBR":"ItemNumber"})

    kmart_master['CustItemNumber'] = kmart_master['K-mart']
    kmart_master = kmart_master.drop(columns=['Unnamed: 0'])

    # strip spaces from vendor_item:
#    kmart_master['CustVendStkNo'] = kmart_master['CustVendStkNo'].str.
    
    #rename item nbr to model no for second join
    
                                 
    #First iteration of join on "vendor" item...
    kmart_merged = pd.merge(kmart, kmart_master, how="left", on="CustItemNumber")
    
    kmart_merged = kmart_merged.drop_duplicates(subset=['CustItemNumber',
                                                        'CustVendStkNo',
                                                        'ItemNumber',
                                                        'POSAmount',  
                                                        'POSQuantity'], 
                                  keep='first')
    
    
    #Create first iteration of "final" dataset. 
    kmart_final = kmart_merged.loc[pd.notnull(kmart_merged['12 Digit UPC'])]
    
    #Create first iteration of error dataset
    kmart_errors = kmart_merged[pd.isna(kmart_merged['12 Digit UPC'])]
        
  
    #Drop columns to avoid column name dupes from kmart_master dataset:
    
    #Same steps again - join on ITEM NBR/Model # in the kmart data, rename master column item nbr to model #
    
    #suppress view vs. copy warning
    pd.options.mode.chained_assignment = None
    #Set all column names/orders to be the same before join
   
    kmart_final['AccountMajor'] = "KMART"
    kmart_final['Account'] = 'KMART'
    kmart_final['BricksClicks'] = "Bricks"
    kmart_final['runcheck'] = 'kmart'
    kmart_final['ItemID'] = ""
    
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    kmart_final['MainlinePremium'] = np.where(kmart_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
           'MainlinePremium','POSAmount','POSQuantity', 'runcheck']
    
    kmart_final = kmart_final[cols_list]
    
    kmart_validation = kmart_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    
    print("Kmart:",len(kmart_final),"Kmart rows successfully joined")
    if len(kmart_errors) > 0:
        print(len(kmart_errors),"Kmart records not found in master part file.")
        #kmart_errors.to_csv('P:\\BI\\POS Data from Paul\\exception_files\\kmart_exceptions_{}.csv'.format(end_of_week))
        print(len(kmart_final), "Kmart records processed successfully")
    else:
        print("Kmart - All rows joined succesfully")
       
    return kmart_final, kmart_errors, kmart_validation

#kmart_final, kmart_errors, kmart_validation = kmart_import(latest_file)