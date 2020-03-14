# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 15:20:27 2020

@author: khickman
"""

# -*- coding: utf-8 -*-
#WORKING

import pandas as pd
import numpy as np

"""To Do
This script takes a filepath, analyses the name for either TS or RL, then calls the appropriate function. 
latest_file = r'\\COL-foxfiles\Data\Departments\Sales\Sales Administration\pos-cleanup-utility\Data\current_raw_customer\wmcom W-E 02-29-2020 TS Data.xlsx'
"""


def wmcom_import(latest_file, datapath):
    
    """
    Importing RL walmart.com file
    """
    try: 
        print("Trying import of RL data: wmcom.com Weekly POS Data file: {}".format(latest_file))
        wmcom = pd.read_excel(latest_file, 
                        encoding="latin", 
                        skiprows=13, 
    #                   header=None, 
    #                   dtypes={"UPC":str}, 
                        converters={"Walmart Item Number":str,
                                    "UPC":str,
                                    "Vendor Stock ID":str})
        
    
        wmcom = wmcom.dropna(axis=0, how='all')
        
        wmcom = wmcom.rename(columns={"Vendor Stock ID":"CustVendStkNo",
                                      "Walmart Item Number":"CustItemNumber",
                                           "Total Net Retail Sales":"POSAmount",
                                           "Total Net Unit Sales":"POSQuantity",
                                           "Item Name":"CustItemDesc"})
        
        wmcom['Short UPC'] = wmcom['UPC'].str[2:]

        #wmcom['12 Digit UPC'] = "0"+wmcom['12 Digit UPC']
        #wmcom_ytd = wmcom[wmcom["Time"] == "YTD"]
        #wmcom = wmcom[wmcom["Time"] != "YTD"]
    
        wmcom = wmcom[wmcom['POSAmount'] != 0]
        
        print("Importing wmcom.com Master Data")
        wmcom_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                                 sheet_name="Dorel Master", 
                                 skiprows=6,
                                 converters={'12 Digit UPC':str,
                                             'Dorel UPC':str,
                                             'Short UPC':str,
                                             'Walmart':str}).rename(columns={"ITEM NBR":"ItemNumber"})
    
        wmcom_master['CustItemNumber'] = wmcom_master['Walmart']
        
        wmcom_merged = pd.merge(wmcom, wmcom_master, how="left", on="Short UPC")
        wmcom_merged = wmcom_merged.rename(columns={"CustItemNumber_x":"CustItemNumber"}).drop(columns=['CustItemNumber_y'])
          
        if wmcom['POSAmount'].sum() != wmcom_merged['POSAmount'].sum():
    #       print("DUPLICATES FOUND")
            
            wmcom_merged = wmcom_merged.drop_duplicates(subset=['CustItemNumber',
                                                                'Short UPC',
                                                                'CustItemDesc',
                                                                'POSAmount',
                                                                'POSQuantity'], 
                                              keep='first')
            
        wmcom_final = wmcom_merged[pd.notnull(wmcom_merged['Brand'])]
        wmcom_errors = wmcom_merged[wmcom_merged['Brand'].isna()]
    
    
        pd.options.mode.chained_assignment = None
        
        wmcom_final['BricksClicks'] = "Clicks"
        wmcom_final['AccountMajor'] = "WALMART.COM"
        wmcom_final['Account'] = "WALMART.COM"
        wmcom_final['ItemID'] = ''
        #wmcom_final['runcheck'] = "wmcom"
        
        
        premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
        wmcom_final['MainlinePremium'] = np.where(wmcom_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
        
        cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']
        
        wmcom_final = wmcom_final[cols_list]
        
        wmcom_validation = wmcom_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                      "POSQuantity":"sum",
                                                      "ItemNumber":"count"}).reset_index()
        
        print(len(wmcom_final),"wmcom records successfully processed")
        
 
        return wmcom_final, wmcom_errors, wmcom_validation
    
    #"""Importing TS walmart.com file"""
    except: 
        print("No RL data...trying Import of TS data: wmcom.com Weekly POS Data file: {}".format(latest_file))
        wmcom = pd.read_excel(latest_file, 
                        encoding="latin", 
    #                    skiprows=12, 
    #                    header=None, 
    #                    dtypes={"UPC":str}, 
                        converters={"Vendor Stk":str,
                                    "UPC":str,
                                    "Item ID":str})
        
    
        wmcom = wmcom.dropna(axis=0, how='all')
        wmcom = wmcom.rename(columns={"Vendor Stk":"CustVendStkNo",
                                      "Item ID":"CustItemNumber",
                                           "TY Sales":"POSAmount",
                                           "TY Qty":"POSQuantity",
                                           "Item":"CustItemDesc"})
        
        wmcom['Short UPC'] = wmcom['UPC'].str[2:]
        #wmcom['12 Digit UPC'] = "0"+wmcom['12 Digit UPC']
    
    
        
        #wmcom_ytd = wmcom[wmcom["Time"] == "YTD"]
    
        wmcom = wmcom[wmcom["Time"] != "YTD"]
        wmcom = wmcom[wmcom['POSAmount'] != 0]
        
        print("Importing wmcom.com Master Data")
        wmcom_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                                 sheet_name="Dorel Master", 
                                 skiprows=6,
                                 converters={'12 Digit UPC':str,
                                             'Dorel UPC':str,
                                             'Short UPC':str,
                                             'Walmart':str}).rename(columns={"ITEM NBR":"ItemNumber"})
    
        wmcom_master['CustItemNumber'] = wmcom_master['Walmart']
        
        wmcom_merged = pd.merge(wmcom, wmcom_master, how="left", on="Short UPC")
        wmcom_merged = wmcom_merged.rename(columns={"CustItemNumber_x":"CustItemNumber"}).drop(columns=['CustItemNumber_y'])
          
        if wmcom['POSAmount'].sum() != wmcom_merged['POSAmount'].sum():
    #        print("DUPLICATES FOUND")
            
            wmcom_merged = wmcom_merged.drop_duplicates(subset=['CustItemNumber',
                                                                'Short UPC',
                                                                'CustItemDesc',
                                                                'POSAmount',
                                                                'POSQuantity'], 
                                              keep='first')
            
        wmcom_final = wmcom_merged[pd.notnull(wmcom_merged['Brand'])]
        wmcom_errors = wmcom_merged[wmcom_merged['Brand'].isna()]
    
    
        pd.options.mode.chained_assignment = None
        
        wmcom_final['BricksClicks'] = "Clicks"
        wmcom_final['AccountMajor'] = "WALMART.COM"
        wmcom_final['Account'] = "WALMART.COM"
        wmcom_final['ItemID'] = ''

        
        premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
        wmcom_final['MainlinePremium'] = np.where(wmcom_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
        
        cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']
        
        wmcom_final = wmcom_final[cols_list]
        
        wmcom_validation = wmcom_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                      "POSQuantity":"sum",
                                                      "BricksClicks":"count"}).reset_index()
        
        print(len(wmcom_final),"wmcom records successfully processed")
        return wmcom_final, wmcom_errors, wmcom_validation
    
#wmcom_final, wmcom_errors, wmcom_validation = wmcom_import(latest_file, datapath)
