# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np



def nordstrom_import(latest_file, datapath):

    print("Importing Nordstrom POS Data file: {}".format(latest_file))
    
    nordstrom = pd.read_excel(latest_file, 
                        encoding="latin", 
                        skiprows=1, 
#                       sheet_name="SAFETY 1st FISCAL WEEK 44", 
                        dtypes={"Store ID":"str"}).rename(columns=
                               {"Sales $":"POSAmount",
                                "Sales U":"POSQuantity"})

    nordstrom = nordstrom[nordstrom['POSAmount'] != 0]
    nordstrom = nordstrom[nordstrom['Bus Unit'].isin(['FULL LINE', 'N.COM','RACK'])]
    
    # Lambda function to check if a given vaue is bricks or clicks.
    nordstrom['BricksClicks'] = nordstrom['Bus Unit'].apply(lambda x:"Clicks" if x == "N.COM" else "Bricks")
    nordstrom['CustVendStkNo'] = nordstrom['VPN'].str.split(",").str[0].str.strip()
    nordstrom['CustItemDesc'] = nordstrom['VPN'].str.split(",").str[1].str.strip()
    
    
    print("Importing Nordstrom Master Data")
    nordstrom_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str, 
                                         'Nordstrom':str}).rename(columns={"ITEM NBR":"ItemNumber"})
    
    
    nordstrom_master['CustVendStkNo'] = nordstrom_master['ItemNumber']
    
    nordstrom_merged = pd.merge(nordstrom, nordstrom_master, on="CustVendStkNo", how="left")
    
    nordstrom_merged = nordstrom_merged.drop_duplicates(subset=['Group',
                                                                'Store',
                                                                'ItemNumber', 
                                                                'POSAmount'], keep='first')
    
    nordstrom_errors = nordstrom_merged[nordstrom_merged['Short Brand'].isna()]
    nordstrom_final = nordstrom_merged[pd.notnull(nordstrom_merged['Short Brand'])]
    
    pd.options.mode.chained_assignment = None
    
    nordstrom_final['AccountMajor'] = "NORDSTROM"
    nordstrom_final['Account']= "NORDSTROM"
    nordstrom_final['CustItemNumber'] = ""
    nordstrom_final['ItemID'] = ''
    nordstrom_final['UPC'] = ""
    
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    nordstrom_final['MainlinePremium'] = np.where(nordstrom_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
       
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']

    
    nordstrom_final = nordstrom_final[cols_list]
    
    nordstrom_validation = nordstrom_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    
    if len(nordstrom_errors) == 0:
        print(len(nordstrom_final),"Nordstrom records successfully processed.")
    else:
        print(len(nordstrom_errors),"Nordstrom records not found in Item master")
    
    return nordstrom_final, nordstrom_errors, nordstrom_validation

#nordstrom_final,nordstrom_errors, nordstrom_validation = nordstrom_import(latest_file)
