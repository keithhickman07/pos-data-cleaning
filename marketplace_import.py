# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

"""

For testing:

"""
#latest_file = (r'P:\BI\POS Data from Paul\customer files 20XX\marketplace-01-04-2020.xlsx')

def market_import(latest_file):
    print("Importing Marketplace POS Data file: {}".format(latest_file))
    market = pd.read_excel(latest_file, 
                        encoding="latin", 
                        #skiprows=2, 
#                       sheet_name="SAFETY 1st FISCAL WEEK 44", 
                        dtypes={"Item_ID":"str"}).rename(columns=
                               {"Item_ID":"CustItemNumber",
                                "ITEM_DESC":"CustItemDesc",
                                "ITEM_SRCCD":"CustVendStkNo", 
                                "CustomerName":"Account", 
                                "SHEXTAMOUNT":"POSAmount",
                                "SHQTYSHIPPED":"POSQuantity"})
    
    market = market[['CustItemNumber',
                     'CustItemDesc',
                     'CustVendStkNo',
                     'Account',
                     'POSAmount',
                     'POSQuantity']]
    
    market = market[market['POSAmount'] != 0]
    
    print("Importing Marketplace Master Data")
    
    marketplace_master = pd.read_excel(r"P:\BI\POS Data from Paul\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})

    #marketplace_master['VNDR STK NBR'] = marketplace_master['ITEM NBR']
    marketplace_master = marketplace_master.drop(columns=['Unnamed: 0'])
    marketplace_master['ItemNumber'] = marketplace_master["ITEM NBR"]
    marketplace_master['CustVendStkNo'] = marketplace_master["ITEM NBR"]
    
    market_merged = pd.merge(market, marketplace_master, on="CustVendStkNo", how="left")

    market_merged = market_merged.drop_duplicates(subset=['CustItemNumber',
                                                          'CustItemDesc',
                                                          'CustVendStkNo',
                                                          'Account',
                                                          'POSAmount',
                                                          'POSQuantity'], keep='first')

    market_errors = market_merged[market_merged['Short Brand'].isna()]
    market_final = market_merged[pd.notnull(market_merged['Short Brand'])]
    
    if len(market_errors) == 0:
        print("All rows processed successfully")
    else:
        print(len(market_errors),"join errors")
        
    market_final['BricksClicks'] = "Clicks"
    market_final['AccountMajor'] = "marketplace"
    market_final['ItemID'] = ""
    market_final['runcheck'] = 'marketplace'

    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    market_final['MainlinePremium'] = np.where(market_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'BricksClicks','Account', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
           'MainlinePremium','POSAmount','POSQuantity', 'runcheck']
    
 
    
    market_validation = market_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
        
    market_final = market_final[cols_list]

    return market_final, market_errors, market_validation


#market_final, market_errors, market_validation = market_import(latest_file)
