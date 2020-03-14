# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


"""
For testing:
latest_file = (r'P:\BI\Keith Hickman\POS\Data\current_raw_customer\Amazon W-E 02-01-2020.xlsx')

Function takes in file location for customer file and a datapath 
"""


def amazon_import(latest_file, datapath):
    
    print("Importing Amazon POS data file: {}".format(latest_file))
    amazon = pd.read_excel(latest_file, skiprows=1).rename(columns=
                          {"ASIN":"CustItemNumber", 
                            "Product Title":"CustItemDesc",
                            "Ordered Revenue":"POSAmount",
                            "Ordered Units":"POSQuantity"})
    
       
    amazon = amazon[['CustItemNumber', 'CustItemDesc', 'POSAmount', 'POSQuantity']]
    
    amazon = amazon[amazon['POSAmount'] != 0]
    
    print("Importing Amazon master...this takes a minute.")
    amazon_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})

    amazon_master['CustItemNumber'] = amazon_master['Amazon ID']
    amazon_master['ItemNumber'] = amazon_master['ITEM NBR']
    
    
    #Merge datasets#
    amazon_merged = pd.merge(amazon, amazon_master, on="CustItemNumber", how="left")
    merged_cols = ['CustItemNumber','CustItemDesc','ItemNumber','POSAmount','POSQuantity', 'Short Brand']
    amazon_merged = amazon_merged[merged_cols]
    
    
    #Duplicate handling:
    if len(amazon_merged) != len(amazon):
        amazon_merged = amazon_merged.drop_duplicates(subset=['CustItemNumber',
                                              'CustItemDesc',
                                              'POSAmount',
                                              'POSQuantity'], 
                                      keep='first')

    amazon_exceptions = amazon_merged[amazon_merged['ItemNumber'].isna()]
    

    amazon_final = amazon_merged[pd.notnull(amazon_merged['ItemNumber'])]
    
   
    #suppress view-vs-copy warning for creating new columns
    pd.options.mode.chained_assignment = None
    #Create columns:

    amazon_final['ItemID'] = ''
    amazon_final['CustVendStkNo'] = ''
    amazon_final['BricksClicks'] = "Clicks"
    amazon_final['AccountMajor'] = "AMAZON"
    amazon_final['Account'] = 'AMAZON'
#    amazon_final['Store Cnt'] = ""
#    amazon_final['Avg Retail'] = ""
#    amazon_final['In Stock%'] = ""
#    amazon_final['On Hand'] = ""
#    amazon_final['On Order'] = ""
    amazon_final['UPC'] = ""

   
    #List comprehension for Premium column:
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    amazon_final['MainlinePremium'] = np.where(amazon_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
   
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']
    
    amazon_final = amazon_final[cols_list]    
    if len(amazon_exceptions) > 0:
        print(len(amazon_exceptions),"records not found in the Item Master crossreference. Please check the exception files and run again.")
        #amazon_exceptions.to_csv('P:\\BI\\POS Data from Paul\\exception_files\\amazon_exceptions_{}.csv').format(end_of_week)
    else:
        print("no exceptions")
    
    #VALIDATION/TEST SCRIPT = COMMENT OUT IN PRODUCTION
    amazon_validation = amazon_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    
    return amazon_final, amazon_exceptions, amazon_validation

#amazon_final, amazon_exceptions, amazon_validation = amazon_import(latest_file)
    