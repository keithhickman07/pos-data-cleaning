# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


"""
#Need to read in different sheet for BBB - each workbook has three sheets...read in the first two. 

For testing:
    latest_file = (r'P:\BI\POS Data from Paul\customer files 20XX\sears-01-04-2020.xlsx')
"""


def sears_import(latest_file):
    
    print("Importing Sears POS data file from: {}".format(latest_file))
    sears = pd.read_excel(latest_file, 
                          skiprows=1,
                          converters={"POSAmount":float}).rename(columns={"Unnamed: 9":"CustItemDesc",
                                                                         "Unnamed: 8":"CustItemNumber",
                                                                         "MODEL #":"CustVendStkNo",
                                                                         "Sears Total Sales $":"POSAmount",
                                                                         "Sears Total Sales Units":"POSQuantity"})
           
    sears = sears[['CustItemNumber','CustVendStkNo', 'CustItemDesc', 'POSAmount', 'POSQuantity']]
    sears = sears[sears['POSAmount'] != 0 ]
    sears = sears.dropna(subset=["POSAmount", "CustVendStkNo"], axis=0)
#   
    print("Importing sears master...this takes a minute.")
    sears_master = pd.read_excel(r"P:\BI\POS Data from Paul\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})

    sears_master['CustVendStkNo'] = sears_master['ITEM NBR']
    sears_master = sears_master.drop(columns=['Unnamed: 0'])
    
    #Merge datasets#
    sears_merged = pd.merge(sears, sears_master, on="CustVendStkNo", how="left")
    sears_merged['ItemNumber'] = sears_merged['CustVendStkNo']
  
    #Duplicate handling:
    if len(sears_merged) != len(sears):
        sears_merged = sears_merged.drop_duplicates(subset=['CustVendStkNo',
                                              'CustItemDesc',
                                              'POSAmount'], 
                                      keep='first')

    sears_errors = sears_merged[sears_merged['Brand'].isna()]
    sears_final = sears_merged[pd.notnull(sears_merged['Brand'])]
    
   
    #suppress view-vs-copy warning for creating new columns
    pd.options.mode.chained_assignment = None
    #Create columns:

   
    sears_final['BricksClicks'] = "Clicks"
    sears_final['AccountMajor'] = "sears"
    sears_final['ItemID'] = ''
    sears_final['Account'] = 'sears'
    sears_final['runcheck'] = "sears"

   
    #List comprehension for Premium column:
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    sears_final['MainlinePremium'] = np.where(sears_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
           'MainlinePremium','POSAmount','POSQuantity', 'runcheck']


    sears_final = sears_final[cols_list]    
    if len(sears_errors) > 0:
        print(len(sears_errors),"records not found in the Item Master crossreference. Please check the exception files and run again.")
        #sears_exceptions.to_csv('P:\\BI\\POS Data from Paul\\exception_files\\sears_exceptions_{}.csv').format(end_of_week)
    else:
        print("no exceptions")
    
    #VALIDATION/TEST SCRIPT = COMMENT OUT IN PRODUCTION
    sears_validation = sears_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    
    return sears_final, sears_errors, sears_validation

#sears_final, sears_exceptions, sears_validation = sears_import(latest_file)
    