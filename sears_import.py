# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


"""
#Need to read in different sheet for BBB - each workbook has three sheets...read in the first two. 

For testing:
    latest_file = (r'P:\BI\Keith Hickman\POS\Data\current_raw_customer\DJG SEARS POS 01-25-20.xlsx')
"""


def sears_import(latest_file, datapath):
    
    print("Importing Sears POS data file from: {}".format(latest_file))
    sears = pd.read_excel(latest_file, 
                          skiprows=1,
                          converters={"POSAmount":float,
                                      "Unnamed: 8":str}).rename(columns={"Unnamed: 9":"CustItemDesc",
                                                                         "Unnamed: 8":"CustItemNumber",
                                                                         "MODEL #":"CustVendStkNo",
                                                                         "Sears Total Sales $":"POSAmount",
                                                                         "Sears Total Sales Units":"POSQuantity"})
           
    sears = sears[['CustItemNumber','CustVendStkNo', 'CustItemDesc', 'POSAmount', 'POSQuantity']]
    sears = sears[sears['POSAmount'] != 0 ]
    sears = sears.dropna(subset=["POSAmount", "CustVendStkNo"], axis=0)
#   
    print("Importing sears master...this takes a minute.")
    sears_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str,
                                         'Sears':str,
                                         'ITEM NBR':str})

    sears_master['CustVendStkNo'] = sears_master['ITEM NBR']
    sears_master = sears_master.drop(columns=['Unnamed: 0'])
    
    #Merge datasets#
    sears_merged_1 = pd.merge(sears, sears_master, on="CustVendStkNo", how="left")
    
    #Pull errors into separate dataframe, rename customer item number column
    sears_errors_1 = sears_merged_1[sears_merged_1['Brand'].isna()].rename(columns=
                                {'CustItemNumber_x':'CustItemNumber'})
    sears_errors_1 = sears_errors_1[['CustItemNumber','CustVendStkNo','CustItemDesc','POSAmount','POSQuantity']]

    #Create "final" dataframe with non-exception records:
    sears_final = sears_merged_1[pd.notnull(sears_merged_1['Brand'])]
    
#   sears_merged['ItemNumber'] = sears_merged['CustVendStkNo']
  
    #Duplicate handling:
    if len(sears_merged_1) != len(sears):
        sears_merged_1 = sears_merged_1.drop_duplicates(subset=['CustVendStkNo',
                                              'CustItemDesc',
                                              'POSAmount'], 
                                      keep='first')
    
    if len(sears_errors_1) > 0:
        sears_master['CustItemNumber'] = sears_master['Sears']
        sears_merged_2 = pd.merge(sears_errors_1, sears_master, on = 'CustItemNumber', how='left')
        sears_merged_2 = sears_merged_2.rename(columns={"CustVendStkNo_y":"ItemNumber",
                                                        "CustVendStkNo_x":"CustVendStkNo"})
        #logic below: if this is len >0, print to file
        sears_errors_2 = sears_merged_2[sears_merged_2['Brand'].isna()]
        
        #sears_merged_2 = sears_merged_2[['ItemNumber','CustItemNumber','CustVendStkNo','CustItemDesc','POSAmount','POSQuantity']]
    
        sears_final = sears_final.append(sears_merged_2)
        #sears_final = pd.concat(frames, join='outer', copy=True)
        
    #suppress view-vs-copy warning for creating new columns
    pd.options.mode.chained_assignment = None
    #Create columns:
    sears_final['BricksClicks'] = "Clicks"
    sears_final['AccountMajor'] = "SEARS"
    sears_final['ItemID'] = ''
    sears_final['Account'] = 'Sears'
    sears_final['UPC'] = ''

   
    #List comprehension for Premium column:
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    sears_final['MainlinePremium'] = np.where(sears_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']
   
    sears_final = sears_final[cols_list] 
    if len(sears_errors_2) > 0:
        print(len(sears_errors_2),"records not found in the Item Master crossreference. Please check the exception files and run again.")
        #sears_exceptions.to_csv('P:\\BI\\POS Data from Paul\\exception_files\\sears_exceptions_{}.csv').format(end_of_week)
    else:
        print("no exceptions")
    
    #VALIDATION/TEST SCRIPT = COMMENT OUT IN PRODUCTION
    sears_validation = sears_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    
    return sears_final, sears_errors_2, sears_validation

#sears_final, sears_exceptions, sears_validation = sears_import(latest_file)
    