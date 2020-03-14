# -*- coding: utf-8 -*-


'sep="\t"'

import pandas as pd
import numpy as np

"""
for testing: 
    
latest_file = (r'P:\BI\Keith Hickman\POS\Data\current_raw_customer\Walmart brick W-E 02-01-2020.xlsx')
    
"""

def walmart_import(latest_file, datapath):
    print("Importing Walmart Weekly POS Data file: {}".format(latest_file))
    walmart = pd.read_excel(latest_file, 
                    encoding="latin", 
#                    skiprows=12, 
                    header=None)

    walmart = walmart.dropna(axis=0, how='any')
    walmart.columns = walmart.iloc[0]
    
    walmart = walmart[1:]
    
    for column in walmart.columns:
        if "POS Sales" in column:
            sales_col = column
        elif "POS Qty" in column:
            qty_col = column
            
    walmart = walmart.rename(columns={"Vendor Stk Nbr":"CustVendStkNo",
                                       sales_col:"POSAmount",
                                       qty_col:"POSQuantity",
                                        "Prime Item Nbr":"CustItemNumber",
                                        "Item Desc 1":"CustItemDesc"})
    
    walmart['CustItemNumber'] = walmart['CustItemNumber'].astype("str")
    walmart = walmart[walmart['Dept Category Description'] != "DOTCOM"]
    walmart = walmart[walmart['POSAmount'] != 0]
    
    print("Importing Walmart Master Data")
    walmart_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str, 
                                         'Walmart':str}).rename(columns={"ITEM NBR":"ItemNumber"})

    walmart_master['CustItemNumber'] = walmart_master['Walmart']
    walmart_master['CustVendStkNo'] = walmart_master['ItemNumber']
    
    walmart_merged = pd.merge(walmart, walmart_master, how="left", on="CustItemNumber")
    
    walmart_final = walmart_merged[pd.notnull(walmart_merged['Brand'])]
    walmart_final = walmart_final.rename(columns={"CustVendStkNo_x":"CustVendStkNo"}).drop(columns=['CustVendStkNo_y'])
    
    walmart_errors = walmart_merged[walmart_merged['Brand'].isna()]

    walmart_final = walmart_final.drop_duplicates(subset=['CustItemDesc',
                                                          'POSAmount', 
                                                          'POSQuantity', 
                                                          'CustItemNumber',
                                                          'CustVendStkNo'], keep='first')
    
    pd.options.mode.chained_assignment = None
    
    walmart_final['BricksClicks'] = "Bricks"
    walmart_final['AccountMajor'] = "WALMART"
    walmart_final['Account'] = 'WALMART'
    walmart_final['ItemID'] = ''
    walmart_final['UPC'] = ''

    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    walmart_final['MainlinePremium'] = np.where(walmart_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']

    
    walmart_final = walmart_final[cols_list]
    
    walmart_validation = walmart_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    if len(walmart_errors) > 0:
        print(len(walmart_errors),"records not found in the Item Master crossreference. Please check the exception files and run again.")
        
    else:
        print("no exceptions")

    return walmart_final, walmart_errors, walmart_validation
