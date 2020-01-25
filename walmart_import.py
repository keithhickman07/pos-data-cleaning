# -*- coding: utf-8 -*-


'sep="\t"'

import pandas as pd
import numpy as np

"""
for testing: 
    
latest_file = (r'P:\BI\POS Data from Paul\customer files 20XX\walmart-brick-01-04-2020.xlsx')
    
"""

def walmart_import(latest_file):
    print("Importing Walmart Weekly POS Data file: {}".format(latest_file))
    #walmart = pd.read_excel(r'P:\BI\POS Data From Paul\customer files 20XX\walmart-brick-01-04-2020.xlsx',
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
    
    
    walmart = walmart[walmart['Dept Category Description'] != "DOTCOM"]
    walmart = walmart[walmart['POSAmount'] != 0]
    
    print("Importing Walmart Master Data")
    walmart_master = pd.read_excel(r"Y:\Sales Administration\2020 POS\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str}).rename(columns={"ITEM NBR":"ItemNumber"})

    walmart_master['CustItemNumber'] = walmart_master['Walmart']
    walmart_master['CustVendStkNo'] = walmart_master['ItemNumber']
    
    walmart_merged = pd.merge(walmart, walmart_master, how="left", on="CustVendStkNo")
    
    walmart_final = walmart_merged[pd.notnull(walmart_merged['Brand'])]
    walmart_final = walmart_final.rename(columns={"CustItemNumber_x":"CustItemNumber"}).drop(columns=['CustItemNumber_y'])
    
    walmart_errors = walmart_merged[walmart_merged['Brand'].isna()]


    pd.options.mode.chained_assignment = None
    
    walmart_final['BricksClicks'] = "Bricks"
    walmart_final['AccountMajor'] = "WALMART"
    walmart_final['Account'] = 'WALMART'
    walmart_final['ItemID'] = ''
    walmart_final['runcheck'] = "walmart"

    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    walmart_final['MainlinePremium'] = np.where(walmart_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
           'MainlinePremium','POSAmount','POSQuantity', 'runcheck']

    
    walmart_final = walmart_final[cols_list]
    
    walmart_validation = walmart_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    if len(walmart_errors) > 0:
        print(len(walmart_errors),"records not found in the Item Master crossreference. Please check the exception files and run again.")
        
    else:
        print("no exceptions")

    return walmart_final, walmart_errors, walmart_validation
