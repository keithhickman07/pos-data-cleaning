# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np

"""
latest_file = r'\\COL-foxfiles\Data\Departments\Sales\Sales Administration\pos-cleanup-utility\Data\current_raw_customer\MEIJER DJG 02-29-20.xlsx'
"""

def meijer_import(latest_file, datapath):
    
    """
    sheets = pd.ExcelFile(latest_file).sheet_names
    meijer_sheet = []
    for x in sheets:
        if x.startswith("YTD") or x == "SUMMARY":
            pass
        else:
            meijer_sheet.append(x)
    """
    
    print("Importing Meijer POS Data file: {}".format(latest_file))
    meijer = pd.read_excel(latest_file, 
                       #encoding="utf-8",
                       error_bad_lines=False,
                       skiprows=4, 
                       sheet_name = 1, 
                       dtype={"Product ID":"str", 
                              "UPC":"str"}).rename(columns={"Unnamed: 3":"CustItemDesc",
                                             "Product ID":"CustItemNumber", 
                                             "Manufacturer Item Code":'CustVendStkNo',
                                             "Sales: $":"POSAmount", 
                                             "Sales: Qty":"POSQuantity"})
    
    meijer = meijer[(meijer['UPC'].notna()) & (meijer['POSAmount'] != 0) & meijer['POSAmount'].notna()]
    meijer['POSQuantity'] = round(meijer['POSQuantity'].astype("int"),0)
    meijer['POSAmount'] = round(meijer['POSAmount'],2)
        
    print("Importing Meijer Item Master")
    meijer_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})

    meijer_master['CustItemNumber'] = meijer_master['Meijer']
    meijer_master = meijer_master.drop(columns=['Unnamed: 0'])
    
    meijer = meijer[['CustItemDesc','POSAmount', 'POSQuantity', 'CustItemNumber','CustVendStkNo', 'UPC']]
    
    meijer_merged = pd.merge(meijer, meijer_master, how="left", on='CustItemNumber',suffixes=('_meijer', '_meijer_master'))
    
    meijer_merged = meijer_merged.drop_duplicates(subset=['CustItemDesc',
                                                          'POSAmount', 
                                                          'POSQuantity', 
                                                          'CustItemNumber',
                                                          'CustVendStkNo',
                                                          'UPC'], keep='first')
    
    meijer_final = meijer_merged.loc[pd.notnull(meijer_merged['12 Digit UPC'])]
    
    meijer_final = meijer_final.rename(columns={"ITEM NBR":"ItemNumber"})
    meijer_errors = meijer_merged[pd.isna(meijer_merged['12 Digit UPC'])].rename(columns={'ITEM NBR_meijer':'ITEM NBR'})

    meijer_errors = meijer_errors[meijer.columns]

      
    meijer_final['BricksClicks'] = "Bricks"
    meijer_final['AccountMajor'] = "MEIJER"
    meijer_final['Account'] = "MEIJER"
    #meijer_final['runcheck'] = 'meijer'
    meijer_final['ItemID'] = ""
   
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    meijer_final['MainlinePremium'] = np.where(meijer_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']

    
    meijer_final = meijer_final[cols_list]
    
    if len(meijer_errors) > 0:
        print(len(meijer_errors),"records not found in master file.")
    else:
        print(len(meijer_final),"rows joined succesfully")
        
    meijer_validation = meijer_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    
        
    return meijer_final, meijer_errors, meijer_validation

#meijer, meijer_errors, meijer_validation = meijer_import(latest_file)