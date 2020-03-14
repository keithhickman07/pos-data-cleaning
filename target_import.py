# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

"""
For testing:
latest_file = (r'\\COL-foxfiles\Data\Departments\Sales\Sales Administration\pos-cleanup-utility\Data\current_raw_customer\Target W-E  03-07-2020.xlsx')

"""

def target_import(latest_file, datapath):

    print("Importing target POS Datafile: {}".format(latest_file))
    target = pd.read_excel(latest_file, 
#                       encoding="latin",
#                       skiprows=1, 
#                       
                        #converters={"Sales $":float}
                        #dtypes={"Sales $":np.float64}
                        )
    
    target['POSAmount'] = target['Sales $'].replace("--",0).astype("float64")
    target['POSQuantity'] = target['Sales U'].replace("--", 0)
    #target['On Order'] = target['End OO U'].replace("--", np.nan).astype("float64")
    target['BricksClicks'] = target['Channel'].apply(lambda x:"Bricks" if x == "STORE" else "Clicks")
    target['Account'] = target['Fulfillment Method']
    target = target[target['Channel']!='DC']
    #target = target[target['POSQuantity'] != 0]
    target = target[target['POSAmount'] != 0]
    target['CustVendStkNo'] = target['Model']
    target['CustItemDesc'] = target['Description']

    target['POSQuantity'] = target['POSQuantity'].astype("int")

    # Lambda function to check if a given vaue is bricks or clicks.
    target['BricksClicks'] = target['Channel'].apply(lambda x:"Clicks" if x == "ONLINE" else "Bricks")
    target['CustItemNumber'] = target['Item'].str.replace("-","")
    
    print("Importing Target Master Data")
    target_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str}).rename(columns={"ITEM NBR":"ItemNumber"})
    target_master['CustItemNumber'] = target_master['Target']
    
    target_merged = pd.merge(target, target_master, on="CustItemNumber", how="left")
    
    #DUPLICATE HANDLING
    if len(target) != len(target_merged):
        target_merged = target_merged.drop_duplicates(subset=['CustItemNumber',
                                          'CustItemDesc',
                                          'CustVendStkNo',
                                          'Fulfillment Method',
                                          'Channel',
                                          'POSQuantity',
                                          'POSAmount'],
                                          keep='first')
    else:
        pass
    
    #First merge
    target_errors = target_merged[target_merged['Short Brand'].isna()].dropna(how="all",axis=1)
    target_final = target_merged[pd.notnull(target_merged['Short Brand'])]


    pd.options.mode.chained_assignment = None
    #Second merge if errors:    
    if len(target_errors) > 0:
        

        target_errors = target_errors.dropna(how="all",axis=1)
        target_errors['ItemNumber'] = target_errors['Model']
        target_merged = pd.merge(target_errors, target_master, how="left", on="ItemNumber")
        target_final = target_final.append(target_merged[pd.notnull(target_merged['Brand'])])
        target_errors = target_merged[target_merged['Brand'].isna()].dropna(how="all",axis=1)

    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    target_final['MainlinePremium'] = np.where(target_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    target_final['AccountMajor'] = "TARGET"
    target_final['UPC'] = ''
    target_final['ItemID'] = ''
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']

    
    target_final = target_final[cols_list]
      
    if len(target_errors) == 0:
        print("No merge/join errors")
    else:
        print(len(target_errors),"records not found in Master Data")
            
    target_validation = target_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}, sort=False).reset_index()
    
    return target_final, target_errors, target_validation

#target_final, target_errors, target_validation = target_import(latest_file)