# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

"""
For testing: 
latest_file = (r"\\COL-foxfiles\Data\Departments\Sales\Sales Administration\pos-cleanup-utility\Data\current_raw_customer\Home Depot W-E 02-29-2020.xlsx")
"""

def depot_import(latest_file, datapath):

    print("Importing Home Depot POS Data file: {}".format(latest_file))
    depot = pd.read_excel(latest_file, 
                        encoding="latin", 
                        skiprows=2, 
                        #sheet_name="SAFETY 1st FISCAL WEEK 44", 
                        dtypes={"SKU":"str"}).rename(columns={"SKU":"CustItemDesc",
                          "Sales $":"POSAmount",
                          "Sales Units":"POSQuantity"})
    
    
    #remove total/subtotal rows
    depot = depot[pd.notnull(depot['CustItemDesc'])]
    
    depot['CustItemNumber'] = depot['CustItemDesc'].str.split(" ").str[0].str.strip()
    depot['CustItemNumber'] = depot['CustItemNumber'].str.split("-").str[0].str.strip()
    depot['CustItemDesc'] = depot['CustItemDesc'].str.split(" ").str[1:].str.join(" ").str.strip()

    depot = depot[['CustItemNumber', 'POSAmount', 'POSQuantity', 'CustItemDesc']]

    depot = depot[depot['POSAmount'] != 0]
    depot['POSQuantity'] = round(depot['POSQuantity'].astype("int"),0)
    depot['POSAmount'] = round(depot['POSAmount'],2)
    
    print("Importing Home Depot Master")
    depot_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str,
                                         'Home Depot':str}).rename(columns={"ITEM NBR":"ItemNumber"})

    depot_master['CustItemNumber'] = depot_master['Home Depot']
    depot_master = depot_master.drop(columns=['Unnamed: 0'])

    depot_merged = pd.merge(depot, depot_master, on="CustItemNumber", how="left")
    
    if depot_merged['POSAmount'].sum() != depot['POSAmount'].sum():
                
        depot_merged = depot_merged.drop_duplicates(subset=['CustItemNumber',
                                           'POSQuantity',                                           
                                           'POSAmount'], keep='first')
    
    depot_final = depot_merged[pd.notnull(depot_merged['12 Digit UPC'])]
    depot_errors = depot_merged[depot_merged['12 Digit UPC'].isna()]
    
    #depot_final.to_csv("depot_final_{}.csv".format(pd.datetime.now().date()))
    #depot_errors.to_csv("depot_final_{}.csv".format(pd.datetime.now().date()))

    pd.options.mode.chained_assignment = None
    #Create columns:
   
    depot_final['BricksClicks'] = "Bricks"
    depot_final['AccountMajor'] = "HOME DEPOT"
    depot_final['Account'] = 'HOME DEPOT'
    depot_final['ItemID'] = ''
    depot_final['CustVendStkNo'] = ''
    depot_final['UPC'] = ''
    
    #List comprehension for Premium column:
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    depot_final['MainlinePremium'] = np.where(depot_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']
    
    
    depot_final = depot_final[cols_list]
    
    if len(depot_errors) > 0:
        print(len(depot_errors),"Depot records not found in the Item Master crossreference. Please check the exception files and run again.")
        #print(len(depot_final),"Depot records processed.")
    else:
        print(len(depot_final),"rows successfully processed.")

    depot_validation = depot_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    return depot_final, depot_errors, depot_validation

#depot_final, depot_errors, depot_validation = depot_import(r"P:\BI\POS Data from Paul\customer files 20XX\depot-01-04-2020.xlsx")