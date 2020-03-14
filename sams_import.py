# -*- coding: utf-8 -*-

"""
latest_file = (r'P:\BI\POS Data from Paul\customer files 20XX\sams-01-11-2020.xlsx')
"""


import pandas as pd
import numpy as np

def sams_import(latest_file, datapath):

    print("Importing Sams POS Datafile: {}".format(latest_file))
    
    sams = pd.read_excel(latest_file, 
                    encoding="latin", 
#                    skiprows=12, 
                    header=None)

    sams = sams.dropna(axis=0, how='any')
    sams.columns = sams.iloc[0]
    sams = sams[1:].rename(columns={"Vendor Stock Nbr":"CustVendStkNo",
                                       "Total Sell Dollars":"POSAmount",
                                       "Total Units Sold":"POSQuantity",
                                        "Item Nbr":"CustItemNumber",
                                        "Item Desc 1":"CustItemDesc"})

    sams = sams[sams['POSAmount'] != 0]
    
    print("Importing Sams Master Data")
    
    sams_master = pd.read_excel(datapath+r"\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str}).rename(columns={"ITEM NBR":"ItemNumber",
                                                        })
    
    sams_master = sams_master.drop(columns=['Unnamed: 0'])
    sams_master['CustVendStkNo'] = sams_master['ItemNumber']

    sams_merged = pd.merge(sams, sams_master, how="left", on="CustVendStkNo")
    
    sams_final = sams_merged[pd.notnull(sams_merged['Brand'])]
    sams_errors = sams_merged[sams_merged['Brand'].isna()]


    pd.options.mode.chained_assignment = None
    
    sams_final['BricksClicks'] = "Bricks"
    sams_final['AccountMajor'] = "SAMS"
    sams_final['Account'] = "SAMS"
    sams_final['UPC'] = ""
    sams_final['ItemID'] = ""

    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    sams_final['MainlinePremium'] = np.where(sams_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
               'MainlinePremium','POSAmount','POSQuantity', 'UPC']

    
    sams_final = sams_final[cols_list]
    
    sams_validation = sams_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    
    print(len(sams_final),"Sams records successfully processed")

    return sams_final, sams_errors, sams_validation

#sams_final, sams_errors, sams_validation = sams_import(latest_file)
