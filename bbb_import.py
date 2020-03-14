# -*- coding: utf-8 -*-
# This script loads and cleans the Buy Buy Baby data 
# sent to Paul Hunt/team in excel format. 

#to-do: 
#Customer name column

import pandas as pd
import numpy as np

"""
#Need to read in different sheet for BBB - each workbook has three sheets...read in the first two. 
For testing:
latest_file = r'\\COL-foxfiles\Data\Departments\Sales\Sales Administration\pos-cleanup-utility\Data\current_raw_customer\BBBMainline TL W-E 02-29-2020.xlsx'
"""


def bbb_import(latest_file, datapath):
    #this will have to be changed to if: filename like... else error logic
    print("Importing BBB POS data file: {}".format(latest_file))
    bbb = pd.read_excel(latest_file,
                    sheet_name="Total",
                    skiprows=5,
#                   dtype={"UPC":"object"},
                    converters={"Sku":str, "UPC":str}).rename(columns={"Sku": "CustItemNumber"
                               ,"Sku Chain Retail":"Avg Retail"
                               ,"Sales Units LW TY":"POSQuantity"
                               ,"VPN":"CustVendStkNo"
                               ,"Unnamed: 7":"CustItemDesc"
#                              ,"Store Count TY":"Store Count"
 #                             ,"On Hand Units EOW LW TY":"On Hand"
  #                            ,"PO On Order Units EOW LW TY":"On Order"
                                  })

    
    
    
#   bbb2 = pd.read_excel(r"P:\BI\POS Data from Paul\bbb-quinny-2019-12-04.xlsx",
 #                   sheet_name="Total",
  #                  skiprows=5, 
   #                 converters={'UPC':str,'Sku':str})
    #bbb = bbb1.append(bbb2)
    
    bbb = bbb[bbb.Concept != "Total"]

    
    #Need VNDR STK NBR, Avg Retail, On hand, On OrderWTD POS Dol/Units, Store Count, 12 Digit UPC
    
    bbb['12 Digit UPC'] = bbb['UPC']
#   bbb['VNDR STK NBR'] = bbb['VNDR STK NBR'].str.strip()
    bbb['POSAmount'] = bbb['POSQuantity'] * bbb['Avg Retail']
    bbb = bbb[bbb['POSAmount'] != 0]
    
#   bbb['12 Digit UPC'] = bbb['12 Digit UPC'].str.lstrip("0")
    
    #print('Cleaning BBB UPC Codes that start with "19","44","52", "65", "72", "93"')
    bbb['12 Digit UPC'] = bbb['12 Digit UPC'].apply(lambda x: "0"+x.strip() if x.startswith(("19","44","52", "65", "72", "93")) else x)
    
    bbb = bbb[['CustItemNumber',
               'CustItemDesc',
               'CustVendStkNo',
               'Avg Retail', 
               'POSAmount', 
               'POSQuantity',
               '12 Digit UPC']]
    bbb['POSQuantity'] = bbb['POSQuantity'].astype("int")
    bbb['POSAmount'] = round(bbb['POSAmount'],2)
    
    print("Importing BBB Master Data...this takes a minute")
    bbb_master = pd.read_excel(datapath+r'\\POS Databasework2020.xlsx',
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str}).drop(columns=['Unnamed: 0']).rename(columns={"ITEM NBR":"ItemNumber"})

    #bbb_master['VNDR STK NBR'] = bbb_master['12 Digit UPC']

    bbb = (bbb.groupby(['CustItemNumber', 'CustItemDesc','12 Digit UPC', 'Avg Retail', 'CustVendStkNo']).agg(
            {'POSAmount': 'sum',
                    'POSQuantity':'sum',
                    #,Avg Retail':'max'
                    })
       .reset_index())
    
    bbb_merged = pd.merge(bbb, bbb_master, how="left", on="12 Digit UPC")
#   bbb_merged = bbb_merged.drop(columns=['VNDR STK NBR_y']).rename(columns={'VNDR STK NBR_x':'VNDR STK NBR'})

    if bbb_merged['POSAmount'].sum() != bbb['POSAmount'].sum():
            
        bbb_merged = bbb_merged.drop_duplicates(subset=['CustItemNumber',
                                           '12 Digit UPC',
                                           'Avg Retail', 
                                           'POSAmount',
                                           'POSQuantity',
                                           'CustVendStkNo'], keep='first')
    
    
    merged_cols = ['CustItemNumber','CustItemDesc','ItemNumber','POSAmount','POSQuantity', 'Short Brand', '12 Digit UPC']
    bbb_merged = bbb_merged[merged_cols]
    
    bbb_final = bbb_merged[pd.notnull(bbb_merged['Short Brand'])]
    bbb_errors = bbb_merged[bbb_merged['Short Brand'].isna()]
    
    pd.options.mode.chained_assignment = None
    #Create columns:
   
    bbb_final['BricksClicks'] = "Bricks"
    bbb_final['AccountMajor'] = "BUY BUY BABY"
    bbb_final['Account'] = "BUY BUY BABY"
    bbb_final.rename(columns={'12 Digit UPC':'UPC'}, inplace=True)
    bbb_final['CustVendStkNo'] = ''
    bbb_final['ItemID'] = ''

    #List comprehension for Premium column:
    premium_list = ['MAXI COSI', 'QUINNY', 'Bebe Confort', 'Baby Art', 'Hoppop', 'TINY LOVE']
    bbb_final['MainlinePremium'] = np.where(bbb_final['Short Brand'].isin(premium_list), 'Premium', 'Mainline')
    
    #bbb_final = bbb_final.rename(columns={"12 Digit UPC_x":"12 Digit UPC"}).drop(columns=['12 Digit UPC_y'])
    cols_list = ['AccountMajor', 'Account', 'BricksClicks', 'CustItemNumber','CustItemDesc','CustVendStkNo','ItemID','ItemNumber',
           'MainlinePremium','POSAmount','POSQuantity', 'UPC']
    
    bbb_final = bbb_final[cols_list]
    
    if len(bbb_errors) > 0:
        print(len(bbb_errors),"records not found in the Item Master crossreference. Please check the exception files and run again.")
    else:
        print(len(bbb_final),"BBB records processed.")
        
    bbb_validation = bbb_final.groupby(['AccountMajor']).agg({"POSAmount":'sum', 
                                                  "POSQuantity":"sum",
                                                  "BricksClicks":"count"}).reset_index()
    return bbb_final, bbb_errors, bbb_validation

