# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 07:28:19 2019

@author: khickman
@description: This file utilizes custom modules to process weekly point of sale data. 
@date: 1/9/2019

"""
import pandas as pd
import datetime
import os
#from pandas.tseries.offsets import *


#Custom Modules:
import depot_import as depot
import amazon_import as amazon
import bbb_import as bbb
import kmart_import as kmart
import marketplace_import as market
import meijer_import as meijer
import nordstrom_import as nordstrom
import sams_import as sams
import sears_import as sears
import target_import as target
import walmart_import as walmart
import wmcom_import as wmcom


def cust_select():
    print("Customer List")
    print("Enter as shown below:")
    print("--------------")
    print("amazon")
    print("BBB: enter as bbb-mainline OR bbb-maxi")
    print("depot")
    print("kmart")
    print("marketplace")
    print("meijer")
    print("nordstrom")
    print("sams")
    print("sears")
    print("target")
    print("walmart - Bricks: enter as walmart")
    print("walmart.com - enter as wmcom")
    
    customer = input("Enter a customer name: ").lower()
    
    return customer

"""
Select most current file:
[ ] change output column names 
[ ] modify item master validation logic to output of function...return item masters.
[ ] Dorel Week

"""

path = 'P:\BI\POS Data from Paul\customer files 20XX'
customer = cust_select()

"""
Pass in customer name to select the last-added file per customer:
"""
def file_select(customer):
    all_files = []
    for subdir, dirs, files in os.walk(path):
        for file in files:
            #implement with multiple customers:
            #        for cust in customer:
            if customer in file.lower():
                all_files.append(os.path.join(subdir,file))
                
            for file in all_files:
                #list_of_files = glob.glob(r'P:\BI\POS Data from Paul\customer files 20XX\{}*'.format(customer)) # * means all if need specific format then *.csv
                latest_file = max(all_files, key=os.path.getctime)
    return latest_file

#file_select("bbb-maxi")

"""
Date Logic: 
    [ ] include DorelWeek
"""
def date_calcs():
    #now = datetime.datetime.now()
   
    lastweektoday = (datetime.datetime.today().date() - datetime.timedelta(days=7)) #one week ago today
    start_of_week = lastweektoday - datetime.timedelta(days=lastweektoday.weekday()) #start of last week
    end_of_week = start_of_week + datetime.timedelta(days=5) #week ending saturday
    year = end_of_week.year

    if end_of_week.month in [1, 2, 3]:
        q = 1
    elif end_of_week.month in [4, 5, 6]:
        q = 2   
    elif end_of_week.month in [7, 8, 9]:
        q = 3
    else:
        q = 4    
    return end_of_week, year, q

end_of_week, year, quarter = date_calcs()


"""
Check if file already exists, and/or if customer is currently in consolidated POS file:
"""

def already_run():
    if os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
        runs = pd.read_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep='\t')
        already_run = pd.Series(runs['runcheck'].unique()).to_list()
        already_run = [x.lower() for x in already_run]

    else:
        print("No customer files have been run yet")
        already_run = []

    return already_run

runs = already_run()


"""
Validate Item Master
"""
from mysql.connector import (connection)

def get_item_master():

    cnx = connection.MySQLConnection(user='NAUTILUS', 
                                  password='kermit',
                                  host='cave-bzintl1',
                                  database='jde', 
                                  port = 5029,
                                  charset='utf8'
                                  #use_pure=False
                                  )
    
    cursor = cnx.cursor()
    
    query = ("SELECT DISTINCT ITM_IdentifierShortItem as ItemID, LITM_Identifier2ndItem as ItemNumber FROM ods_f4101_item_master")
    
    
    cursor.execute(query)
    records = cursor.fetchall()
    
#    item_ids = [x[0] for x in records]
    item_nums = [x[1] for x in records]
    
    #for x in cursor:
    #    print(x)
    
    cnx.close()
    
#    items_df = pd.DataFrame(list(zip(item_ids, item_nums)), columns=['ItemId', 'ItemNumber'])
    return item_nums


def get_ph_dorel_master():
    ph_master = pd.read_excel(r"P:\BI\POS Data from Paul\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})

    im_cols = ['ITEM NBR']
    ph_master = ph_master[im_cols]
    ph_master = ph_master['ITEM NBR'].dropna().reset_index()

    return ph_master


print("Validating Item Master")
items_list = get_item_master()
ph_master = get_ph_dorel_master()

ph_master['invalid'] = ph_master['ITEM NBR'].apply(lambda x: 0 if x in items_list else 1)

invalid_items = ph_master[ph_master['invalid'] == 1]
if len(invalid_items) > 0:
    print("there are invalid items in the master file")
else:
    print("No invalid items found")



"""
Main logic:
    - check whether customer has already been run
    - parse through user input to determine which file to run
"""

if customer in runs:
    print("This customer has already been run this week")
    
else:
    if customer == "amazon":
        amazon_final, amazon_errors, amazon_validation = amazon.amazon_import(file_select(customer))
        amazon_final['WeekEnding'] = end_of_week
        
        if len(amazon_errors) > 0:
            amazon_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve exceptions and re-run")
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            amazon_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            amazon_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week), sep="\t")
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            amazon_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            amazon_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))

    
    elif customer == "bbb-mainline":
        bbb_final, bbb_errors, bbb_validation = bbb.bbb_import(file_select(customer), customer)
        bbb_final['WeekEnding'] = end_of_week
       
        if len(bbb_errors) > 0:
            bbb_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
              
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            bbb_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            bbb_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            bbb_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            bbb_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
            
    elif customer == "bbb-maxi":
        bbb_final, bbb_errors, bbb_validation = bbb.bbb_import(file_select(customer), customer)
        bbb_final['WeekEnding'] = end_of_week
       
        if len(bbb_errors) > 0:
            bbb_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
              
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            bbb_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            bbb_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            bbb_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            bbb_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
            
    elif customer == "depot":
        depot_final, depot_errors, depot_validation = depot.depot_import(file_select(customer))
        depot_final['WeekEnding'] = end_of_week
        
        if len(depot_errors) > 0:
            depot_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
              
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            depot_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            depot_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            depot_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            depot_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        
    elif customer == "kmart":
        kmart_final, kmart_errors, kmart_validation = kmart.kmart_import(file_select(customer))
        kmart_final['WeekEnding'] = end_of_week
                    
        if len(kmart_errors) > 0:
            kmart_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            kmart_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            kmart_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            kmart_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            kmart_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
    
    elif customer == "nordstrom":
        nordstrom_final, nordstrom_errors, nordstrom_validation = nordstrom.nordstrom_import(file_select(customer))
        nordstrom_final['WeekEnding'] = end_of_week
    
        if len(nordstrom_errors) > 0:
            nordstrom_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            nordstrom_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            nordstrom_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            nordstrom_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            nordstrom_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
    
    elif customer == "marketplace":
        market_final, market_errors, market_validation = market.market_import(file_select(customer))
        market_final['WeekEnding'] = end_of_week
    
        if len(market_errors) > 0:
            market_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            market_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            market_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            market_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            market_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
    
    
    elif customer == "meijer":
        meijer_final, meijer_errors, meijer_validation = meijer.meijer_import(file_select(customer))
        meijer_final['WeekEnding'] = end_of_week
    
        if len(meijer_errors) > 0:
            nordstrom_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            meijer_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            meijer_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            meijer_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            meijer_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
    
    
    elif customer == "sams":
        sams_final, sams_errors, sams_validation = sams.sams_import(file_select(customer))
        sams_final['WeekEnding'] = end_of_week
    
        if len(sams_errors) > 0:
            sams_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            sams_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            sams_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            sams_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            sams_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
            
    elif customer == "sears":
        sears_final, sears_errors, sears_validation = sears.sears_import(file_select(customer))
        sears_final['WeekEnding'] = end_of_week
        
        if len(sears_errors) > 0:
            sears_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions.csv'.format(customer))
            print("Please resolve exceptions and re-run")
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            sears_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            sears_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            sears_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            sears_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
    
    elif customer == "target":
        target_final, target_errors, target_validation = target.target_import(file_select(customer))
        target_final['WeekEnding'] = end_of_week
    
        if len(target_errors) > 0:
            target_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve",len(target_errors),"exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            target_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            target_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            target_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            target_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
    
    elif customer == "walmart":
        walmart_final, walmart_errors, walmart_validation = walmart.walmart_import(file_select(customer))
        
        if len(walmart_errors) > 0:
            walmart_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve", len(walmart_errors),"exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            walmart_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            walmart_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            walmart_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            walmart_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
    
    elif customer == "wmcom":
        wmcom_final, wmcom_errors, wmcom_validation = wmcom.wmcom_import(file_select(customer))
        if len(wmcom_errors) > 0:
            wmcom_errors.to_csv(r'P:\BI\POS Data from Paul\exception_files\{}_exceptions_{}.csv'.format(customer, end_of_week))
            print("Please resolve", len(wmcom_errors),"exceptions and re-run")
            
        elif os.path.exists(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week)):
            print("Appending to existing file")
            wmcom_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), mode='a', header='false', sep="\t", index=False)
            wmcom_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        else:
            print("Creating new consolidated POS file for Week-ending {}".format(end_of_week))
            wmcom_final.to_csv(r"P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv".format(end_of_week), sep="\t", index=False)
            wmcom_validation.to_csv(r'P:\BI\POS Data from Paul\validation_files\{}_validation_{}.csv'.format(customer, end_of_week))
        
        
    else:
        pass

#weekly_pos_consol = pd.read_csv(r'P:\BI\POS Data from Paul\weekly_pos_consolidated\pos_consol-{}.csv'.format(end_of_week), sep="\t")



