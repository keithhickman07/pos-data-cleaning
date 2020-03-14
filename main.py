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
import re
import time
import shutil


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



"""
Path locations:
    Functions will take datapath for Item-master cross-ref file
"""
#appdir = os.getcwd()
datapath = r'\\COL-foxfiles\Data\Departments\Sales\Sales Administration\pos-cleanup-utility\Data'
exception_path = datapath+r'\current_exceptions'
tsv_path = datapath+r'\current_processed_tsv'
csv_path = datapath+r'\archive_processed_csv'
validation_path = datapath+r'\validation'
archive_raw = datapath+r'\archive_raw'


"""
Date Logic: 
    [ ] include DorelWeek
    returns LW end of week, quarter, year
    Include future date logic funcs here. 
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
Validate Item Master
    Connects to infobright using python-mysql connector
    returns unique Item ID and Item Number
    outputs a dataframe
    [ ] write dataframe to csv
    [ ] for check logic - read from csv if timestamp within the last week
    [ ] else: get an updated item master
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
   
    query = (r'SELECT DISTINCT ITM_IdentifierShortItem as ItemID, LITM_Identifier2ndItem as ItemNumber FROM ods_f4101_item_master')
    cursor.execute(query)
    records = cursor.fetchall()
    item_nums = [x[1] for x in records]
    
    cnx.close()
    return item_nums

"""
get_ph_dore_master Returns Dorel Item Master (excel)
for cross-validation with Item master (data warehouse) from above function
"""

def get_ph_dorel_master():
    ph_master = pd.read_excel(datapath+r'\POS Databasework2020.xlsx',
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})

    im_cols = ['ITEM NBR']
    ph_master = ph_master[im_cols]
    ph_master = ph_master['ITEM NBR'].dropna().reset_index()

    return ph_master


print("Validating Item Master Crossreference ")

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

while True:
    runnit = str(input("Do you want to process a customer file?: [y/N]: ")).lower()
    while runnit.lower() not in ("y","n"):
        print("Please enter 'y' or 'n'")
        runnit = str(input("Do you want to process a customer file?: [y/N]: ")).lower()
    
    """
    if runnit = no, then break, else run main code block
    """
    if runnit == "n":
        break
    
    else:
                
        """
        Main code block:
        Pass in customer name to select the last-added file per customer:
            Checks whether the file name (lowercase) matches the customer name
        """
    
    
        def cust_select():
            print("*"*20)
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
            customer = re.sub(r'[^\w\s]','',customer)
            
            return customer
        
        customer = cust_select()
        
        def file_select(customer):
            latest_file = ""
            for subdir, dirs, files in os.walk(datapath+'\current_raw_customer'):
                for file in files:
                    if customer in re.sub(r'[^\w\s]','',file).lower():
                       latest_file = (os.path.join(subdir,file))
                       break
                    else:
                        continue
    
            return latest_file
                        
    
        latest_file = file_select(customer)
        
        def check_latest(latest_file):
            run_main = False
            if latest_file == "":
                print("Customer file not found. Please try again")
                customer=cust_select()
                file_select(customer)
                
            else:
                run_main = True
                
            return run_main
        run_main = check_latest(latest_file)
        
    
        if run_main:
                   
            if customer == "amazon":
                amazon_final, amazon_errors, amazon_validation = amazon.amazon_import(file_select(customer), datapath)
                amazon_final['WeekEnding'] = end_of_week
                
                if len(amazon_errors) > 0:
                    amazon_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    amazon_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    amazon_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    amazon_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
            
            elif customer == "bbbmainline":
                bbb_final, bbb_errors, bbb_validation = bbb.bbb_import(file_select(customer), datapath)
                bbb_final['WeekEnding'] = end_of_week
               
                if len(bbb_errors) > 0:
                    bbb_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run") 
                    
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    bbb_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    bbb_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    bbb_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
                    
            elif customer == "bbbmaxi":
                bbb_final, bbb_errors, bbb_validation = bbb.bbb_import(file_select(customer), datapath)
                bbb_final['WeekEnding'] = end_of_week
               
                if len(bbb_errors) > 0:
                    bbb_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")  
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    bbb_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    bbb_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    bbb_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
                    
            elif customer == "depot":
                depot_final, depot_errors, depot_validation = depot.depot_import(file_select(customer), datapath)
                depot_final['WeekEnding'] = end_of_week
                
                if len(depot_errors) > 0:
                    depot_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")  
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    depot_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    depot_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    depot_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
                
            elif customer == "kmart":
                kmart_final, kmart_errors, kmart_validation = kmart.kmart_import(file_select(customer), datapath)
                kmart_final['WeekEnding'] = end_of_week
                            
                if len(kmart_errors) > 0:
                    kmart_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    kmart_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    kmart_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    kmart_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            
            elif customer == "nordstrom":
                nordstrom_final, nordstrom_errors, nordstrom_validation = nordstrom.nordstrom_import(file_select(customer), datapath)
                nordstrom_final['WeekEnding'] = end_of_week
            
                if len(nordstrom_errors) > 0:
                    nordstrom_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    nordstrom_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    nordstrom_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    nordstrom_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            
            elif customer == "marketplace":
                market_final, market_errors, market_validation = market.market_import(file_select(customer), datapath)
                market_final['WeekEnding'] = end_of_week
            
                if len(market_errors) > 0:
                    market_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    market_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    market_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    market_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            
            elif customer == "meijer":
                meijer_final, meijer_errors, meijer_validation = meijer.meijer_import(file_select(customer), datapath)
                meijer_final['WeekEnding'] = end_of_week
            
                if len(meijer_errors) > 0:
                    meijer_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    meijer_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    meijer_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    meijer_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            
            elif customer == "sams":
                sams_final, sams_errors, sams_validation = sams.sams_import(file_select(customer), datapath)
                sams_final['WeekEnding'] = end_of_week
            
                if len(sams_errors) > 0:
                    sams_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    sams_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    sams_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    sams_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
                    
            elif customer == "sears":
                sears_final, sears_errors, sears_validation = sears.sears_import(file_select(customer), datapath)
                sears_final['WeekEnding'] = end_of_week
                
                if len(sears_errors) > 0:
                    sears_errors.to_csv(exception_path+r'\{}_exceptions.csv'.format(customer))
                    print("Please resolve exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    sears_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    sears_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    sears_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            
            elif customer == "target":
                target_final, target_errors, target_validation = target.target_import(file_select(customer), datapath)
                target_final['WeekEnding'] = end_of_week
            
                if len(target_errors) > 0:
                    target_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve",len(target_errors),"exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    target_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    target_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    target_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            
            elif customer == "walmart":
                walmart_final, walmart_errors, walmart_validation = walmart.walmart_import(file_select(customer), datapath)
                
                if len(walmart_errors) > 0:
                    walmart_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve", len(walmart_errors),"exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    walmart_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    walmart_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    walmart_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            
            elif customer == "wmcom":
                wmcom_final, wmcom_errors, wmcom_validation = wmcom.wmcom_import(file_select(customer), datapath)
                if len(wmcom_errors) > 0:
                    wmcom_errors.to_csv(exception_path+r'\{}_exceptions_{}.csv'.format(customer, end_of_week))
                    print("Please resolve", len(wmcom_errors),"exceptions and re-run")
                else:
                    print("Creating new consolidated {} file for Week-ending {}".format(customer,end_of_week))
                    wmcom_final.to_csv(tsv_path+r'\{}-{}.csv'.format(customer,end_of_week), sep="\t", index=False)
                    wmcom_final.to_csv(csv_path+r'\{}-{}.csv'.format(customer,end_of_week), index=False)
                    wmcom_validation.to_csv(validation_path+r'\{}_validation_{}.csv'.format(customer, end_of_week))
                    shutil.move(file_select(customer), archive_raw)
        
            else:
                print("Didn't recognize that customer...please enter the name again...slowly.")
        else:
            print("Customer not found...can we try it again?")

        time.sleep(2)

print("Program exiting...y'all come back now, y'hear?")        
time.sleep(4)
