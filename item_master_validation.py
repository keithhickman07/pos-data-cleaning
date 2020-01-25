# -*- coding: utf-8 -*-
"""
Created on Sun Jan 12 10:56:54 2020

@author: khickman
"""
from mysql.connector import (connection)
import pandas as pd

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
    ph_master = pd.read_excel("P:\BI\POS Data from Paul\POS Databasework2020.xlsx",
                             sheet_name="Dorel Master", 
                             skiprows=6,
                             converters={'12 Digit UPC':str,
                                         'Dorel UPC':str,
                                         'Short UPC':str})

    im_cols = ['ITEM NBR']
    ph_master = ph_master[im_cols]
    ph_master = ph_master['ITEM NBR'].dropna().reset_index()

    return ph_master



items_list = get_item_master()
ph_master = get_ph_dorel_master()

ph_master['invalid'] = ph_master['ITEM NBR'].apply(lambda x: 0 if x in items_list else 1)

invalid_items = ph_master[ph_master['invalid'] == 1]
if len(invalid_items) > 0:
    print("there are invalid items in the master file")
else:
    print("No invalid items found")




