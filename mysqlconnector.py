# -*- coding: utf-8 -*-
"""
Created on Sun Jan 12 10:56:54 2020

@author: khickman
"""
from mysql.connector import (connection)

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

item_ids = [x[0] for x in records]
item_nums = [x[1] for x in records]

#for x in cursor:
#    print(x)

cnx.close()