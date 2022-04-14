#!/usr/bin/python3

#import time
import json
import logging
import requests
import datetime
import mysql.connector
from datetime import datetime
from EtsyV3API import LogMsg
from EtsyV3API import setToken
from EtsyV3API import getShopTransactions
from EtsyV3API import getShopPaymentAccountLedgerEntries
from EtsyV3API import getShopReceipts
from EtsyV3API import getShopPaymentsByReceiptId
from EtsyV3API import refreshToken
import configparser


def getAllPaymentAndRecipts(api_key, token, base_url, payload):
    shop_id = payload['shop_id']

    cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
    receipts = cnx.cursor()
    SQL = "SELECT DISTINCT receipt_id FROM etsy_orders"
    receipts.execute (SQL)

    for ReceiptID in receipts:
        payload = {'shop_id': shop_id, 'receipt_id': ReceiptID[0]}
        r = getShopReceipts(api_key, token, base_url, payload)
        #print (r['InsertSQL'])
        p = getShopPaymentsByReceiptId(api_key, token, base_url, payload)
        #print (p['APIResponse'])
    cnx.close()

def UpdateOrder(etsy_access_token, etsy_refresh_token, api_keystring, payload):
    shop_id = payload['shop_id']
    transaction_id = payload['transaction_id']

    # Set URL and Headers
    myurl = base_url + f'/shops/{shop_id}/transactions/{transaction_id}'
    headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}
    body=None

    # Request data from Etsy V3 API
    resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))

    # If good status code do nothing 
    if resp.status_code == 200:
        pass
    elif resp.status_code == 401 and resp.json().get('error') == 'invalid_token':
        etsy_access_token = refreshToken(user_id,api_keystring,etsy_refresh_token)
        headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}

        #try getting data again
        resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))

    Receipts = resp.json()

    # Build Update SQL
    updateOrderSQL = "UPDATE etsy_orders"
    updateOrderSQL += " set paid_timestamp = %s, shipped_timestamp = %s, expected_ship_date = %s"
    updateOrderSQL += " WHERE transaction_id = %s"

    #Set Values
    if Receipts['paid_timestamp'] == None:
        paid_timestamp = datetime(1970, 1, 1)
    else:
        paid_timestamp = datetime.fromtimestamp(Receipts['paid_timestamp'])
    
    if Receipts['shipped_timestamp'] == None:
        shipped_timestamp = datetime(1970, 1, 1)
    else:
        shipped_timestamp = datetime.fromtimestamp(Receipts['shipped_timestamp'])

    expected_ship_date = datetime.fromtimestamp(Receipts['expected_ship_date'])

    # Build Update Data Dict
    updateData = (paid_timestamp, shipped_timestamp, expected_ship_date, transaction_id)
    LogMsg ("Updating Transaction ID: " + str(transaction_id))

    cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
    cursor = cnx.cursor()
    cursor.execute (updateOrderSQL, updateData)
    cnx.commit()
    cnx.close()

def checkOrderUpdates(etsy_access_token, etsy_refresh_token, api_keystring, payload):
    shop_id = payload['shop_id']
    LogMsg (" -- ")
    
    OrderSQL = "SELECT transaction_id, receipt_id FROM etsy_orders WHERE shipped_timestamp = '1970-01-01 00:00:00'"

    ## Connect to MySQL Database
    SQLConnection = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)

    # Instantiate OrderSQLCursor
    OrderSQLCursor = SQLConnection.cursor()

    # Execute Order SQL Query
    OrderSQLCursor.execute (OrderSQL)
    
    if OrderSQLCursor.rowcount == 0:
        LogMsg ("There are no updatable orders")
    else:
        for (TransactionID, ReceiptID) in OrderSQLCursor:
            payload = {'shop_id': shop_id, 'transaction_id': TransactionID}
            UpdateOrder(etsy_access_token, etsy_refresh_token, api_keystring, payload)

################################### Begin #####################################
# Read Config File
config = configparser.ConfigParser()
config.read('EtsyV3API.conf')

# Set Variables
DBUser = config['MYSQL_CONNECTION']['Username']
DBPassword = config['MYSQL_CONNECTION']['Password']
DBHost = config['MYSQL_CONNECTION']['Host']
DBName = config['MYSQL_CONNECTION']['Database']
LogFileName = config['CONFIG']['LogFileName']
user_id = config['CONFIG']['User_ID']
base_url = config['CONFIG']['Base_URL']
PingURL = config['CONFIG']['Ping_URL']
oAuth_URL = config['CONFIG']['oAuth_URL']

# Start Logging
logging.basicConfig(filename=LogFileName,level=logging.INFO)
LogMsg ("===============================================================================")
LogMsg ("Starting Etsy Job")

# Get the token and refresh if needed
etsy_access_token, etsy_refresh_token, api_keystring, shop_id = setToken(user_id, DBUser, DBPassword, DBHost, DBName, PingURL)

payload = {'shop_id': shop_id, 'base_url': base_url, 'DBUser': DBUser, 'DBPassword': DBPassword, 'DBHost': DBHost, 'DBName': DBName, 'user_id': user_id, 'oAuth_URL': oAuth_URL}
getShopTransactions(etsy_access_token, etsy_refresh_token, api_keystring, payload)
checkOrderUpdates(etsy_access_token, etsy_refresh_token, api_keystring, payload)
getShopPaymentAccountLedgerEntries(etsy_access_token, etsy_refresh_token, api_keystring, payload)

# Only needed when reloading all data again
#getAllPaymentAndRecipts(api_keystring, etsy_access_token, base_url, payload)

LogMsg ("Ending Etsy Job")
LogMsg (" ")
