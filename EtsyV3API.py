#!/usr/bin/python3

import mysql.connector
from datetime import datetime
import logging
import requests
import json
from JSONtoSQL import JSONtoSQL
import configparser
from time import sleep


# Read Config File
config = configparser.ConfigParser()
config.read('EtsyV3API.conf')

# Set Global Variables
#PingURL = config['CONFIG']['Ping_URL']
#oAuth_URL = config['CONFIG']['oAuth_URL']

def SysDate ():
	return (datetime.now().strftime("%d-%b-%y %H:%M:%S"))
########################################################################################################################################################

def LogMsg (MessageText):
   logging.info ('%s: %s', SysDate(),MessageText)
########################################################################################################################################################

def getMaxTimeStamp(TableName, ColumnName, payload):
    DBUser = payload['DBUser']
    DBPassword = payload['DBPassword']
    DBHost = payload['DBHost']
    DBName = payload['DBName']
    
    cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
    rec = cnx.cursor()
    SQL = "select round(unix_timestamp(max(" + ColumnName + ")),0)+1 maxts from " + TableName
    rec.execute (SQL)

    for r in rec:
        maxts = r
    cnx.close()
    #print (r[0])
    #print (datetime.timestamp(r[0]))
    return (r[0])
########################################################################################################################################################

def checkToken(api_keystring,etsy_access_token, PingURL):
    myurl = PingURL
    headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}
    body=None

    LogMsg ("Checking token: " + PingURL)
    # Check validity of token
    resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))
    # If good status code do nothing 
    if resp.status_code == 200:
        LogMsg ("Token is good")
        LogMsg (" -- ")
        return True
    else:
        LogMsg ("Token needs to be refreshed")
        LogMsg (" -- ")
        return False
########################################################################################################################################################

def setToken (user_id, DBUser, DBPassword, DBHost, DBName,PingURL):
    cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
    rec = cnx.cursor()
    rec.execute("SELECT etsy_access_token, etsy_refresh_token, api_keystring, shop_id FROM etsy_oauth WHERE user_id = " + user_id)
    
    for r in rec:
        etsy_access_token, etsy_refresh_token, api_keystring, shop_id = r
    cnx.close()

    LogMsg (" -- ")
    LogMsg ("Grabbing token from database")
    LogMsg ("Access Token: " + etsy_access_token)
    LogMsg ("Refresh Token: " + etsy_refresh_token)
    LogMsg ("Shop ID: " + str(shop_id))
    LogMsg (" -- ")

    if checkToken(api_keystring,etsy_access_token,PingURL):
        return r
    else:
        payload = {'DBUser': DBUser, 'DBPassword': DBPassword, 'DBHost': DBHost, 'DBName': DBName}
        return refreshToken(user_id,shop_id,api_keystring,etsy_refresh_token, payload)
########################################################################################################################################################

def refreshToken(user_id,shop_id,api_keystring,etsy_refresh_token,payload):
    DBUser = payload['DBUser']
    DBPassword = payload['DBPassword']
    DBHost = payload['DBHost']
    DBName = payload['DBName']
    
    # Read Config File
    config = configparser.ConfigParser()
    config.read('EtsyV3API.conf')
    # Set Global Variables
    oAuth_URL = config['CONFIG']['oAuth_URL']
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'refresh_token',
        'client_id': api_keystring,
        'refresh_token': etsy_refresh_token
    }

    # Call Etsy V3 API to get Refreshed Token
    resp = requests.post(oAuth_URL, headers=headers, data=data)
    
    LogMsg ("Grabbing new token: " + resp.url)
    LogMsg ("Response Code: " + str(resp.status_code))
    
    if resp.status_code == 200:
        etsy_access_token = resp.json().get('access_token')
        etsy_refresh_token = resp.json().get('refresh_token')
        
        # Write tokens to the log
        LogMsg ("Access Token: " + etsy_access_token)
        LogMsg ("Refresh Token: " + etsy_refresh_token)
        LogMsg (" -- ")

        # Update database with new tokens
        cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
        rec = cnx.cursor()
        UpdateSQL = "UPDATE etsy_oauth SET etsy_access_token = '" + etsy_access_token + "', etsy_refresh_token = '" +  etsy_refresh_token + "', last_refresh_ts = sysdate() WHERE user_id = " + user_id
        rec.execute(UpdateSQL)
        cnx.commit()
        cnx.close()

        return etsy_access_token, etsy_refresh_token, api_keystring, shop_id
########################################################################################################################################################

def getShopTransactions(etsy_access_token, etsy_refresh_token, api_keystring, payload):
    shop_id = payload['shop_id']
    base_url = payload['base_url']
    DBUser = payload['DBUser']
    DBPassword = payload['DBPassword']
    DBHost = payload['DBHost']
    DBName = payload['DBName']
    user_id = payload['user_id']


    # MySQL Connection for Inserting Etsy Transactions
    cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
    cursor = cnx.cursor()

    MaxTS = getMaxTimeStamp('etsy_orders','create_timestamp',payload)

    myurl = base_url + f'/shops/{shop_id}/receipts?min_created={MaxTS}'

    # Set Headers based on token
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

    transactions = resp.json()

    if transactions['count'] == 0:
        LogMsg("There are no new orders at this time.")
    else:
        LogMsg ("Found new orders.")
        for orders in transactions['results']:

            # Insert Receipts
            try:
                insertReceiptSQL = JSONtoSQL("etsy_receipts", orders)
            except:
                LogMsg ("insertReciptSQL Failed")
            try:
                cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
                cursor = cnx.cursor()
                cursor.execute (insertReceiptSQL)
                cnx.commit()
                cnx.close()
            except mysql.connector.IntegrityError as err:
                LogMsg ("Receipt ID: " + str(orders['receipt_id']) + " already exist.")

            insertOrderSQL = "INSERT INTO etsy_orders"
            insertOrderSQL += " (transaction_id,etsy_title,seller_user_id,buyer_user_id,create_timestamp,paid_timestamp,shipped_timestamp,quantity,receipt_id,is_digital,listing_id,transaction_type,product_id,sku,price_amt,shipping_cost,shipping_profile_id,min_processing_days,max_processing_days,shipping_method,shipping_upgrade,expected_ship_date)"
            insertOrderSQL += " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            # Set Date Defaults
            orders.setdefault('create_timestamp','1970-01-01 00:00:00+00:00')
            orders.setdefault('paid_timestamp','1970-01-01 00:00:00+00:00')
            orders.setdefault('shipped_timestamp','1970-01-01 00:00:00+00:00')
            orders.setdefault('expected_ship_date','1970-01-01 00:00:00+00:00')

            # populate fields
            transaction_id = orders['transactions'][0]['transaction_id']
            title = orders['transactions'][0]['title']
            seller_user_id = orders['transactions'][0]['seller_user_id']
            buyer_user_id = orders['transactions'][0]['buyer_user_id']
            create_timestamp = datetime.fromtimestamp(orders['transactions'][0]['create_timestamp'])#[0:19]
            if orders['transactions'][0]['paid_timestamp'] == None:
                paid_timestamp = datetime(1970, 1, 1)
            else:
                paid_timestamp = datetime.fromtimestamp(orders['transactions'][0]['paid_timestamp'])#[0:19]
            if orders['transactions'][0]['shipped_timestamp'] == None:
                shipped_timestamp = datetime(1970, 1, 1)
            else:
                shipped_timestamp = datetime.fromtimestamp(orders['transactions'][0]['shipped_timestamp'])#[0:19]
            quantity = orders['transactions'][0]['quantity']
            receipt_id = orders['transactions'][0]['receipt_id']
            
            if orders['transactions'][0]['is_digital']:
                is_digital = 'Y'
            else:
                is_digital = 'N'
            
            listing_id = orders['transactions'][0]['listing_id']
            transaction_type = orders['transactions'][0]['transaction_type']
            product_id = orders['transactions'][0]['product_id']
            sku = orders['transactions'][0]['sku']
            price_amount = orders['transactions'][0]['price']['amount']/100
            shipping_cost_amount = orders['transactions'][0]['shipping_cost']['amount']/100
            #variations = orders['variations']
            shipping_profile_id = orders['transactions'][0]['shipping_profile_id']
            min_processing_days = orders['transactions'][0]['min_processing_days']
            max_processing_days = orders['transactions'][0]['max_processing_days']
            shipping_method = orders['transactions'][0]['shipping_method']
            shipping_upgrade = orders['transactions'][0]['shipping_upgrade']
            expected_ship_date = datetime.fromtimestamp(orders['transactions'][0]['expected_ship_date'])#[0:19]

            insertData = (transaction_id,title,seller_user_id,buyer_user_id,create_timestamp,paid_timestamp,shipped_timestamp,quantity,receipt_id,is_digital,listing_id,transaction_type,product_id,sku,price_amount,shipping_cost_amount,shipping_profile_id,min_processing_days,max_processing_days,shipping_method,shipping_upgrade,expected_ship_date)

            ## Connect to MySQL Database
            try:
                cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
                cursor = cnx.cursor()
                cursor.execute (insertOrderSQL, insertData)
            except mysql.connector.IntegrityError as err:
                LogMsg ("Transaction ID: " + str(transaction_id) + " already exists.")
            cnx.commit()
            cnx.close()

            payload = {'shop_id': shop_id, 'receipt_id': receipt_id, 'base_url': base_url, 'DBUser': DBUser, 'DBPassword': DBPassword, 'DBHost': DBHost, 'DBName': DBName}
            r = getShopReceipts(etsy_access_token, etsy_refresh_token, api_keystring, payload)
            p = getShopPaymentsByReceiptId(etsy_access_token, etsy_refresh_token, api_keystring, payload)
######################################################################################################################################################## 

def getShopPaymentAccountLedgerEntries (etsy_access_token, etsy_refresh_token, api_keystring, payload):
    shop_id = payload['shop_id']
    base_url = payload['base_url']
    DBUser = payload['DBUser']
    DBPassword = payload['DBPassword']
    DBHost = payload['DBHost']
    DBName = payload['DBName']
    user_id = payload['user_id']

    DataExists = True

    MinTS = getMaxTimeStamp('etsy_ledger','create_timestamp',payload)
    MaxTS = MinTS + 86400

    while DataExists:
        myurl = base_url + f'/shops/{shop_id}/payment-account/ledger-entries?min_created={MinTS}&max_created={MaxTS}&limit=100'

        # Set Headers based on token
        headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}
        body=None

        # Request data from Etsy V3 API
        resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))
        LogMsg (" -- ")

        # If good status code do nothing 
        if resp.status_code == 200:
            pass
        elif resp.status_code == 401 and resp.json().get('error') == 'invalid_token':
            token = refreshToken(user_id,api_keystring,etsy_refresh_token)
            headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}

            #try getting data again
            resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))

        try:
            transactions = resp.json()
            if transactions['count'] == 0:
                LogMsg("There are no new ledger entries.")
            else:
                LogMsg ("Found new ledger entries.")
                        
                for LedgerEntries in transactions['results']:
                    InsertLedgerSQL = JSONtoSQL("etsy_ledger", LedgerEntries)
                    try:
                        cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
                        cursor = cnx.cursor()
                        cursor.execute (InsertLedgerSQL)
                    except mysql.connector.IntegrityError as err:
                        print ("Ledger Exists")

                    cnx.commit()
                    cnx.close()

            MinTS = MaxTS
            MaxTS += 86400
        except:
            LogMsg("In getShopPaymentAccountLedgerEntries: Error occurred between " + str(MinTS) + " and " + str(MaxTS))
        
        #created_timestamp = int((datetime.now() - datetime(1970,1,1)).total_seconds())
        if MaxTS >= int((datetime.now() - datetime(1970,1,1)).total_seconds()): #created_timestamp:
            DataExists = False
########################################################################################################################################################

def getShopReceipts (etsy_access_token, etsy_refresh_token, api_keystring, payload):
    
    shop_id = payload['shop_id']
    receipt_id = payload['receipt_id']
    base_url = payload['base_url']
    DBUser = payload['DBUser']
    DBPassword = payload['DBPassword']
    DBHost = payload['DBHost']
    DBName = payload['DBName']
    user_id = payload['user_id']

    # Set URL    
    myurl = base_url + f'/shops/{shop_id}/receipts/{receipt_id}'

    # Set Headers based on token
    headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}
    body=None

    # Request data from Etsy V3 API
    resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))
    LogMsg (" -- ")
    LogMsg ("Looking for receipts")

    # If good status code do nothing 
    if resp.status_code == 401 and resp.json().get('error') == 'invalid_token':
        etsy_access_token = refreshToken(user_id,api_keystring,etsy_refresh_token)
        headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}

        #try getting data again
        resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))
    elif resp.status_code == 400:
        LogMsg ("Got 400 Bad Request. Trying again in 5 seconds")
        sleep(5)
        resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))

    if resp.status_code == 200:
        transactions = resp.json()

        LogMsg ("Found ReceiptID: " + str(receipt_id))

    # Insert Receipts
        try:
            insertReceiptSQL = JSONtoSQL("etsy_receipts", transactions)
            try:
                cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
                cursor = cnx.cursor()
                cursor.execute (insertReceiptSQL)
                cnx.commit()
                cnx.close()
            except mysql.connector.IntegrityError as err:
                LogMsg ("Receipt ID: " + str(transactions['receipt_id']) + " already exist.")
        except Exception as ex:
            LogMsg ("insertReceiptSQL Failed with " + ex)
        
    else:
        LogMsg ('getShopReceipts failed with error code ' + str(resp.status_code))
    
    retval = {'InsertSQL': insertReceiptSQL,'APIResponse': transactions}
    return retval
########################################################################################################################################################

def getShopPaymentsByReceiptId (etsy_access_token, etsy_refresh_token, api_keystring, payload):
    
    shop_id = payload['shop_id']
    receipt_id = payload['receipt_id']
    base_url = payload['base_url']
    DBUser = payload['DBUser']
    DBPassword = payload['DBPassword']
    DBHost = payload['DBHost']
    DBName = payload['DBName']
    user_id = payload['user_id']

    # Set URL    
    myurl = base_url + f'/shops/{shop_id}/receipts/{receipt_id}/payments'

    # Set Headers based on token
    headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}
    body=None

    # Request data from Etsy V3 API
    resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))
    LogMsg (" -- ")
    LogMsg ("Looking for payment records")

    # If good status code do nothing 
    if resp.status_code == 200:
        pass
    elif resp.status_code == 401 and resp.json().get('error') == 'invalid_token':
        etsy_access_token = refreshToken(user_id,api_keystring,etsy_refresh_token)
        headers = {'x-api-key': api_keystring, 'Authorization' : f'Bearer {etsy_access_token}'}

        #try getting data again
        resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))
    elif resp.status_code == 400:
        LogMsg ("Got 400 Bad Request. Trying again in 5 seconds")
        sleep(5)
        resp = requests.request(method='GET',url=myurl,headers=headers,data=json.dumps(body))

    transactions = resp.json()

    if transactions['count'] == 0:
        LogMsg("There is no payment record for ReceiptID: " + str(receipt_id))
    else:
        LogMsg ("Found payment for ReceiptID: " + str(receipt_id))
        for payments in transactions['results']:
            insertPaymentSQL = JSONtoSQL('etsy_payments',transactions['results'][0])
            
            # Connect to MySQL Database
            try:
                cnx = mysql.connector.connect(user=DBUser, password=DBPassword,	host=DBHost, database=DBName)
                cursor = cnx.cursor()
                cursor.execute (insertPaymentSQL)
            except mysql.connector.IntegrityError as err:
                LogMsg ("PaymentID " + str(payments['payment_id']) + " already exists.")
            cnx.commit()
            cnx.close()
    retval = {'InsertSQL': insertPaymentSQL,'APIResponse': transactions}
    return retval
########################################################################################################################################################
