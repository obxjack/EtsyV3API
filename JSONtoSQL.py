#!/usr/bin/python3

import json
from mysqlx import IntegrityError

def JSONtoSQL(TableName, data):
    sqlstatement = ""

    for j in data:
        keylist = "("
        valuelist = "("
        firstPair = True
        for key, value in data.items():
            if value == None:
                pass
            else:
                if type(value) == list:
                    pass
                else:
                    if firstPair and type(value) != dict:
                        keylist += key
                    if not firstPair and type(value) != dict:
                        keylist += ", " + key
                        valuelist += ", "
                    firstPair = False
                    #keylist += key

                    if value is None:
                        valuelist += "'0'"
                    elif type(value) == bool:
                        if value is True:
                            valuelist += "1"
                        else:
                            valuelist += "0"

                    elif type(value) == int:
                        valuelist += str(value)
                    
                    elif type(value) == str:
                        valuelist += "'" + value + "'"

                    elif type(value) == dict:
                        # Loop through Dictionary Value
                        for interiorkey, interiorvalue in value.items():
                            if not firstPair:
                                keylist += ", "
                                valuelist += ", "
                            #Add Column Name to List
                            keylist += key + "_" + interiorkey

                            if type(interiorvalue) == bool:
                                if value is True:
                                    valuelist += "1"
                                else:
                                    valuelist += "0"
                    
                            if type(interiorvalue) == int:
                                valuelist += str(interiorvalue)
                            
                            if type(interiorvalue) == str:
                                valuelist += "'" + interiorvalue + "'"

        keylist += ")"
        valuelist += ")"

    sqlstatement += "INSERT INTO " + TableName + " " + keylist + " VALUES " + valuelist + ";" # + "\n"
    #print(sqlstatement)
    return(sqlstatement)
