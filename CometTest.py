import os
import psycopg2
from timeit import default_timer as timer
import numpy as np
import psycopg2.extras as extras

try:
    con = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password = "mysecretpassword")
    con.autocommit = True
    print("Datenbank erfolgreich initialisiert")


except:
    print("Datenbank konnte nicht geladen werden")



def insertComet(connection,path):
    totalTime=0
    myCursor = connection.cursor()
    cometNumber=1
    fileList=[]
    with os.scandir(path) as it:
        for entry in it:
            if entry.name.endswith(".ctwu") and entry.is_file():
                fileList.append(entry.path)

    fileList.sort()

    for path in fileList:
        start = timer()
        with open(path, 'rb') as file:
            floatValues= np.array(np.fromfile(file, dtype=np.float32))
        sqlString2 = []
        for i in range(0,len(floatValues),7):
            sqlString2.append("(")
            sqlString2.append(str(floatValues[i]))
            sqlString2.append(",")
            sqlString2.append(str(floatValues[i+1]))
            sqlString2.append(",")
            sqlString2.append(str(floatValues[i+2]))
            sqlString2.append(",")
            sqlString2.append(str(floatValues[i+3]))
            sqlString2.append(",")
            sqlString2.append(str(floatValues[i+4]))
            sqlString2.append(",")
            sqlString2.append(str(floatValues[i+5]))
            sqlString2.append(",")
            sqlString2.append(str(floatValues[i+6]))
            sqlString2.append(")")
            sqlString2.append(",")
        s = "".join(sqlString2)
        end = timer()
        print(cometNumber)
        cometNumber = cometNumber + 1
        myCursor.execute("INSERT INTO COMETS VALUES" + s[0:-1])
        end = timer()
        totalTime += (end-start)
        print(end - start)
    print(totalTime)

insertComet(con,"/Users/rubenverma/Documents/Bachelorarbeit/1002378")
