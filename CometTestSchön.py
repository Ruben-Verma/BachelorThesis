import os
import psycopg2
from timeit import default_timer as timer
import numpy as np
import psycopg2.extras as extras


def sqlStringMaker(byteArray):
    sqlString2 = []
    for i in range(0, len(byteArray), 7):
        sqlString2.append("(")
        sqlString2.append(str(byteArray[i]))
        sqlString2.append(",")
        sqlString2.append(str(byteArray[i + 1]))
        sqlString2.append(",")
        sqlString2.append(str(byteArray[i + 2]))
        sqlString2.append(",")
        sqlString2.append(str(byteArray[i + 3]))
        sqlString2.append(",")
        sqlString2.append(str(byteArray[i + 4]))
        sqlString2.append(",")
        sqlString2.append(str(byteArray[i + 5]))
        sqlString2.append(",")
        sqlString2.append(str(byteArray[i + 6]))
        sqlString2.append(")")
        sqlString2.append(",")
    return "".join(sqlString2)

def insertComet(connection, path):
    totalTime = 0
    myCursor = connection.cursor()
    cometNumber = 1
    fileList = []
    with os.scandir(path) as it:
        for entry in it:
            if entry.name.endswith(".ctwu") and entry.is_file():
                fileList.append(entry.path)

    fileList.sort()

    for path in fileList:
        start = timer()
        with open(path, 'rb') as file:
            floatValues = np.array(np.fromfile(file, dtype=np.float32))
        s = sqlStringMaker(floatValues)
        end = timer()
        print(cometNumber)
        cometNumber = cometNumber + 1
        myCursor.execute("INSERT INTO COMETS VALUES  " + s[0:-1])
        end = timer()
        totalTime += (end - start)
        print(end - start)
    print(totalTime)

def searchTime(connection,time1,time2):
    myCursor = connection.cursor()
    start = timer()
    myCursor.execute("SELECT * FROM COMETS WHERE particestate_vz BETWEEN %s AND %s ORDER BY particestate_vz ASC",(time1,time2))
    result = myCursor.fetchall()
    end = timer()
    print(end - start)
    return result

def createIndexOnTime(connection):
    myCursor = connection.cursor()
    start = timer()
    myCursor.execute("CREATE INDEX Zeitindex on Comets(particestate_vz)")
    end = timer()
    print(end - start)

def dropIndex(connection):
    myCursor = connection.cursor()
    start = timer()
    myCursor.execute("Drop Index Zeitindex")
    end = timer()
    print(end - start)

def setSharedBuffers(connection,memory):
    myCursor= connection.cursor()
    myCursor.execute("ALTER SYSTEM SET shared_buffers to 1024")

def changeSystemConfiguration(connection):
    myCursor = connection.cursor()
    myCursor.execute("ALTER SYSTEM SET max_connections = '200' ")
    myCursor.execute("ALTER SYSTEM SET shared_buffers = '2GB' ")
    myCursor.execute("ALTER SYSTEM SET effective_cache_size = '6GB' ")
    myCursor.execute("ALTER SYSTEM SET maintenance_work_mem = '512MB' ")
    myCursor.execute("ALTER SYSTEM SET checkpoint_completion_target = '0.9' ")
    myCursor.execute("ALTER SYSTEM SET wal_buffers = '16MB' ")
    myCursor.execute("ALTER SYSTEM SET default_statistics_target = '100' ")
    myCursor.execute("ALTER SYSTEM SET random_page_cost = '1.1'")
    myCursor.execute("ALTER SYSTEM SET work_mem = '5242kB' ")
    myCursor.execute("ALTER SYSTEM SET min_wal_size = '1GB' ")
    myCursor.execute("ALTER SYSTEM SET max_wal_size = '4GB' ")

def createGINIndexOnTime(connection):
    myCursor = connection.cursor()
    start = timer()
    myCursor.execute("CREATE INDEX Zeitindex on Comets USING GIN(particestate_vz)")
    end = timer()
    print(end - start)

def createBRINIndexOnTime(connection):
    myCursor = connection.cursor()
    start = timer()
    myCursor.execute("CREATE INDEX Zeitindex on Comets USING BRIN(particestate_vz)")
    end = timer()
    print(end - start)

try:
    con = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword")
    con.autocommit = True
    print("Datenbank erfolgreich initialisiert")


except:
    print("Datenbank konnte nicht geladen werden")


dropIndex(con)
myCursor = con.cursor()
myCursor.execute("EXPLAIN ANALYZE SELECT * FROM COMETS WHERE particestate_vz BETWEEN %s AND %s ORDER BY particestate_vz ASC",(2316962600,2524599300))
for elements in myCursor.fetchall():
    print(elements)

#insertComet(con,"/Users/rubenverma/Documents/Bachelorarbeit/1002378")
