import psycopg2
from timeit import default_timer as timer
import numpy as np
import psycopg2.extras as extras
import os

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

print(con)

filename = '/Users/rubenverma/Documents/Bachelorarbeit/1002378/workunit_1002378_vel8_mass4.39e-08_MODUST_sepangle52_75.ctwu'
tabularData=[]

with open(filename, 'rb') as file:
    floatValues= np.array(np.fromfile(file, dtype=np.float32))

myCursor=con.cursor()
start = timer()






sqlString2 = []
print(len(floatValues)/100)
print(len(floatValues))
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


sqlString = ""


myCursor.execute("INSERT INTO COMETS VALUES" + s[0:-1])

end = timer()
print(end - start)


