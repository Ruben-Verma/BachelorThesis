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

filename = '/Users/rubenverma/Documents/Bachelorarbeit/1002378/workunit_1002378_vel8_mass0.01_MODUST_sepangle30_1.ctwu'
tabularData=[]

with open(filename, 'rb') as file:
    floatValues= np.array(np.fromfile(file, dtype=np.float32))

myCursor=con.cursor()
start = timer()

sqlParamList=[]

for i in range(0,len(floatValues),7):
    sqlParamList.append((floatValues[i].item(),floatValues[i+1].item(),floatValues[i+2].item(),floatValues[i+3].item(),floatValues[i+4].item(),floatValues[i+5].item(),floatValues[i+6].item()))


psycopg2.extras.execute_batch(myCursor,"INSERT INTO COMETS VALUES (%s,%s,%s, %s,%s,%s,%s)",sqlParamList,page_size=100000)

end = timer()
print(end - start)


