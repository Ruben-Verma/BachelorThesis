import psycopg2
from timeit import default_timer as timer
import numpy as np


try:
    con = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password = "mysecretpassword")
    print("Datenbank erfolgreich initialisiert")


except:
    print("Datenbank konnte nicht geladen werden")

print(con)
filename = '/Users/rubenverma/Documents/Bachelorarbeit/1002378/workunit_1002378_vel8_mass0.01_MODUST_sepangle30_1.ctwu'
tabularData=[]

with open(filename, 'rb') as f:
    floatValues= np.array(np.fromfile(f, dtype=np.float32))

len(floatValues)
start = timer()
id = 0

sqlString = ""


for i in np.arange(0,len(floatValues),7):
    con.cursor().execute("INSERT INTO particleComets VALUES (%s,%s,%s, %s,%s,%s,%s,%s)",
                         (floatValues[i].item(),
                          floatValues[i+1].item(),
                          floatValues[i+2].item(),
                          floatValues[i+3].item(),
                          floatValues[i+4].item(),
                          floatValues[i+5].item(),
                          floatValues[i+6].item(),
                          floatValues[7].item(),))
    if i % 1000:
        con.commit()
con.commit()
con.close
end = timer()
print(end - start)
