import sqlite3 
import json

connection = sqlite3.connect("reta60Tournaments.db") 
crsr = connection.cursor() 

def printTables(docums):
    for row in docums:
        print(row[1])






sql_command="SELECT * FROM USER"
crsr.execute(sql_command)
ans = crsr.fetchall()  
printTables(ans)