import sqlite3 
import json
from datetime import datetime
from pymongo import MongoClient


client = MongoClient('mongodb+srv://ShyGuy118:fg1511973@cluster0-lqqq1.mongodb.net/test?retryWrites=true&w=majority')
db = client

connection = sqlite3.connect("reta60Tournaments.db") 
crsr = connection.cursor() 

def printTables(documents):
    for row in documents:
        for field in row:
            print(field)
    
def insertUser(db, _id, fname, lname, gender, userType, birthday, joiningDate):
    db.users.insert({ "_id": _id,"fname": fname, "lname": lname, "gender": gender, "userType": userType, "birthdate": datetime.strptime(birthday, '%Y-%m-%d'), "joiningDate": datetime.strptime(joiningDate, '%Y-%m-%d %H:%M:%S')})



def readAndInsertUsers(db):
    sql_command="SELECT * FROM USER"
    crsr.execute(sql_command)
    documents = crsr.fetchall()  
    #print(documents)
    fields = list(map(lambda x: x[0], crsr.description))
    for row in documents:
        document={}
        i=0
        for field in row:
            if field=='birthDate':
                field=datetime.strptime(field, '%Y-%m-%d')
            elif field=='joiningDate':
                field=datetime.strptime(field, '%Y-%m-%d %H:%M:%S')
            document[fields[i]]=field
            i+=1
        db.users.insert(document)
        print(document)
        print("1 new user inserted")
            

        
       

   

        
    
    
           

        
       
            

    

    




def main():
    db = client.reta60Mongo
    readAndInsertUsers(db)

    
  
    
    #cierra cluster mongo
    client.close() 
    #cierra y guarda base de datos local
    connection.commit() 
    connection.close() 


main()




