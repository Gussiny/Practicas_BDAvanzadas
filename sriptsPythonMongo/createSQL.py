import sqlite3 

connection = sqlite3.connect("reta60Tournaments.db") 

crsr = connection.cursor() 


crsr.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='USER' ''')


def printTables(docums):
    for row in docums:
        print(row)

if crsr.fetchone()[0]==1 : 
	print('Table already exists.')
else :
    sql_command = """CREATE TABLE USER(  
    userId INTEGER PRIMARY KEY AUTOINCREMENT,  
    fname VARCHAR(20),  
    lname VARCHAR(30),  
    gender CHAR(1),  
    userType VARCHAR(30),
    birthDate DATE,
    joiningDate DATE DEFAULT(datetime('now','localtime')) );"""

    crsr.execute(sql_command)

    print("Table created")
    
    sql_command = """INSERT INTO user(fname, lname, gender, userType, birthDate) VALUES ("Fabian", "Ramirez", "M", "player", "1999-10-04");"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO user(fname, lname, gender, userType, birthDate) VALUES ("Gustavo", "Flores", "M", "player", "1998-01-14");"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO user(fname, lname, gender, userType, birthDate) VALUES ("Erick", "Mendoza", "M", "player", "1999-06-23");"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO user(fname, lname, gender, userType, birthDate) VALUES ("Oscar", "Del Bull", "M", "player", "1999-07-26");"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO user(fname, lname, gender, userType, birthDate) VALUES ("Sergio", "Sgioma", "M", "admin", "1987-05-19");"""
    crsr.execute(sql_command) 




sql_command="SELECT * FROM USER"
crsr.execute(sql_command)
ans = crsr.fetchall()  
printTables(ans)

sql_command="PRAGMA foreign_keys = ON;"
crsr.execute(sql_command)


crsr.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='TOURNAMENT' ''')
if crsr.fetchone()[0]==1 : 
	print('Table already exists TOURNAMENT.')
else :
    print("Table TOURNAMENT MISS")
    sql_command = """CREATE TABLE TOURNAMENT(  
    tournamentId INTEGER PRIMARY KEY AUTOINCREMENT,  
    tName VARCHAR(20),  
    tMode CHAR(1),  
    tGender CHAR(1),
    startDate DATE,
    userId INTEGER NOT NULL,
    tCreation DATE DEFAULT(datetime('now','localtime')),
    FOREIGN KEY(userId) REFERENCES user(userId) );"""

    crsr.execute(sql_command)

    print("Table TOURNAMENT CREATED")
    sql_command = """INSERT INTO TOURNAMENT(tName, tMode, tGender, startDate, userId) VALUES ("Abierto Tec 2020", "S", "M", "2020-05-22", 5);"""
    crsr.execute(sql_command) 





sql_command="SELECT * FROM TOURNAMENT"
crsr.execute(sql_command)
ans = crsr.fetchall()  
print(ans)







sql_command="SELECT * FROM TOURNAMENT NATURAL JOIN USER"
crsr.execute(sql_command)
ans = crsr.fetchall()  
printTables(ans)





crsr.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='REGISTRATIONS' ''')
if crsr.fetchone()[0]==1 : 
	print('Table already exists REGISTRATIONS.')
else :
    print("Table REGISTRATIONS MISS")
    sql_command = """CREATE TABLE REGISTRATIONS(  
    registrationId INTEGER PRIMARY KEY AUTOINCREMENT,  
    userId INTEGER NOT NULL,  
    tournamentId INTEGER NOT NULL,
    joiningDate DATE DEFAULT(datetime('now','localtime')),
    status VARCHAR(20) DEFAULT "regular",
    FOREIGN KEY(userId) REFERENCES user(userId),
    FOREIGN KEY(tournamentId) REFERENCES TOURNAMENT(tournamentId) );"""
    crsr.execute(sql_command)
    print("Table REGISTRATIONS created")

    sql_command = """INSERT INTO REGISTRATIONS(userId, tournamentId) VALUES (1, 1);"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO REGISTRATIONS(userId, tournamentId) VALUES (2, 1);"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO REGISTRATIONS(userId, tournamentId) VALUES (3, 1);"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO REGISTRATIONS(userId, tournamentId) VALUES (4, 1);"""
    crsr.execute(sql_command) 



sql_command="SELECT tName, REGISTRATIONS.userId, USER.fName, status FROM TOURNAMENT JOIN  REGISTRATIONS JOIN USER ON REGISTRATIONS.userId=USER.userId"
crsr.execute(sql_command)
ans = crsr.fetchall()  
printTables(ans)


crsr.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='MATCH' ''')
if crsr.fetchone()[0]==1 : 
	print('Table already exists MATCH.')
else :
    print("Table MATCH MISS")
    sql_command = """CREATE TABLE MATCH(  
    matchId INTEGER PRIMARY KEY AUTOINCREMENT,  
    userId INTEGER NOT NULL,  
    user2Id INTEGER NOT NULL,
    tournamentId INTEGER NOT NULL,
    creationDate DATE DEFAULT(datetime('now','localtime')),
    matchDate DATETIME,
    status VARCHAR(20) DEFAULT "regular",
    round INTEGER NOT NULL,
    winnerId INTEGER NOT NULL DEFAULT "",
    FOREIGN KEY(winnerId) REFERENCES user(userId),
    CONSTRAINT "player1" FOREIGN KEY(userId) REFERENCES user(userId),
    CONSTRAINT "player2" FOREIGN KEY(user2Id) REFERENCES user(userId),
    FOREIGN KEY(tournamentId) REFERENCES TOURNAMENT(tournamentId) );"""
    
    crsr.execute(sql_command)
    print("Table MATCH created")

    sql_command = """INSERT INTO MATCH(matchId, userId, user2Id, tournamentId, matchDate, round) VALUES (101, 1, 2, 1, '2020-05-23 14:00:00', 1);"""
    crsr.execute(sql_command) 





crsr.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='SETS' ''')
if crsr.fetchone()[0]==1 : 
	print('Table already exists SETS.')
else :
    print("Table SETS MISS")

    sql_command = """CREATE TABLE SETS(  
    setId INTEGER PRIMARY KEY AUTOINCREMENT,  
    user1Points INTEGER NOT NULL DEFAULT 0,  
    user2Points INTEGER NOT NULL DEFAULT 0,
    numSet INTEGER NOT NULL,
    matchId INTEGER NOT NULL,
    FOREIGN KEY(matchId) REFERENCES MATCH(matchId));"""
    
    crsr.execute(sql_command)
    print("Table SETS created")


    sql_command = """INSERT INTO SETS(user1Points, user2Points, numSet, matchId) VALUES (6, 4, 1, 101);"""
    crsr.execute(sql_command) 
    sql_command = """INSERT INTO SETS(user1Points, user2Points, numSet, matchId) VALUES (6, 0, 2, 101);"""
    crsr.execute(sql_command) 



    
sql_command="SELECT * FROM MATCH"
crsr.execute(sql_command)
ans = crsr.fetchall()  
printTables(ans)









connection.commit() 
connection.close() 



