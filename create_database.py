import sqlite3


print("INITIATING CONNECTION TO DB.")
conn = sqlite3.connect('test.db')
c = conn.cursor()
print("SUCCESSFULLY CONNECTED TO DB.")

c.execute('''
          CREATE TABLE IF NOT EXISTS transactions
          ([betDataID] INTEGER PRIMARY KEY, 
          [timestamp] INTEGER,
          [customerID] INTEGER,
          [outcomeID] INTEGER,
          [transactionID] INTEGER,
          [price] REAL,
          [unitstake] REAL,
          [eventID] INTEGER,
          [eventDescription] TEXT,
          [marketDescription] TEXT,
          [outcomeDescription] TEXT,
          [subtypeDescription] TEXT
          )
          ''')

c.execute('''
          CREATE TABLE IF NOT EXISTS settlements
          ([betDataID] INTEGER, 
          [eventType] TEXT,
          [transactionID] INTEGER,
          [verdict] TEXT
          )
          ''')

c.execute('''
          CREATE TABLE IF NOT EXISTS withdrawals
          ([accountID] INTEGER, 
          [eventType] TEXT,
          [amount] REAL
          )
          ''')

print("CREATED DATABASE TABLES.")

conn.commit()
conn.close()
print("CLOSED CONNECTION TO DB.")
