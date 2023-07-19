
CREATE TABLE transactions
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
          );


CREATE TABLE withdrawals
          ([accountID] INTEGER, 
          [eventType] TEXT,
          [amount] REAL
          );         
         
 CREATE TABLE settlements
          ([betDataID] INTEGER, 
          [eventType] TEXT,
          "transactionID" INTEGER,
          [verdict] TEXT
          );        
         

---------------------------------------------------
-- SQL queries for CSV files

SELECT t.customerID, sum(t.price) as sum_loss FROM settlements s 
join transactions t on s.betDataID = t.betDataID
where s.verdict like 'L'
and t.customerID not like '123456789'
group by t.customerID;

SELECT t.customerID, sum(t.price) as sum_win FROM settlements s 
join transactions t on s.betDataID = t.betDataID
where s.verdict like 'W'
and t.customerID not like '123456789'
group by t.customerID;


SELECT t.customerID, sum(w.amount) as sum_withdraw FROM withdrawals w 
join transactions t on w.accountID = t.customerID
and t.customerID not like '123456789'
group by t.customerID;
         