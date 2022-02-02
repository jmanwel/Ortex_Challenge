import csv
import sqlite3
from sqlite3 import Error

def sql_connection():
    try:
        con = sqlite3.connect(':memory:')
        print("Connection is established: Database is created in memory")
        cur = con.cursor()
        cur.execute("CREATE TABLE t (transactionID,gvkey,companyName,companyISIN,companySEDOL,insiderID,insiderName,insiderRelation,insiderLevel,connectionType,connectedInsiderName,connectedInsiderPosition,transactionType,transactionLabel,iid,securityISIN,securitySEDOL,securityDisplay,assetClass,shares,inputdate,tradedate,maxTradedate,price,maxPrice,value,currency,valueEUR,unit,correctedTransactionID,source,tradeSignificance,holdings,filingURL,exchange);")

        with open('C:\Ortex\Programming_Excercise\\2017.csv', newline='', encoding="utf8") as File:            
            dr = csv.DictReader(File)          
            to_db = [(i['transactionID'],i['gvkey'],i['companyName'],i['companyISIN'],i['companySEDOL'],i['insiderID'],i['insiderName'],i['insiderRelation'],i['insiderLevel'],i['connectionType'],i['connectedInsiderName'],i['connectedInsiderPosition'],i['transactionType'],i['transactionLabel'],i['iid'],i['securityISIN'],i['securitySEDOL'],i['securityDisplay'],i['assetClass'],i['shares'],i['inputdate'],i['tradedate'],i['maxTradedate'],i['price'],i['maxPrice'],i['value'],i['currency'],i['valueEUR'],i['unit'],i['correctedTransactionID'],i['source'],i['tradeSignificance'],i['holdings'],i['filingURL'],i['exchange']) for i in dr]
            
        cur.executemany("INSERT INTO t (transactionID,gvkey,companyName,companyISIN,companySEDOL,insiderID,insiderName,insiderRelation,insiderLevel,connectionType,connectedInsiderName,connectedInsiderPosition,transactionType,transactionLabel,iid,securityISIN,securitySEDOL,securityDisplay,assetClass,shares,inputdate,tradedate,maxTradedate,price,maxPrice,value,currency,valueEUR,unit,correctedTransactionID,source,tradeSignificance,holdings,filingURL,exchange) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
        con.commit()
        
        #What Exchange has had the most transactions in the file? 
        # cur.execute("SELECT COUNT(*), exchange FROM t WHERE exchange != 'off exchange' GROUP BY exchange ORDER BY COUNT(*) DESC LIMIT 1")        
        # rows = cur.fetchall()
        # for row in rows:
        #     print(row)
        # ANSWER:  (3146, 'LSE (UK)')

        #In August 2017, which companyName had the highest combined valueEUR?

        # cur.execute("SELECT companyName, valueEUR, inputdate FROM t WHERE inputdate like '201708%' ORDER BY valueEUR DESC LIMIT 1")        
        
        # rows = cur.fetchall()
        # for row in rows:
        #     print(row)
        # ANSWER: ('BOSSARD HOLDING AG', '89767.3901', '20170829')

        #For 2017, only considering transactions with tradeSignificance 3, what is the percentage of transactions per month?
        # cur.execute("SELECT COUNT(*) FROM t WHERE inputdate like '2017%' AND tradeSignificance = '3'")
        # rows = cur.fetchall()
        # for row in rows:
        #     total2017 = row[0]
        
        # listMont =[{'month':'jan', 'id':'01'},{'month':'feb', 'id':'02'},{'month':'mar', 'id':'03'},{'month':'apr', 'id':'04'},{'month':'may', 'id':'05'},{'month':'jun', 'id':'06'},{'month':'jul', 'id':'07'},{'month':'aug', 'id':'08'},{'month':'sept', 'id':'09'},{'month':'oct', 'id':'10'},{'month':'nov', 'id':'11'},{'month':'dec', 'id':'12'}]
        
        # totalPerMonth = []
        # for lm in listMont:
        #     trans = {}
        #     trans['month'] = lm['month']
        #     cur.execute(f"SELECT COUNT(*) FROM t WHERE inputdate like '2017{lm['id']}%' AND tradeSignificance = '3'")
        #     rows = cur.fetchall()
        #     for row in rows:
        #         total = row[0]
        #     trans['per'] = f"{round(100*(total/total2017),2)}%"            
        #     totalPerMonth.append(trans)

        # for t in totalPerMonth:
        #     print(f"{t['month']} => {t['per']}")

        # ANSWER:
        # jan => 7.77%
        # feb => 7.76%
        # mar => 12.05%
        # apr => 7.2%
        # may => 11.06%
        # jun => 1.4%
        # jul => 0.1%
        # aug => 7.67%
        # sept => 12.0%
        # oct => 11.71%
        # nov => 10.96%
        # dec => 10.34%
         
        con.close()
    except Error:
        print(Error)
    finally:
        con.close()

sql_connection()
