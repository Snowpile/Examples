import re
import sys
from bs4 import BeautifulSoup as bs4
import json
import time
import pyodbc
import datetime
import requests
import yfinance as yf


cxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=0.0.0.0,1434;UID=uid;PWD=pwd;Database=db", autocommit=True)
csr = cxn.cursor()

def getTickers():
    # Populate the ticker (stock) and ETF lists below from SQL Server. What Tickers are missing in each?
    csr.execute("SELECT DISTINCT TickerName FROM Nibblonian.dbo.Ticker WHERE TickerType = 'Stock'")
    tickers = [row[0] for row in csr.fetchall()]
    print(tickers)
    return tickers

def getOptionsContracts(tickers):
    # (1) Get all options contracts that currently exist in the SQL Server and have not expired
    # "optionsContracts" Stucture: [[eD1, [C11, ...]], [eD2, [C12, ...]], ..., [eD3, [C13, ...]]]
    optionsContracts, existingContracts = [], ''
    previousFri = datetime.date.today() + datetime.timedelta((4 - datetime.date.today().weekday()) % 7) - datetime.timedelta(days=7)
    query = f"""SELECT DISTINCT ContractName
                    FROM Nibblonian.dbo.Ticker as A
                    LEFT JOIN Nibblonian.dbo.Options as B on A.TickerKey = B.TickerKey
                    WHERE TickerType = 'Stock'
                        and B.[ExpireDate] > '{previousFri}'"""
    csr.execute(query)
    data = csr.fetchall()
    for row in data:
        optionsContracts.append(row[0])
        existingContracts += row[0]
    # (2) Get any contracts not already in the SQL Server
    contractsNotInSQL = []
    for t, ticker in enumerate(tickers):
        ticker = yf.Ticker(ticker)
        expireDates = [[x, int(datetime.datetime(int(x[:4]), int(x[:7][-2:]), int(x[-2:]), 0, 0).timestamp())] for x in ticker.options]
        for d, date in enumerate(expireDates):
            chain = ticker.option_chain(date[0])
            calls, puts = chain.calls, chain.puts
            [contractsNotInSQL.append(c[1].contractSymbol) for c in calls.iterrows() if c[1].contractSymbol not in existingContracts]
            [contractsNotInSQL.append(c[1].contractSymbol) for c in puts.iterrows() if c[1].contractSymbol not in existingContracts]

    # (2.0) INSERT all options contracts that are not already in the SQL Server
    # (2.1) Get all the options that don't exist in the SQL Server yet
    optionsNotInSQL, startTime = [], datetime.datetime.now()
    for i, option in enumerate(contractsNotInSQL):
        firstInt = re.search(r"\d", option).start()
        contractName = option
        contractType = option[len(option[:firstInt]) + 6:][0]
        strikePrice = option[len(option[:firstInt]) + 7:]
        optionsNotInSQL.append([contractName, contractType, strikePrice, date[0], contractName[:firstInt]])
        optionsContracts.append(contractName)
    print(f"Options not in SQL yet: {str(len(optionsNotInSQL))}")
    # (1.2) INSERT the missing Options Contracts into SQL
    optionsNotInSQLCount = 0
    for option in optionsNotInSQL:
        # Add the new Options Contract to SQL Server
        print(f"\t\tDoesn't exist! Ingest Contract: {option}")
        query = f"""INSERT INTO Nibblonian.dbo.Options
                    SELECT A.TickerKey, A.ContractName, A.ContractType, A.StrikePrice, A.ExpireDate, A.LoadDateTime
                    FROM (
                        SELECT TickerKey = (SELECT TickerKey FROM dbo.Ticker WHERE TickerName = '{option[4]}')
                            ,ContractName = '{option[0]}'
                            ,ContractType = '{option[1]}'
                            ,StrikePrice = '{option[2][:-3]}.{option[2][-3:]}'
                            ,ExpireDate = '{option[3]}'
                            ,LoadDateTime = GETDATE()) as A
                    LEFT JOIN Nibblonian.dbo.Options as B
                        on A.ContractName = B.ContractName
                    WHERE B.TickerKey is NULL"""
        csr.execute(query)
        optionsNotInSQLCount +=1
    print(f"\tINSERTed {str(optionsNotInSQLCount)} new options into SQL.")
    return optionsContracts, existingContracts

def getOptionsData():
    startTime = datetime.datetime.now()
    # Sleep for 0.5 seconds between candlestick graph data hits. Don't want to hit Yahoo's servers too often.
    sleepTime = 0.25

    # Get the Option Contract Expire dates. Always going 3 years behind or 1 day. Fix this eventually to match that logic.
    monday = datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday())
    mondayLong = int((datetime.datetime(monday.year, monday.month, monday.day, 0, 0, 0) - datetime.datetime(1970,1,1)).total_seconds())
    friday = datetime.date.today() + datetime.timedelta((4 - datetime.date.today().weekday()) % 7)
    fridayLong = int((datetime.datetime(friday.year, friday.month, friday.day, 0, 0, 0) - datetime.datetime(1970,1,1)).total_seconds())
    # expireDate = [string, ex: '2021-10-01', Next/Current Friday @ 00:00:00.000, 3 Years ago Long, period2, period1 if ran before Friday]
    # Replace "mondayLong" with "fridayLong" if ran Thursday night or Friday evening, Friday + 86400 to pull all data existing within Friday
    expireDate = (str(friday), fridayLong, fridayLong - 94608000, fridayLong + 86400, mondayLong)
    print(expireDate)

    # () Get the Contracts list to get data for
    query = f"""SELECT A.TickerName, B.ContractName, LastDateWithData = ISNULL(MAX(LongDate) - 86400, {expireDate[2]})
                FROM Nibblonian.dbo.Ticker as A
                LEFT JOIN Nibblonian.dbo.Options as B on A.TickerKey = B.TickerKey
                LEFT JOIN Nibblonian.dbo.CandleData as C on B.OptionsKey = C.OptionsKey
                WHERE TickerType = 'Stock'
                    and B.[ExpireDate] > '{datetime.date.today() + datetime.timedelta((4 - datetime.date.today().weekday()) % 7) - datetime.timedelta(days=7)}'
                GROUP BY A.TickerName, B.ContractName
                ORDER BY B.ContractName"""
    csr.execute(query)
    optionsData = [[x[0], x[1], x[2]] for x in csr.fetchall()]

    # (2) Get the Options Contract Daily stats [Open, High, Volume, Low, Close]
    # testContract = 'AMC211001C00010000'
    # oCl = [testContract]
    for contract in optionsData:
        urlChartHeaders = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            # cookie: APID=VB87b22320-d96f-11e7-84e4-06944911f706; B=5s5povdd2c5r7&b=3&s=qs; A1=d=AQABBJqeM14CEA7fuRtemlldXen0ayNx6b8FEgEBBAFCS2E0YtxT0iMA_eMAAAcIZxcmWvs4F14&S=AQAAAlo7WmGjUXxyWIH5MsE4wGE; A3=d=AQABBJqeM14CEA7fuRtemlldXen0ayNx6b8FEgEBBAFCS2E0YtxT0iMA_eMAAAcIZxcmWvs4F14&S=AQAAAlo7WmGjUXxyWIH5MsE4wGE; A1S=d=AQABBJqeM14CEA7fuRtemlldXen0ayNx6b8FEgEBBAFCS2E0YtxT0iMA_eMAAAcIZxcmWvs4F14&S=AQAAAlo7WmGjUXxyWIH5MsE4wGE&j=US; GUC=AQEBBAFhS0JiNEIftARu; cmp=t=1633012172&j=0; PRF=t%3DAMC211001P00053000%252BAMC211001P00051000%252BAMC%252BAMC211001C00018000%252BAMC211001P00054000%252BAMC211001C00024000%252BAMC211001C00010000%252BAMC211001C00039000%26qct%3Dcandle; APIDTS=1633014484",
            "origin": "https://finance.yahoo.com",
            "referer": f"https://finance.yahoo.com/quote/{contract[0]}/chart?p={contract[0]}",
            "sec-ch-ua": '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"}
        # url = f"https://query1.finance.yahoo.com/v8/finance/chart/{oCN[0]}?region=US&lang=en-US&includePrePost=false&interval=1d&useYfid=true&range=max&corsDomain=finance.yahoo.com&.tsrc=finance"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{contract[1]}?symbol={contract[1]}&period1={contract[2]}&period2={expireDate[3]}&useYfid=true&interval=1d&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=kCq4XSwo.Ug&corsDomain=finance.yahoo.com"
        res = requests.get(url, headers=urlChartHeaders)
        if res.status_code == 200:
            data = res.json()
            timeList, openList, highList, volumeList, closeList, lowList = [], [], [], [], [], []
            try:
                for n, item in enumerate(data["chart"]["result"][0]["timestamp"]):
                    timeList.append(data["chart"]["result"][0]["timestamp"][n])
                    openList.append(data["chart"]["result"][0]["indicators"]["quote"][0]["open"][n])
                    highList.append(data["chart"]["result"][0]["indicators"]["quote"][0]["high"][n])
                    volumeList.append(data["chart"]["result"][0]["indicators"]["quote"][0]["volume"][n])
                    closeList.append(data["chart"]["result"][0]["indicators"]["quote"][0]["close"][n])
                    lowList.append(data["chart"]["result"][0]["indicators"]["quote"][0]["low"][n])
            except:
                print(f"\t\tNo data? Double check if no data exists for: {contract[1]}")
                continue
            # (2.2) Put the Options Contract candlestick data into SQL (Only if doesn't Exist else UPATE)
            for tIdx, data in enumerate(timeList):
                query = f"""INSERT INTO Nibblonian.dbo.CandleData
                            SELECT A.OptionsKey, A.LongDate, A.DayDate, A.[Open], A.High, A.Volume, A.Low, A.[Close], A.LoadDateTime
                            FROM (
                                SELECT OptionsKey = (SELECT OptionsKey FROM Nibblonian.dbo.Options WHERE ContractName = '{contract[1]}')
                                    ,LongDate = '{data}'
                                    ,DayDate = '{str(datetime.datetime.fromtimestamp(data))[:10]}'
                                    ,[Open] = {"NULL" if openList[tIdx] is None else openList[tIdx]}
                                    ,High = {"NULL" if highList[tIdx] is None else highList[tIdx]}
                                    ,Volume = {"NULL" if volumeList[tIdx] is None else int(volumeList[tIdx])}
                                    ,Low = {"NULL" if lowList[tIdx] is None else lowList[tIdx]}
                                    ,[Close] = {"NULL" if closeList[tIdx] is None else closeList[tIdx]}
                                    ,LoadDateTime = GETDATE()) as A
                            LEFT JOIN Nibblonian.dbo.CandleData as B
                                on A.OptionsKey = B.OptionsKey
                                and A.LongDate = B.LongDate
                            WHERE B.OptionsKey is NULL

                            UPDATE A
                            SET A.[Open] = B.[Open], A.High = B.High, A.Volume = B.Volume, A.Low = B.Low, A.[Close] = B.[Close], A.LoadDateTime = GETDATE()
                            FROM Nibblonian.dbo.CandleData as A
                            LEFT JOIN (
                                SELECT OptionsKey = (SELECT OptionsKey FROM Nibblonian.dbo.Options WHERE ContractName = '{contract[1]}')
                                    ,LongDate = '{data}'
                                    ,DayDate = '{str(datetime.datetime.fromtimestamp(data))[:10]}'
                                    ,[Open] = {"NULL" if openList[tIdx] is None else openList[tIdx]}
                                    ,High = {"NULL" if highList[tIdx] is None else highList[tIdx]}
                                    ,Volume = {"NULL" if volumeList[tIdx] is None else volumeList[tIdx]}
                                    ,Low = {"NULL" if lowList[tIdx] is None else lowList[tIdx]}
                                    ,[Close] = {"NULL" if closeList[tIdx] is None else closeList[tIdx]}
                                ) as B
                                on A.OptionsKey = B.OptionsKey
                                and A.LongDate = B.LongDate
                            WHERE A.[Open] <> B.[Open] or A.High <> B.High or A.Volume <> B.Volume or A.Low <> B.Low or A.[Close] <> B.[Close]"""
                csr.execute(query)
        else:
            print(f"\t{res.status_code} {res.content}")
            print(f"\tError getting Options Contracts from url: {url}")
            # sys.exit(-1)
        # (3) Sleep so it doesn't hit Yahoo's server too much
        time.sleep(sleepTime)
        print(f"Time to load Options data for {contract[1]}: {str(datetime.datetime.now() - startTime)}")
    print(f"Overall time: {str(datetime.datetime.now() - startTime)}")

def updateCSV():
    query = f"""SELECT A.TickerName
                    ,B.ContractType
                    ,B.ContractName
                    ,B.[ExpireDate]
                    ,C.DayDate
                    ,C.Volume
                FROM dbo.Ticker as A
                LEFT JOIN dbo.Options as B on A.TickerKey = B.TickerKey
                LEFT JOIN dbo.CandleData as C on B.OptionsKey = C.OptionsKey
                WHERE Volume is not NULL
                ORDER BY A.TickerName, B.ContractName, C.DayDate"""
    csr.execute(query)
    path = "G:/My Drive/DFRDD/data.csv"
    file = open(path, 'w+')
    file.write("TickerName|ContractType|ContractName|ExpireDate|TradeDate|Volume\n")
    for x in csr.fetchall():
        file.write(f"{x[0]}|{x[1]}|{x[2]}|{x[3]}|{x[4]}|{x[5]}\n")
    file.close()

# Are we going to do anything with ETFs? If so, what?
# Comment out a function to run
# tickers = getTickers()
# optionsContracts, existingContracts = getOptionsContracts(tickers)
getOptionsData()
# updateCSV()
