from tkinter import *
import tkinter as tk
import tkinter.ttk as ttk
import tkinter.messagebox as tkmb
import threading
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)
import pandas
import numpy
import pyodbc
from pytrends.request import TrendReq
import json
import random
import os
import sys
import math
import hmac
import hashlib
import datetime
import time
import requests
import base64
from requests.auth import AuthBase


# Create custom authentication for Exchange
class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = signature.digest().encode('base64').rstrip('\n')

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request

class CryptoAutomatedTrader(tk.Frame):

    def __init__(self, parent):
        tk.Frame.__init__(self, parent)
        self.parent = parent
        self.initializeGUI()

    def initializeGUI(self):
        """
        Creates a graphic user interface which allows the user to manage all of the Automated Crypto Bot variables.
        """

        # Rules for how resizing the GUI changes the rows and columns within
        self.parent.title("[Nibbler (NIB)] Non-Typical Investment Bot")
        # self.parent.grid_rowconfigure(1, weight=1)
        # self.parent.grid_columnconfigure(0, weight=1)

        # Menubar items
        menu = Menu(self.parent)
        self.parent.config(menu=menu)
        environmentMenu = Menu(menu)
        changeMarketVariablesMenu = Menu(menu)
        displayDataMenu = Menu(menu)
        helpMenu = Menu(menu)
        menu.add_cascade(label='Environment', menu=environmentMenu)
        menu.add_cascade(label='Variables', menu=changeMarketVariablesMenu)
        menu.add_cascade(label='Display Info', menu=displayDataMenu)
        menu.add_cascade(label='Help', menu=helpMenu)
        menu.add_command(label='About')  # , command=lambda: self.function(variables))
        environmentMenu.add_command(label='Testing')  # , command=lambda: self.function(variables))
        environmentMenu.add_command(label='Production')  # , command=lambda: self.function(variables))
        changeMarketVariablesMenu.add_command(label='TBD')  # , command=lambda: self.function(variables))
        displayDataMenu.add_command(label='TBD')  # , command=lambda: self.function(variables))
        helpMenu.add_command(label='TBD')  # , command=lambda: self.function(variables))

        # Initialize "Global" Variables
        self.cryptDict = {}
        self.cbServer = StringVar()
        # Set "Global" Variables
        self.cryptDict["products"], self.cryptDict["history"], self.cryptDict["orderBook"], self.cryptDict["tradeRules"] = [], [], [], []
        cbAPIK = 'key'
        cbAPIS = 'secret'
        cbAPIP = 'password'
        self.cbServer.set('https://api.pro.coinbase.com/')
        self.headers = {'Content-Type': 'application/json'}
        self.auth = CoinbaseExchangeAuth(cbAPIK, cbAPIS, cbAPIP)
        self.sqlConnection = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=0.0.0.0;UID=uid;PWD=pwd;Database=db", autocommit=True)
        self.sqlConnectionHR = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=0.0.0.0;UID=uid;PWD=pwd;Database=db", autocommit=True)
        self.sqlConnectionUP = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=0.0.0.0;UID=uid;PWD=pwd;Database=db", autocommit=True)
        self.sqlConnectionOM = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=0.0.0.0;UID=uid;PWD=pwd;Database=db", autocommit=True)
        self.regularCursor = self.sqlConnection.cursor()
        self.cursorHR = self.sqlConnectionHR.cursor()
        self.cursorUP = self.sqlConnectionUP.cursor()
        self.cursorOM = self.sqlConnectionOM.cursor()

        # GUI-Top: Options to manipulate the left and right Tree-Views below
        self.placeholder = Label(text="Existing Possibilities:")
        self.placeholder.grid(row=0, column=0, sticky=W)
        self.fromPurchase = Label(text='From Purchase:')
        self.fromPurchase.grid(row=0, column=6, sticky=W)

        # The Left Treeview will be the list of Market Options. There Will be a trigger so when one is clicked the middle Treeview changes for the selected market!
        columnsTreeviewLeft = ('#1')
        self.treeLeft = ttk.Treeview(self.parent, columns=columnsTreeviewLeft, show='headings')
        self.treeLeft.heading('#1', text='Market Options', anchor=tk.W)
        self.treeLeft.column('#1', stretch=YES, width=70)
        self.treeLeft.grid(row=1, column=0, rowspan=6, sticky=NSEW)
        self.treeLeft.bind('<Double-Button-1>', self.populateMarket)

        # GUI-Middle: Dynamic list depending on selection from options above?! Should ALWAYS be populated with all available options DESC by some column?!
        # Maybe make a buton to show existing OrderBook data for a specific Market?
        columnsTreeviewMid = ('#1', '#2', "#3")
        self.treeMid = ttk.Treeview(self.parent, columns=columnsTreeviewMid, show='headings')
        self.treeMid.heading('#1', text='Market', anchor=tk.W)
        self.treeMid.heading('#2', text='Trade Score')
        self.treeMid.heading('#3', text='Trade Amount')
        self.treeMid.column('#1', stretch=YES, width=90)
        self.treeMid.column('#2', stretch=YES, width=100, anchor=tk.E)
        self.treeMid.column('#3', stretch=YES, width=100, anchor=tk.E)
        # Best practice is to let the columnspan go 1 more than you expect to account for the scrollbar OR build that into the last column?!
        # Are we going to have some sort of rowspan here? Show what...... 5-10 cryptos? dependent of score value from column 2?
        self.treeMid.grid(row=1, column=1, rowspan=6, columnspan=3, sticky=NSEW)
        self.treeMid.bind('<Double-Button-1>', self.showProductGraph)
        self.scrollbarTMy = Scrollbar(self.parent)
        self.scrollbarTMy.grid(row=1, column=4, rowspan=6, sticky='NSE')  # 3 or 4?!
        self.scrollbarTMy.config(command=self.treeMid.yview)
        self.treeMid.config(yscrollcommand=self.scrollbarTMy.set)

        # GUI-Right: Active positions for the NIB
        # This is going to always be empty unless I build a testing environment or pretend using this is doing things......
        columnsTreeviewRight = ('#1', '#2', '#3', '#4')
        self.treeRight = ttk.Treeview(self.parent, columns=columnsTreeviewRight, show='headings')
        self.treeRight.heading('#1', text='Crypto', anchor=tk.W)
        self.treeRight.heading('#2', text='$ Worth')
        self.treeRight.heading('#3', text='$ Delta')
        self.treeRight.heading('#4', text='% Delta')
        self.treeRight.column('#1', stretch=YES, width=75)
        self.treeRight.column('#2', stretch=YES, width=100)
        self.treeRight.column('#3', stretch=YES, width=100)
        self.treeRight.column('#4', stretch=YES, width=100)
        # rowspan here HAS TO MATCH self.treeLeft and vice-versa
        self.treeRight.grid(row=1, column=6, rowspan=6, columnspan=5, sticky=NSEW)
        self.scrollbarTRy = Scrollbar(self.parent)
        self.scrollbarTRy.grid(row=1, column=11, rowspan=6, sticky='NSE')
        self.scrollbarTRy.config(command=self.treeRight.yview)
        self.treeRight.config(yscrollcommand=self.scrollbarTRy.set)

        # GUI-Bottom: What do I need here? Do I need anything here?!
        self.placeholderLabel = Label(text="List # of models ran, ID of Model with best #'s so far. What the variables are.")
        self.placeholderLabel.grid(row=7, column=0, columnspan=10, sticky=W)
        self.productPlot = Figure(figsize=(8, 3), dpi=100)
        self.productPlotSubplot = self.productPlot.add_subplot(111)
        self.canvasFlag = 1

        # Steps after building the GUI:
        #   1) Build the GUI as of the State that it was in when it was closed but also the BASE STATE to continue operations
        self.currentState()
        # Initializes all the background tasks necessary to make this work
        # Honestly I have NO IDEA WHAT THIS IS: CHECK "TONNE VAYS" on youtube. From Dan. Might help??!
        # Need threads to do:
        #   2) Start the background task to populate historical data (Public API hits)
        self.programThreads('Historical Rates', {"sleepAmount": 0.5, "environment": "Dev", "devSleepAmount": 86400})
        #   3) Check for new Products every 24 hours, or if something weird happens with the buy/sell logic. Use "specialFlag" to close non-standard threads.
        self.programThreads('Update Products', {"sleepAmount": 86400, "specialFlag": False})
        #   4) Continuously optimize Crypto Market Models
        self.programThreads('Optimize Models', {"sleepAmount": 1})
        #   5) Run the Buy/Sell algorithm (Private API hits)
        self.programThreads('Run Buy/Sell Algorithm', {"RA": "Run Buy/Sell Algorithm", "trendSleepAmount": 3600})
        # I MAY want to update the MAIN thread with results from the background threads above.... Will need to figure that out.

    def helpmenu(self, value):
        # Create a dictionary so the helpbox will show the message depending on which item was selected
        helpmenudict = {'1': '',
                        '2': '',
                        '3': ''}
        tkmb.showinfo('Using Nibbler', helpmenudict[value])

    def clearall(self):
        print('EVERY program needs a "Clear All"')

    def programThreads(self, threadID, parameters):
        if threadID == 'Historical Rates':
            # print("Paused while developing optimization models")
            thread = threading.Thread(target=self.getHistoricalRates, args=(parameters,), daemon=True)
            # return
        elif threadID == 'Update Products':
            thread = threading.Thread(target=self.updateProducts, args=(parameters,), daemon=True)
        elif threadID == 'Optimize Models':
            thread = threading.Thread(target=self.optimizeModels, args=(parameters,), daemon=True)
        elif threadID == 'Run Buy/Sell Algorithm':
            thread = threading.Thread(target=self.buySellAlgorithm, args=(parameters,), daemon=True)
        else:
            return
        thread.start()

    def getHistoricalRates(self, parameters):
        # To back populate every market the sleepAmount is set to 2 seconds between API hits
        # Once all markets have been fully back populated this will change to every hour/minute?
        while 1 == 1:
            # (1) Get the list of products to get historical rates for if needed
            listOfProducts = []
            query = f"""SELECT A.ProductID
                            ,A.HaveHistoricRates
                            ,MaxDataDate = ISNULL(MAX(DATEADD(SECOND, B.CandleTime, '1970-01-01')), '1970-01-01')
                            ,MinDate = ISNULL(A.MinDate, '1970-01-01')
                        FROM db.schema.table1 as A 
                        LEFT JOIN db.schema.table2 as B on A.ProductID = B.ProductID
                        GROUP BY A.ProductID, A.HaveHistoricRates, A.MinDate"""
            self.cursorHR.execute(query)
            [listOfProducts.append(x) for x in self.cursorHR.fetchall()]
            # (2) Loop through the list of products and get the missing historic rates
            getMinDates = []
            for product in listOfProducts:
                # if product[0] not in ['SHIB-USDT','YFII-USDT']: continue
                text = 'New Products that need Start Dates: '
                maxUTC = (datetime.datetime.utcnow() + datetime.timedelta(hours=-1)).replace(minute=0, second=0, microsecond=0)
                if product[1] == 0 and str(product[3]) == '1970-01-01':
                    text += product[0] if text == 'New Products that need Start Dates: ' else ', ' + product[0]
                    self.placeholderLabel.configure(text=text)
                    self.placeholderLabel.update()
                    continue
                STime = datetime.datetime.combine(product[2] if product[1] == 1 else product[3], datetime.datetime.min.time())
                intervals = [(432000, 86400), (108000, 21600), (18000, 3600), (4500, 900), (1500, 300), (300, 60)]
                fullDataReceived = 0
                print(f"Backfilling HistoricRates for: {product[0]}; starting at: {str(STime)}")
                while fullDataReceived == 0:
                    # IF HaveHistoricalRates == 0 THEN Loop through the larger intervals first until data is found for finding STime and ETime faster
                    if product[1] == 0:
                        counter = 0
                        while counter < 6:  # THIS ISN'T WORKING YET!!! DDX-USDT has data starting on 2021-09-01 and it skips over right now (RESET DDX-USDT to 0)
                            ETime = STime + datetime.timedelta(minutes=intervals[counter][0])
                            if ETime > maxUTC:
                                ETime = maxUTC
                            params = {"start": STime.isoformat(), "end": ETime.isoformat(), "granularity": intervals[counter][1]}
                            try:
                                r = requests.get(self.cbServer.get() + 'products/' + product[0] + '/candles', params=params, headers=self.headers)
                            except:
                                print(r.status_code, r.content)
                            # print(f"\t{params}, {self.cbServer.get() + 'products/' + product[0] + '/candles'}")
                            # print(f"\t{r.status_code}, {r.content}")
                            if r.status_code == 400 or len(r.json()) == 0 or 'message' in str(r.json()):
                                STime = ETime
                                if STime == ETime and ETime == maxUTC:
                                    counter, fullDataReceived = 6, 1
                                    break
                            else:
                                counter += 1
                            time.sleep(parameters["sleepAmount"])
                    else:
                        ETime = STime + datetime.timedelta(minutes=intervals[5][0])
                    dataEntered = 0
                    while ETime < maxUTC:
                        self.cursorHR.execute('TRUNCATE TABLE Stage.HistoricRates')
                        params = {"start": STime.isoformat(), "end": ETime.isoformat(), "granularity": "60"}
                        try:
                            r = requests.get(self.cbServer.get() + 'products/' + product[0] + '/candles', params=params, headers=self.headers)
                            print(f"\tData points between '{STime.isoformat().replace('T', ' ')}' and '{ETime.isoformat().replace('T', ' ')}': " + str(len(r.json())) + f" {product[0]}")
                            # print(r.json())
                            # Set STime to the ETime and add 300 units to the ETime to keep going
                            STime = ETime
                            ETime = ETime + datetime.timedelta(minutes=300)
                            if len(r.json()) == 0 or 'message' in str(r.json()):
                                continue
                            query = "INSERT INTO Stage.HistoricRates "
                            for i, x in enumerate(r.json(), 1):  # [Time, Low, High, Open, Close, Volume]
                                query += f"SELECT '{product[0]}','{x[0]}','{numpy.format_float_positional(x[1], trim='-')}','{numpy.format_float_positional(x[2], trim='-')}','{numpy.format_float_positional(x[3], trim='-')}','{numpy.format_float_positional(x[4], trim='-')}','{numpy.format_float_positional(x[5], trim='-')}',GETDATE() UNION "
                            self.cursorHR.execute(query[:-6])
                            self.cursorHR.execute('EXEC Prod.PopulateHistoricRates')
                            dataEntered = 1
                        except BaseException as e:
                            print(r.status_code, r.content)
                            print(str(e))
                        time.sleep(parameters["sleepAmount"])
                    if product[1] == 0 and dataEntered == 1:
                        self.cursorHR.execute(f"UPDATE Prod.Products SET HaveHistoricRates = 1 WHERE ProductID = '{product[0]}'")
                    fullDataReceived = 1
            time.sleep(parameters["sleepAmount"] if parameters["environment"] != 'Dev' else parameters["devSleepAmount"])

    def updateProducts(self, parameters):
        while 1 == 1:
            # print("Updating Products. Default setting is every 24 hours at UTC time.\nCurrent time: " + datetime.datetime.utcnow().isoformat().replace('T', ' '))
            self.cursorUP.execute('TRUNCATE TABLE Stage.Products')
            r = requests.get(self.cbServer.get() + 'products', headers=self.headers)
            query = 'INSERT INTO Stage.Products '
            for x in r.json():
                if x["quote_currency"] not in ['EUR', 'USD', 'GBP']:
                    query += f"""SELECT '{x["id"]}'
                                    ,'{x["display_name"]}'
                                    ,'{str(x["base_currency"])}'
                                    ,'{str(x["quote_currency"])}'
                                    ,'{str(x["base_increment"])}'
                                    ,'{str(x["quote_increment"])}'
                                    ,'{str(x["base_min_size"])}'
                                    ,'{str(x["base_max_size"])}'
                                    ,'{str(x["min_market_funds"])}'
                                    ,'{str(x["max_market_funds"])}'
                                    ,'{x["status"]}'
                                    ,'{x["status_message"].replace("'", "''")}'
                                    ,{('1' if x["cancel_only"] else '0')}
                                    ,{('1' if x["limit_only"] else '0')}
                                    ,{('1' if x["post_only"] else '0')}
                                    ,{('1' if x["trading_disabled"] else '0')}
                                    ,{('1' if x["fx_stablecoin"] else '0')} UNION """
            self.cursorUP.execute(query[:-6])
            self.cursorUP.execute('EXEC Prod.UpdateProducts')
            # Reset the products in the Crypto Dictionary
            self.cryptDict["products"] = []
            self.currentState()
            if parameters["specialFlag"]:
                return
            time.sleep(parameters["sleepAmount"])

    def optimizeModels(self, parameters):
        print("Optimize Models. Parameters: " + str(parameters))
        #   -Sup./Dem.: Using the Supply (high), Demand (low) zones Trey talked about. Either can be a mid zone too.
        #   -Day Trade: Using inflection points and mins/maxs to trade short term only.
        #   -Hold: Hold to determine new Supply/Demand zones. Applies to Sup./Dem. only.
        #   -Long: Held from low and unsure of new or any zones.
        #   -Inactive: Unsure of what to do with product at the moment
        # I think.... each Product graph needs to be multiplied by it's "Base" currency to get the "actual" crypto graph?!
        # But even if so.... either graph should be able to be used for trade models? Or maybe only use the product graph?!

        # Populate cryptDict["history"] for analysis
        # for x in self.cryptDict["products"]:
        #     # Get the data
        #     if x["id"] != '': continue
        #     print(x["id"])
        #     # query = ''
        #     # self.cursorOM.execute(query)

    def buySellAlgorithm(self, parameters):
        print("Buy/Sell Algorithm. Not available yet.")
        # Types of trade flags for each product are dependent on the models developed in the function above (optimizeModels).

    def currentState(self):
        # (1) Populate the Crypto Dictionary "Products" object. All markets for the bot to buy/sell in.
        marketOptions = []
        self.regularCursor.execute('SELECT DISTINCT ProductID, DisplayName, QuoteCurrency FROM Prod.Products')
        for x in self.regularCursor.fetchall():
            self.cryptDict["products"].append({"id": x[0], "displayName": x[1], "quoteCurrency": x[2]})
            if x[2] not in marketOptions:
                marketOptions.append(x[2])
        marketOptions.sort()
        self.treeLeft.delete(*self.treeLeft.get_children())
        [self.treeLeft.insert('', 'end', text=x, values=x) for x in marketOptions]

    def populateMarket(self, event):
        # Remove existing items in the treeMid
        self.treeMid.delete(*self.treeMid.get_children())
        # Populate treeMid with the Products available from the selected Market Options
        for x in self.cryptDict["products"]:
            if x["quoteCurrency"] == self.treeLeft.item(self.treeLeft.selection())['text']:
                self.treeMid.insert('', 'end', text=x["id"], values=(x["displayName"], 'tbd', 'tbd'))

    def showProductGraph(self, event):
        # ADD A SCALE OPTION MENU to see more recent data with more granularity.
        product = self.treeMid.item(self.treeMid.selection())['text']
        startTime = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        phrETime = startTime.isoformat()
        phrSTime = (startTime + datetime.timedelta(hours=-300)).isoformat()
        query = f"""SELECT CandleTime, OpenValue, CloseValue
                    FROM Prod.HistoricRates
                    WHERE ProductID = '{product.replace('/', '-')}'"""
        self.regularCursor.execute(query)
        y, times = [], []
        # Figure out a faster way to do this like with pandas dfs?
        for i, x in enumerate(self.regularCursor.fetchall()):
            times.append(datetime.datetime.utcfromtimestamp(x[0]))
            y.append((float(x[1]) + float(x[2]))/2)
        # (2) Gets the Regression data for the plot. This will come from SQL eventually
        if self.canvasFlag is None:
            self.productPlotSubplot.clear()
        self.canvas = FigureCanvasTkAgg(self.productPlot, master=self.parent)
        # Plot the Products data
        self.productPlotSubplot.plot(times, y)
        self.productPlotSubplot.title(f"Points plotted for {product}: {str(len(times))}")
        self.canvas.draw()
        # placing the canvas on the Tkinter window
        self.canvas.get_tk_widget().grid(row=8, column=0, columnspan=12)
        self.canvasFlag = None
        self.placeholderLabel.configure(text=f"Points plotted for {product}: {str(len(times))}")
        self.placeholderLabel.update()


def main():
    root = tk.Tk()
    CryptoAutomatedTrader(root)
    root.mainloop()


if __name__ == "__main__":
    main()
