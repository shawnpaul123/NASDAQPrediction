#create imports
import pandas as pd
import numpy as np
import yfinance as yf

import datetime
from time import time

from datetime import date
from dateutil.relativedelta import relativedelta

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 

import requests
from bs4 import BeautifulSoup

import pickle


#Improvement in pooling
#Process took 41.73 seconds
#Process took 14.47 seconds 

'''
Collect and store all dataframes of stocks
 '''



# Create a pool of workers equaling cores on the machine
pool = ThreadPool() 

stock_file_NASDAQ = r"https://en.wikipedia.org/wiki/NASDAQ-100"


#get dictionary of all stocks

def return_dictonaries_of_stock_tickers(URL):
    website_url = ""
    website_url = requests.get(URL).text
    companies = []
    symbols   = []

    soup = BeautifulSoup(website_url,'lxml')
    My_table = soup.find('table',{'class':'wikitable sortable','id':'constituents'})

    for row in My_table.findAll('tr'):
        cells=row.findAll('td')
        
        if len(cells)==2:
            companies.append(cells[0].find(text=True))
            symbols.append((cells[1].find(text=True)).rstrip())

    dictionary = dict(zip(companies, symbols))
    return dictionary




#download all the datasets

def API_download_datasets(stock_name):
	
	stock = yf.Ticker(stock_name)
	stock = stock.history(period="max")

	stock.to_parquet(r"C:\Users\shawn paul\Desktop\PyFinanceProj\Stock_Data\{}.parquet".format(stock_name))
	#with open(r'C:\Users\shawn paul\Desktop\PyFinanceProj\Early_dates\date_pickle_early\{}.pickle'.format(stock_name)) as ed:
	

        
   



def download_datasets():
	stock_file_NASDAQ = r"https://en.wikipedia.org/wiki/NASDAQ-100"
	syms = []
	syms = return_dictonaries_of_stock_tickers(stock_file_NASDAQ)
	syms = list(syms.values())
	results = pool.map(API_download_datasets,syms)
	# Close the pool
	pool.close()
	# Combine the results of the workers
	pool.join() 
	

			

	


#need to check if files exist
download_datasets()





