#read old_df.pq
#create new indicators
#create your class of visualizations
#save & bokeh for images
# drop all rows with nans
# review and learn pyspart

import logging
from numba import jit
from sklearn import preprocessing
import os
import numpy as np
import pandas as pd

from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, show, output_file


class Stock_Data_Test_Args:

    def __init__(self):
        #stock data

        self.NASDAQ_URL = r"https://en.wikipedia.org/wiki/NASDAQ-100"
        self.list_ticks = r'C:\Users\shawn paul\Desktop\PyFinanceProj\NASDAQPrediction\Stock_Data'
        

    def get_ticker_list(self):
        stock_pqs = os.listdir(r'C:\Users\shawn paul\Desktop\PyFinanceProj\NASDAQPrediction\Stock_Data')
        tickers = []
        for stk in stock_pqs:
            stk = stk[:-8]#remove file type
            tickers.append(stk)
        return tickers


#needs a loist of stock tickers passed into this
class create_and_plot_indicators:

    def __init__(self,stock):
       
        self.stock_list = stock
        self.sym = self.stock_list[0]
        self.fourier = "fft"
        self.price_col = 'Close'
        self.freq = 35
        self.old_df_path = r"C:\Users\shawn paul\Desktop\PyFinanceProj\NASDAQPrediction\stored_data\old_df.parquet"
        self.data_path = r"C:\Users\shawn paul\Desktop\PyFinanceProj\NASDAQPrediction\stored_data"
        
    def read_stock(self):
        #reads single dataframe
        path = r"C:\Users\shawn paul\Desktop\PyFinanceProj\NASDAQPrediction\Stock_Data\{}.parquet".format(self.sym)
        df = pd.read_parquet(path)
        return df


    def create_plots(self):
        #gets single dataframe
        df = self.read_stock()
        df = self.fourier_transform_plots(df)
        
        source = ColumnDataSource(df)
        df.index.name = 'Date'
        p = figure(x_axis_type="datetime", plot_width=800, plot_height=350)
        p.line('Date', self.price_col, source=source)
        p.line('Date', self.fourier, source=source)

        output_file("stcok_price.html")
        show(p)






    def fourier_transform_plots(self,df1):
        #singledataframeread in timeseries of one stock and fft/ifft
        df = df1
        '''
        price = self.price_col
        for column in df:

        a = df[self.price_col]        
        fft = np.fft.fft(a)
        fft[self.freq:] = 0
        itx = np.fft.ifft(fft)
        df['fft'] = itx.real        
        return df'''


    def read_and_display_old_df(self):
        path = self.old_df_path
        df = pd.read_parquet(path,engine = 'pyarrow')
        print(df.head(5))
    

    def EDA_old_df(self):
        #return 
        pass


    def clean_missing_data(self):
        #interpolate cubically forward:https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#filling-missing-values-fillna
        #return a csv that has the starting data of each column
        pass

   





def create_indicators(df1,price_col,syms):
    df = df1
    for sym in syms:    
        df['ma7_'.format(sym)] = df[price_col.format(sym)].rolling(window=7).mean()
        df['ma14_'.format(sym)] = df[price_col.format(sym)].rolling(window=14).mean()
        df['ema_'.format(sym)] = df[price_col.format(sym)].rolling(window=7).mean()
    return df


#delte after use
def get_test_stock():
    pass

stk = Stock_Data_Test_Args()
stock_ticker = stk.get_ticker_list()
c = create_and_plot_indicators(stock_ticker)
c.read_and_display_old_df()