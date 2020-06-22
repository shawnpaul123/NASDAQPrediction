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
        stock_pqs = os.listdir(self.list_ticks)
        tickers = []
        for stk in stock_pqs:
            stk = stk[:-8]#remove file type
            tickers.append(stk)
        return tickers


#needs a loist of stock tickers passed into this
class functional_dataclean_visulaize:

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
    



    def run_codes_single_v_series(self, valtype):
        if valtype == "single":
            #read one df of stock as df
            pass

        if valtype == "series":
            #run operations on old df
            pass




class pyspark_datanalysis:
#takes in dataframe and conducts pyspark analysis on it


    def __init__(self,df):
        self.df = df





class missing_data:

    def __init__(self,df,df_cols,datatype,drop_cols):

        self.df = df
        self.cols = df_cols
        self.type = datatype
        self.drop_cols = drop_cols



    def missing(self):

        dff = self.df
        df_deets =  round((dff.isnull().sum() * 100/ len(dff)),2).sort_values(ascending=False)
        return df_deets



    #returns list of column names and number of dates
    def returns_data_stats(self):
        #find all columns with dates and times
        #rems both unique values
        #returns count of unique values for each column and overlapping numbers

        #str list of df columns
        col_names = []
        #numeric count of df items
        col_items = []
        #empty dataframes in a column

        if self.datatype == "Numeric"
            for col in self.cols:
                col_names.append(col)
                col_nums = df[col].to_numpy()
                col_unique = np.unique(col_nums)
                col_items.append(col_unique)
                #get 2d array of numpy with all vals for each col


        if self.datatype == "Date"
            for col in self.cols:
                col_names.append(col)
                col_nums = list(set(df[col].tolist()))
                col_items.append(col_nums)
                #get 2d array of cols and respective dates

        for col in self.cols:













                











        #find number 





        pass


    def visualize_data_stats(self)
        pass

    def undersampling(self):
        pass


    def oversampling(self):
        pass


    def smote(self):
        pass


    def smotetimeseries(self):
        pass






    

   





stk = Stock_Data_Test_Args()
stock_ticker = stk.get_ticker_list()
c = create_and_plot_indicators(stock_ticker)
c.read_and_display_old_df()