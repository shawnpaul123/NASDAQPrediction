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
from dask import dataframe as dd 
from sklearn import preprocessing





#time series related
from tsfresh import extract_features
from tsfresh import select_features
from tsfresh.utilities.dataframe_functions import impute
from tsfresh.utilities.distribution import LocalDaskDistributor
import random#for testing


#visuals related
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, show, output_file

#stats and pca libarires
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
from sklearn.decomposition import PCA # for PCA calculation


#have all global variables related to stock data  
#have all reading of 
class Stock_Data_Test_Args:
    #have all global variables over here
    def __init__(self):
        #stock data
        self.NASDAQ_URL = r"https://en.wikipedia.org/wiki/NASDAQ-100"
        self.list_ticks = r'.\Stock_Data'
        self.sym = 'AAPL'#test stock
        self.fourier = "fft"
        self.price_col = 'Close'
        self.freq = 35
        self.old_df_path = r".\stored_data\old_df.parquet"
        self.data_path = r".\stored_data"

        

    def get_ticker_list(self):
        stock_pqs = os.listdir(self.list_ticks)
        tickers = []
        for stk in stock_pqs:
            stk = stk[:-8]#remove file type
            tickers.append(stk)
        return tickers


   
        
        
    def read_stock(self):
        #reads single dataframe
        path = r".\Stock_Data\{}.parquet".format(self.sym)
        df = pd.read_parquet(path)
        return df




    def read_and_display_old_df(self):
        path = self.old_df_path
        df = pd.read_parquet(path,engine = 'pyarrow')
        

        return df
    





#receives dataframe and inherits setting class
class missing_data_mad_analysis:

    def __init__(self,df,df_cols):

        self.df = df
        self.column_names = df.columns
        self.cols = df_cols
 
   
        self.threshold = 0.5 # rejects cols with 50 percent of the missing data
        self.empty_rows = 150 #removes subset if all rows have missing values
        self.rel_col = 'C'

        self.chunk_size = 20#drops the n rows if there are missing vals not meeting a threshold
        super(missing_data_mad_analysis,self).__init__()


    #to be applied on combined dataset
    def missing_data_analysis(self):        

        df = self.df    
        zero_val = (df == 0.00).astype(int).sum(axis=0)
        mis_val = df.isnull().sum()
        mis_val_percent = 100 * df.isnull().sum() / len(df)
        mz_table = pd.concat([zero_val, mis_val, mis_val_percent], axis=1)
        mz_table = mz_table.rename(
        columns = {0 : 'Zero Values', 1 : 'Missing Values', 2 : '% of Total Values'})
        mz_table['Total Zero Missing Values'] = mz_table['Zero Values'] + mz_table['Missing Values']
        mz_table['% Total Zero Missing Values'] = 100 * mz_table['Total Zero Missing Values'] / len(df)

   
        return mz_table
        #sample output and source ->https://stackoverflow.com/questions/37366717/pandas-print-column-name-with-missing-value

    

    #returns list of column names and number of missing dates


    def returns_data_stats(self):
        missing_df = self.missing_data_analysis(self)
        missing_df.to_csv(r"./stored_data/missing_df_combined.csv")
        return True


    def converts_between_types(self,convert_type,data_frame):

        dataframe = data_frame

        if convert_type =='dask':
            dataframe = dd.from_pandas(dataframe,npartitions=3)
            return dataframe

        if convert_type == 'pandas':
            #dask compute and return dataframe
            dataframe = dataframe.compute()

    
    def chunk_dataframe(self):

        size = self.chunk_size
        seq = self.df
        for pos in range(0, len(seq), size):
            yield seq.iloc[pos:pos + size] #multiple returns

    #if the number of rows have n number missing in y rows, then remove the whole subset of 
    #the dataframe

    def removes_rows_with_missing_blanks(self):

        dataframe = self.df
        column_names = dataframe.columns

        df_list  = []

 
        chnk = self.chunk_dataframe()

        for i in chnk:

                  #returs list of na values in a dataframe
            sum_zeros = np.array(i.isnull().sum(axis=1).tolist())
            sum_zeros = np.sum(sum_zeros)


            if sum_zeros < (self.empty_rows*self.chunk_size):

                df = i 

            else:
                #return empty datafame
           
                df = pd.DataFrame(columns= column_names)

            df_list.append(df)


        cleaned_section_df = pd.concat(df_list)

        return cleaned_section_df


    def create_categories(self,df):
        #create categories based on certain values
        #one hot encode - buy sell - hold
        #offset timed data for one stock
        #create category of buy vs sell based on diff
        #del original timed data
        pass


    def combine_categories(self,df):
        #combines categories together to fewer categories in a dataset
        #for stocks combine hold and buy
        pass


    def plot_percentage_categories(self,df):
        pass



    def normalize_dataframe(self,df):
        x = df.values #returns a numpy array
        min_max_scaler = preprocessing.MinMaxScaler()
        x_scaled = min_max_scaler.fit_transform(x)
        df = pd.DataFrame(x_scaled)
        return df

                


'''
#distraction
#predict sleep stage, entering management from technical role
#http://veekaybee.github.io/2019/02/13/data-science-is-different/
https://www.streamlit.io/
'''


#try using super with python
class time_series_stats_analysis(missing_data_mad_analysis):

    def __init__(self,df,df_columns):
        self.decomp = True  
        self.time_model_name = None#model_name
        self.time_column = 'Date'#time_column
        self.column_focus ='Id' #column_focus#columns to focus for prediciton(buy sell hold)
        self.chunk_size=50
        self.df = df
        self.df_columns = df_columns

        super(time_series_stats_analysis,self).__init__(self.df,self.df_columns)




    def find_correlattion_between_vars(self,df):
        pass


    def seasonal_decomp(self,df):
        #https://www.statsmodels.org/stable/examples/notebooks/generated/stl_decomposition.html->doesn't work
        #https://machinelearningmastery.com/decompose-time-series-data-trend-seasonality/
        print(df.head(8))
        for col in df.columns:
            res = STL(df[col],period=3).fit()
            res.plot()
            plt.show()


    def time_series_filters(self,df):
        #https://www.statsmodels.org/stable/examples/notebooks/generated/tsa_filters.html
        pass



    def create_timestamps_and_ids(self,df):
        df['Date'] = df.index
        df['Id'] = random.randrange(0,4)
        return df


    def chunk_dataframe(self,df):
        size = self.chunk_size
        seq = df
  
        for pos in range(0, len(seq), size):
         
            yield seq.iloc[pos:pos + size] #multiple returns


        

    def pca(self,df):
        '''
        -call preprsuper ocessing for normlaziation
        - apply pca and keep all components
        - find how mny compos adre needed to expalin the variacnce
        - get list
        '''
        df = super(time_series_stats_analysis,self).normalize_dataframe(df)
        print(df.head(5))
        pca = PCA()
        X_pca = pca.fit(df)
        plt.plot(np.cumsum(pca.explained_variance_ratio_))
        plt.xlabel('number of components')
        plt.ylabel('cumulative explained variance')
        plt.show()


        return X_pca











#(stock,df_cols,datatype,drop_cols)


#mdma = missing_data_mad_analysis(stock,None,None,None)
#df = mdma.removes_rows_with_missing_blanks()


if __name__ == '__main__':


    stk = Stock_Data_Test_Args()

    #returns list of stock tickers
    stock = stk.read_stock()
    tssa = time_series_stats_analysis(stock,stock.columns)
    tsfresh = tssa.pca(stock)
    print(stock.head(5))
    #print(tsfresh.head(5))


class model_create:

    def __init__(self,model_name):
        pass





    def genetic_algo_tuning(self):
        pass
        #use GAs to tune for the best model

    def create_model_lstm(self):
        pass

    def create_model_bi_lstm(self):
        pass

    def create_model_transformer(self):
        pass


    def compile_model(self):
        pass


    def visualize_results(self):
        pass


    def weight_prune(self):
        pass


    def save_best_model_with_params(self):
        pass







'''

#PSG ---------------> Type I polysomnography, a sleep study performed overnight while being continuously monitored by a credentialed technologist, 
                      is a comprehensive recording of the biophysiological changes that occur during sleep. 

#EEG --------------->Gets electrical signals in the brain


'''

'''

Resources:
#r'https://www.sciencedirect.com/science/article/pii/S2352914820302161'



'''





