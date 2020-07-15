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
from sklearn.preprocessing import LabelBinarizer

from stored_variables import stored_variables







#time series related
from tsfresh import extract_features
from tsfresh.utilities.dataframe_functions import impute
import random


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
    


#-------------------------------------------------------------------------------------


#receives dataframe and inherits setting class
class missing_data_mad_analysis(stored_variables):

    def __init__(self):
        pass
        '''
 
        self.threshold = 0.5 # rejects cols with 50 percent of the missing data
        self.empty_rows = 150 #removes subset if all rows have missing values
        self.rel_col = 'C'

        self.chunk_size = 20#drops the n rows if there are missing vals not meeting a threshold
        self.chosen_col = 'Close_ATVI' #choice fo stock

        self.bar = 1#percent change for buy signal
        self.category_colummn = ''
        '''
        super(missing_data_mad_analysis,self).__init__()
    


    #to be applied on combined dataset
    def missing_data_analysis(self,df):        

           
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


    def returns_data_stats(self,df):
        missing_df = self.missing_data_analysis(df)
        missing_df.to_csv(r"./stored_data/missing_df_combined.csv")
        return df


    def converts_between_types(self,convert_type,data_frame):

        dataframe = data_frame

        if convert_type =='dask':
            dataframe = dd.from_pandas(dataframe,npartitions=3)
            return dataframe

        if convert_type == 'pandas':
            #dask compute and return dataframe
            dataframe = dataframe.compute()

    
    def chunk_dataframe(self,df):

        size = self.chunk_size
        seq = df
        for pos in range(0, len(seq), size):
            yield seq.iloc[pos:pos + size] #multiple returns

    #if the number of rows have n number missing in y rows, then remove the whole subset of 
    #the dataframe

    def removes_rows_with_missing_blanks(self,df):

        dataframe = df
        column_names = dataframe.columns

        df_list  = []

 
        chnk = self.chunk_dataframe(df)

        for i in chnk:

                  #returs list of na values in a dataframe
            sum_zeros = np.array(i.isnull().sum(axis=1).tolist())
            sum_zeros = np.sum(sum_zeros)


            if sum_zeros < (self.empty_rows*self.chunk_size*len(df.columns)):
               

                df = i

            else:
                #return empty datafame
           
                df = pd.DataFrame(columns= column_names)

            df_list.append(df)


        cleaned_section_df = pd.concat(df_list)

        return cleaned_section_df



    def col_conditions(self,df):
        conditions = [
            (df[self.chosen_col] >= self.bar*df[self.chosen_col].median()),
            ((df[self.chosen_col] < self.bar*df[self.chosen_col].max()) & (df[self.chosen_col]*df[self.chosen_col].median() >= 0)),
            (df[self.chosen_col] < 0)]
        return conditions

    def create_categories(self,df):
        #create categories based on certain values
        #one hot encode - buy sell - hold
        #offset timed data for one stock
        #create category of buy vs sell based on diff
        #del original timed data
        df_change = df.pct_change()
        pct_cols = []
        for col in df.columns:
            pct_cols.append('pct_change' + col)

        df_change.columns = pct_cols

        df[self.time_column] = df.index
        df[self.time_column]  = pd.to_datetime(df[self.time_column])

        df_change[self.time_column] = df_change.index
        df_change[self.time_column] = pd.to_datetime(df_change[self.time_column])
       

        #changes buy,sell,hold on a single stock value
        #df['Operation'] = df.apply(self.col_conditions(df),axis=0)

        choices = [1,0,-1]#buy,hold,sell


        

        df = df.reset_index(drop=True)
        df = df.rename_axis(None)

        df_change = df_change.reset_index(drop=True)
        df_change = df_change.rename_axis(None)



     

        df_comb = pd.merge(df,df_change, left_on=self.time_column, right_on=self.time_column)
        '''
        jobs_encoder = LabelBinarizer()
        jobs_encoder.fit(df_comb['Operation'])
        transformed = jobs_encoder.transform(df_comb['Operation'])
        ohe_df = pd.DataFrame(transformed)
        df_comb =  pd.concat([df_comb, ohe_df], axis=1)#.drop(['Operation'], axis=1)


        '''
        

        conds = self.col_conditions(df_comb)

        df_comb[self.category_column] = np.select(conds , choices, default=3)

 
        print(df_comb[self.category_column ])
        df_comb.Operation = df_comb.Operation.astype(float)

        
        return df_comb




    def combine_categories(self,df,bool_cond,col_work, new_val):
        #combines categories together to fewer categories in a dataset
        #for stocks combine hold and buy
        df.loc[bool_cond,col_work] = new_val
        return df


    def get_percentage_categories(self,df):

        df2 = df[self.category_column].value_counts(normalize=True) * 100
        print(df2)
        bool_cond = df[self.category_column] == 3.0
        if self.combine:
            self.combine_categories(df,bool_cond,self.category_column,1 )

        df2 = df[self.category_column].value_counts(normalize=True) * 100
        print(df2)
        return df



    def normalize_dataframe(self,df):

       
        x = df.values #returns a numpy array
        min_max_scaler = preprocessing.MinMaxScaler()
        
        x_scaled = min_max_scaler.fit_transform(x)

        df2 = pd.DataFrame(x_scaled)
        df2.columns = df.columns

        return df2

    def mdma_analysis_complete(self,df):
        df = self.returns_data_stats(df)
        df = self.removes_rows_with_missing_blanks(df)
        df = self.returns_data_stats(df)
        df = self.create_categories(df)  
        df = self.get_percentage_categories(df)  
        #set column as index
        df = df.set_index(self.time_column)

        #df = df.select_dtypes('float')     
        
        #df = self.normalize_dataframe(df)
     

        df.to_csv('test_shawn.csv')
        #df = df.groupby('domain')['ID'].nunique()
        '''




        '''

        return df




                
#-------------------------------------------------------------------------------------

#base class-> will have different number of columns with tffresh extraction
class time_series_stats_analysis(missing_data_mad_analysis,stored_variables):

    def __init__(self):
        '''
        self.time_model_name = None#model_name
        self.time_column = 'Date'#time_column
        self.column_focus ='Id' #column_focus#columns to focus for prediciton(buy sell hold)
        self.chunk_size=50
        self.comp_imp = 0.99
        self.results_csv = None
        '''

        super(time_series_stats_analysis,self).__init__()

   

    def pca(self,df):
        '''
        -call preprsuper ocessing for normlaziation
        - apply pca and keep all components
        - find how mny compos adre needed to expalin the variacnce
        - get list
        '''
        #create next function that takes import features and keeps categories
       # df = super(time_series_stats_analysis,self).normalize_dataframe(df)
      
        pca = PCA(n_components = self.comp_imp)
        #https://github.com/ansjin/blogs/blob/master/Dimensionality%20Reduction/Dimensionality%20Reduction%20using%20PCA%20on%20multivariate%20timeseries%20data.ipynb
        #find solution of taking care of missing data
        df = df.dropna()

        X_pca = pca.fit(df)
        imp_features = pd.DataFrame(pca.components_, columns = df.columns)
        n_pcs= pca.n_components_ 
        most_important = [np.abs(pca.components_[i]).argmax() for i in range(n_pcs)]
        initial_feature_names = df.columns

        most_important_names = [initial_feature_names[most_important[i]] for i in range(n_pcs)]
        

        #returns most importnant components list
        return most_important_names



    def split_components(self):
        pass


    def tsfresh_extract(self,ts_df):

        extracted_features = extract_features(ts_df.dropna(), column_id= self.time_column, n_jobs = 0) #extract all features using TSFRESH
        print(extracted_features)
        impute(extracted_features) 
        label_df = labels.groupby([self.time_column]).first() #match the size of the labels to the size of the features
        df = label_df.join(extracted_features) #join labels and features df

        df.to_csv(self.extracted_path)
        return df






    def complete_analysis_and_output(self,df):
        #remove blanks and return clean df
        df = super(time_series_stats_analysis,self).mdma_analysis_complete(df)
        #df_pca = self.tsfresh_extract(df)
 
        df_pca_cols = self.pca(df)#self.pca(df_pca)

        #get common itms between the 2 lists
        
        df = df[df_pca_cols]

        return df





if __name__ == '__main__':


    stk = Stock_Data_Test_Args()

    #returns list of stock tickers-
    stock = stk.read_and_display_old_df()   #.read_stock()

    tssa = time_series_stats_analysis()
    df = tssa.complete_analysis_and_output(stock)
    print(df.head(5))



#------------------------------------------------------------------------------------










'''
#PSG ---------------> Type I polysomnography, a sleep study performed overnight while being continuously monitored by a credentialed technologist, 
                      is a comprehensive recording of the biophysiological changes that occur during sleep. 

#EEG --------------->Gets electrical signals in the brain
'''




