'''
-create first df with fake titles and rows till date
::iterate over reading each df
- read first stock
- get col names
    - 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'
- drop cols['Open', 'High', 'Low', 'Dividends', 'Stock Splits']
-rename cols : "stock_"{}.format(stock)
-filter out dates after jan1st
-set first column as main  

- create big dataset and store it in different folder with datstamp
- conduct PCA to reduce dimensions after choosing target variable
- create new dataset with reduced comapnents with optimal number of components
'''

#import download_all_dataframes
import pandas as pd

import dask
import dask.dataframe as dd



import download_all_dataframes

##todo
##need to adjust for inflation before normaization
##parralize normalization by applying function to pandas
##create tech indicators

import time
import logging

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = r"C:\Users\shawn paul\Desktop\PyFinanceProj\NASDAQPrediction\test_logs\create_large_dataset.log",level = logging.DEBUG, format =LOG_FORMAT)
logger = logging.getLogger()#root logger








def clean_final_df_cols(df1,syms):

    df = df1
    df = df.rename(columns={col: col.split('_')[0] for col in df.columns})
    
    new_columns = ['Open_{}', 'High_{}', 'Low_{}', 'Close_{}', 'Volume_{}', 'Dividends_{}', 'Stock Splits_{}']
    #rename all columns to appropriate stocks
    columns = []
    for sym in syms:
        for new in new_columns:
            columns.append(new.format(sym))
    
    df.columns = columns

    drop_cols =  ['Open_{}', 'High_{}', 'Low_{}', 'Dividends_{}', 'Stock Splits_{}']
    drop_columns = []  
    
    for sym in syms:
        for drop in drop_cols:
            drop_columns.append(drop.format(sym))
    
    df = df.drop(columns=drop_columns)

    return df






def read_pq(sym):

    path = r"C:\Users\shawn paul\Desktop\PyFinanceProj\NASDAQPrediction\Stock_Data\{}.parquet".format(sym)
    df = pd.read_parquet(path)    

    return df

    


def main_create_giga_ds(URL):

    syms = download_all_dataframes.return_dictonaries_of_stock_tickers(URL)
    syms = list(syms.values())
    i = 0

    for sym in syms:



        if i == 0:
            df = read_pq(sym)
            df =  dd.from_pandas(df, npartitions=3)
            old_df = df
    
            i = i+1



        else:
            df = read_pq(sym)
            df =  dd.from_pandas(df, npartitions=3)
            old_df = dd.merge_asof(old_df, df , left_index=True, right_index=True)
            


    old_df.compute()
    df = clean_final_df_cols(old_df,syms)
    assert len(df.columns) == 200 , "columns have not been dropped"
    print(df.head(5))
    logger.info("Number of columns")
    logger.info(len(df.columns))
    df.to_parquet('old_df.parquet')
 




main_create_giga_ds(r"https://en.wikipedia.org/wiki/NASDAQ-100")
logger.info("This works")




'''
              Open_x    High_x     Low_x  ...       ma7      ma14       ema
Date                                      ...                              
1993-10-25  0.010638  0.009861  0.007746  ...  0.002190  0.001924  0.002190
1993-10-26  0.010397  0.009620  0.006886  ...  0.002208  0.001964  0.002208
1993-10-27  0.006407  0.005652  0.006886  ...  0.002252  0.002022  0.002252
1993-10-28  0.004715  0.005411  0.003812  ...  0.002297  0.002078  0.002297
1993-10-29  0.006528  0.005772  0.006640  ...  0.002337  0.002125  0.002337
'''








