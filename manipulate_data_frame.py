#read old_df.pq
#create new indicators
#create your class of visualizations
#save & sns  for images
# drop all rows with nans
# review and learn pyspart

import logging
from numba import jit
from sklearn import preprocessing

  
def create_indicators(df1,price_col,syms):

    df = df1
    for sym in syms:
    
        df['ma7_'.format(sym)] = df[price_col.format(sym)].rolling(window=7).mean()
        df['ma14_'.format(sym)] = df[price_col.format(sym)].rolling(window=14).mean()
        df['ema_'.format(sym)] = df[price_col.format(sym)].rolling(window=7).mean()
 
    return df



