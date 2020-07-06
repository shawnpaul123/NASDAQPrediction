#streamlit datascince ui

import streamlit as st
from manipulate_data_frame import Stock_Data_Test_Args


import numpy
import pandas as pd


SDTA = Stock_Data_Test_Args()
df = SDTA.read_and_display_old_df()
print(df.head(5))


#https://docs.streamlit.io/en/stable/getting_started.html



st.title('My first app')

class general_visualizers:

	def __init__(self):
		self.app_name = "Stocks Visualizer for combined dataframe"
		self.nhead = 10


	def fetch_data(self,df):
		data_load_state = st.text('Loading data...')
		df_show = df.head(self.nhead)
		data_load_state.text('Loading data...done!')
		st.text(df_show)




gva = general_visualizers()
fetch = gva.fetch_data(df)


