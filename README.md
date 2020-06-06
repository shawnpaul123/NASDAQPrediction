# NASDAQPrediction
Using parallel programming and other cool swe methods to trade NASDAQ Stocks


Goal is:


Run scripts using a luigi pipeline as well as using cloud computing and spark to analyze large datasets

Run API's (built by moi) to process data to return the necessary output to analyze stock trends and predict using ML/DL

Get better understanding of docker/Deep learning TF 2.0/CI-CD and visualization(Bokeh) concepts

dowload_all_datasets.py:-> webscrapeds NASDAQ stock ticker names and uses yfinance to download historical data
create_large_stock_dataset.py :-> uses dask(parallel computing) to create a large structured dataset of all the stocks asap
