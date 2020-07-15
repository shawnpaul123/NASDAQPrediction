

class stored_variables:#hass all the values of the global variables for the multiple self sharing classes

    def __init__(self):

        self.threshold = 0.5 # rejects cols with 50 percent of the missing data
        self.empty_rows = 0.55 #removes subset if all rows have missing values
   

        self.chunk_size = 20#drops the n rows if there are missing vals not meeting a threshold

        self.chosen_col = 'pct_changeClose_ATVI' #choice fo stock
        self.bar = 0.5#percent change for buy signal

        self.time_column = 'Date'#time_column
        self.column_focus ='Id' #column_focus#columns to focus for prediciton(buy sell hold)
        self.chunk_size=50
        self.comp_imp = 0.99
        self.results_csv = None
        self.extracted_path = r'.\stored_data\ts_extracted.csv'

        self.category_column = 'Operation'
        self.combine = 'True'





        super(stored_variables,self).__init__()