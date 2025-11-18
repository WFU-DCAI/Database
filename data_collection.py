#Script to gather data through the API and save the data as a csv file. 


import pandas as pd
from sodapy import Socrata 

#load data from api 
client = Socrata("data.cityofnewyork.us", None)

results = client.get("uvpi-gqnh", limit=684000)

results_df = pd.DataFrame.from_records(results)

#save as a csv file 
results_df.to_csv('data/2015_NY_Tree_Census.csv', index=False)
