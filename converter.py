import pandas as pd
import numpy as np
import os
from sets import Set
import csv
import datetime
from datetime import date, timedelta
import time


def get_data(filename):
    with open(filename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        skip_list = [];
        index = 0
        for row in spamreader:

            row = ''.join(row)
            print row
            # Skip the rows that start with commas, rk or are empty
            if row.startswith(',') or (row.startswith('Rk') and index > 2) or (not row):
                skip_list.append(index)
            index+=1
        print skip_list

    df_temp = pd.read_csv(filename, index_col='Date',
            parse_dates=True, na_values=['nan'], skiprows=skip_list)
#    df_temp = df_temp.rename(columns={'Adj Close': symbol})
#    df = df.join(df_temp)
#    if symbol == 'SPY':  # drop dates SPY did not trade
#        df = df.dropna(subset=["SPY"])

    return df_temp

#print os.path.exists("../tutorial/QB/players_B_BarkMa00_gamelog___stats.csv")
stats_dir = os.path.join(os.getcwd(), "../../../Desktop/InfoVis/project/tutorial/")
print get_data(os.path.join(stats_dir, "tutorial/QB/players_B_BarkMa00_gamelog___stats.csv"))
#with open('tutorial/QB/players_B_BarkMa00_gamelog___stats.csv', 'rb') as csvfile:
#    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
#    for row in spamreader:
#        print ', '.join(row)
#        print '\n'
