import pandas as pd
import numpy as np
import os
import os.path
from sets import Set
import csv
import datetime
from datetime import date, timedelta
import time
import requests
from requests.auth import HTTPBasicAuth
import json
from pprint import pprint
import sys
import argparse
import logging
import datetime

#logging.basicConfig(filename='qbs_retired_converter\''+ str(datetime.datetime.now())+'\'.log',level=logging.DEBUG)

def get_data(filename_pass, filename_rush, active, pguid):

    heading_dict = {}
    headings_count = 0
    first_heading = -1
    if filename_pass != None:
        with open(filename_pass, 'rb') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            skip_list = [];
            index = 0
            for row in spamreader:
                row = ''.join(row)

                # Getting the original column headers and putting into a dict
                if row.startswith('Year') and headings_count == 0:
                    for idx, val in enumerate(row.split(",")):
                        if val != '':
                            if first_heading == -1:
                                first_heading = idx
                            heading_dict[idx] = val
                    headings_count+=1

                # Skip the rows that start with commas, rk or are empty
                if row.startswith('Career') or (not row):
                    skip_list.append(index)
            print skip_list
        # Creating a dataframe ignoring the skip_list rows and indexed using the date
        df_temp_pass = pd.read_csv(filename_pass, index_col='Year',
                parse_dates=True, na_values=['nan', 'NaN'], skiprows=skip_list)
        df_temp_pass = df_temp_pass.fillna(0)
        print df_temp_pass

    first_heading = -1
    headings_count = 0
    if filename_rush != None:
        with open(filename_rush, 'rb') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            skip_list = [];
            index = 0
            for row in spamreader:
                row = ''.join(row)
    #            print row

                # Getting the original column headers and putting into a dict
                if row.startswith(',') and headings_count == 0:
                    for idx, val in enumerate(row.split(",")):
                        if val != '':
                            if first_heading == -1:
                                first_heading = idx
                            heading_dict[idx] = val
                    headings_count+=1

                # Skip the rows that start with commas, rk or are empty
                if row.startswith('Career') or (not row):
                    skip_list.append(index)
            print skip_list
#    print heading_dict
        # Creating a dataframe ignoring the skip_list rows and indexed using the date
        df_temp_rush = pd.read_csv(filename_rush, index_col='Year',
                parse_dates=True, na_values=['nan', 'NaN'], skiprows=skip_list)
        df_temp_rush = df_temp_pass.fillna(0)
        print df_temp_rush


    sys.exit("DONE")
    # Column header renaming
    columns = list(df_temp.columns.values)
#    print columns

    columns_map = fetch_updated_column_names(columns, first_heading, heading_dict)
#    print columns_map

    # Rename the columns in our dataframe
    df_temp.rename(columns=columns_map,inplace=True)

#    pguid = fetch_player_guid(pfr_name)

    game_payloads, df_temp = create_game_entries(pguid, df_temp)
    # Call the create_game_entries

    season_payloads, df_temp = create_season_entries(pguid, df_temp)
    # Call the create_season_entries

#    career_payload = assemble_career_payload(pguid, df_temp, active)

    # Call create career_entry
    return game_payloads, season_payloads, df_temp

parser = argparse.ArgumentParser(description='Converter options.')
parser.add_argument('-input_file', metavar='i',
                   help='Input json file that contains player information')
parser.add_argument('-type',
                   help='sum the integers (default: find the max)')
args = parser.parse_args()
print args.input_file
print args.type
if args.type != "QB_Retired":
    sys.exit("INVALID TYPE")
# Reading the json file
with open(args.input_file) as data_file:
    data = json.load(data_file)
    print len(data)
    count = 0
    for i in data:
        fname_pass = None
        fname_rush = None
        if count > -1:
            if args.type == "QB_Retired":
                fname_pass = "QB_Retired_Revised/players_" + i['player_name'].split(" ")[1][0] + "_" + i['pfr_name'] + "_passing.csv"
                fname_rush = "QB_Retired_Revised/players_" + i['player_name'].split(" ")[1][0] + "_" + i['pfr_name'] + "_rushing_and_receiving.csv"
            print fname_pass
            print fname_rush
            active = (args.type.find("Active") != -1)

            pguid = i['pfr_name']
        #        print pguid
            if os.path.isfile(fname_pass) or os.path.isfile(fname_rush):
                print "A csv files for " + i['player_name'] + "," + i['pfr_name'] + " exists"

                try:
                    # Fill out the payloads
                    game_payloads, season_payloads, df_career = get_data(fname_pass, fname_rush, active, pguid)
                    sys.exit("DONE")
                    # Call assemble career payload
                    career_payload = assemble_career_payload(pguid, df_career, i, active)
                    print career_payload

                    print "=========================="
                    print "Starting SEASONS creation"
                    print "=========================="
#                    create_season_entry(season_payloads)
                    print "=========================="
                    print "SEASONS creation completed"
                    print "=========================="
                    print "=========================="
                    print "CAREER update starting"
                    print "=========================="
#                    update_career_entry(career_payload)
                    print "=========================="
                    print "CAREER update completed"
                    print "=========================="


                except Exception:
                    logging.exception("Error Occurred with" + pguid + " on json line " + str(count))
            else:
#            if not os.path.isfile(fname):
                print "No stats for " + i['player_name'] + " defaulting to zeros"
#                career_payload = assemble_empty_career_payload(pguid, i, active)
#                create_career_entry(career_payload)

        count += 1
    print str(count) + " players processed"
