import pandas as pd
import numpy as np
import os
from sets import Set
import csv
import datetime
from datetime import date, timedelta
import time
import requests
from requests.auth import HTTPBasicAuth

def get_data(filename):

#    players_B_BarkMa00_gamelog
    players_index = filename.find("players_")
    players_index += (len("players_") + 2)
    gamelog_index = filename.find("_gamelog")
    pfr_name = filename[players_index:gamelog_index]

    print "PFR_NAME IS:", pfr_name

    heading_dict = {}
    headings_count = 0
    first_heading = -1
    with open(filename, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        skip_list = [];
        index = 0
        for row in spamreader:
            row = ''.join(row)
            print row

            # Getting the original column headers and putting into a dict
            if row.startswith(',') and headings_count == 0:
                for idx, val in enumerate(row.split(",")):
                    if val != '':
                        if first_heading == -1:
                            first_heading = idx
                        heading_dict[idx] = val
                headings_count+=1

            # Skip the rows that start with commas, rk or are empty
            if row.startswith(',') or (row.startswith('Rk') and index > 2) or (not row):
                skip_list.append(index)
            index+=1
        print skip_list
    print heading_dict

    # Creating a dataframe ignoring the skip_list rows and indexed using the date
    df_temp = pd.read_csv(filename, index_col='Date',
            parse_dates=True, na_values=['nan', 'NaN'], skiprows=skip_list)
    df_temp = df_temp.fillna(0)


    # Column header renaming
    columns = list(df_temp.columns.values)
    print columns

    columns_map = fetch_updated_column_names(columns, first_heading, heading_dict)
    print columns_map

    # Rename the columns in our dataframe
    df_temp.rename(columns=columns_map,inplace=True)

    pguid = fetch_player_guid(pfr_name)

    # Iterate over the dataframe
    for index, row in df_temp.iterrows():
        ff_points = calculate_game_ffpoints(row)
        print ff_points
        payload = assemble_payload(pguid, row, index, ff_points)
        print payload
        create_game_entry(payload)

    return df_temp

def create_game_entry(payload):
    r = requests.post("http://127.0.0.1:8000/games/",
                      data=payload,
                      auth=HTTPBasicAuth('andrasta', 'aA187759!'))
    print r.status_code
    print r.text

def assemble_payload(pguid, row, index, ff_points):
    data = {'pguid': str(pguid),
            'year': row['Year'],
            'game_count_played': row['Rk'],
            'game_number': row['G#'],
            'date': str(index.date()),
            'home_team': row['Tm'],
            'home_or_away': ['home','away'][row['Home or Away'] == ''],
            'opp_team': row['Opp'],
            'result': row['Result']}

    # At this point they're gonna be conditional based on type...
    # QB related stats
    if 'Passing_Cmp' in row:
        passing_data = {'gs': row['GS'],
                        'pass_comp': row['Passing_Cmp'],
                        'pass_att': row['Passing_Att'],
                        'comp_pct': row['Passing_Cmp%'],
                        'pass_yards': row['Passing_Yds'],
                        'pass_tds': row['Passing_TD'],
                        'ints_thrown': row['Passing_Int'],
                        'qb_rating': row['Passing_Rate'],
                        'yards_per_pass_att': row['Passing_Y/A'],
                        'adj_yards_per_pass_att': row['Passing_AY/A']}
        temp = data.copy()
        temp.update(passing_data)
        data = temp

    # WR/RB related stats
    if 'Rushing_Att' in row:
        rushing_data = {'rush_att': row['Rushing_Att'],
                        'rush_yards': row['Rushing_Yds'],
                        'yards_per_rush_att': row['Rushing_Y/A'],
                        'rush_tds': row['Rushing_TD']}

        temp = data.copy()
        temp.update(rushing_data)
        data = temp

    if 'Receiving_Tgt' in row:
        receiving_data = {'pass_tgts': row['Receiving_Tgt'],
                          'receptions': row['Receiving_Rec'],
                          'rec_yards': row['Receiving_Yds'],
                          'yards_per_rec': row['Receiving_Y/R'],
                          'rec_tds': row['Receiving_TD']}
        temp = data.copy()
        temp.update(receiving_data)
        data = temp

    if 'Kick Returns_Rt' in row:
        kick_ret_data = {'kickoff_returns': row['Kick Returns_Rt'],
                         'kr_yards': row['Kick Returns_Yds'],
                         'yards_per_kick_return': row['Kick Returns_Y/Rt'],
                         'kr_tds': row['Kick Returns_TD']}
        temp = data.copy()
        temp.update(kick_ret_data)
        data = temp

    if 'Punt Returns_Ret' in row:
        punt_ret_data = {'punt_returns': row['Punt Returns_Ret'],
                         'pr_yards': row['Punt Returns_Yds'],
                         'yards_per_punt_return': row['Punt Returns_Y/R'],
                         'pr_tds': row['Punt Returns_TD']}
        temp = data.copy()
        temp.update(punt_ret_data)
        data = temp

    data['game_ff_pts'] = ff_points
    return data

def calculate_game_ffpoints(row):
    '''
    Passing Score = (# of Passing TD * 4) + (# of Passing Yards / 25) + (# of 2 point conversions * 2) - (# of Interceptions * 2)
    Receiving Score = (# of Receiving TD * 6) + (# of Receiving Yards / 10) + (# of 2 point conversion receiving * 2)
    Rushing Score = (# of Rushing TD * 6) + (# of Rushing Yards / 10) + (# of 2 point conversion rushing * 2)
    Kickoff Return TD = 6pts
    Punt Return TD = 6pts
    Fumble Recovered for TD = 6pts
    Each Fumble Lost = -2
    '''
    total = 0
    if 'Passing_TD' in row:
        passing_score = (row['Passing_TD']*4) + (row['Passing_Yds']/25) - (row['Passing_Int']*2)
        total += passing_score
    if 'Receiving_TD' in row:
        receiving_score = (row['Receiving_TD']*6) + (row['Receiving_Yds']/10)
        total += receiving_score
    if 'Rushing_TD' in row:
        rushing_score = (row['Rushing_TD']*6) + (row['Rushing_Yds']/10)
        total += rushing_score
    if 'Kick Returns_Rt' in row:
        misc_score = (row['Kick Returns_TD']*6)
        total += misc_score
    if 'Punt Returns_Ret' in row:
        misc_score = (row['Punt Returns_TD']*6)
    return total

def fetch_updated_column_names(columns, first_heading, heading_dict):

    # Keep the old ones up to the ones we wanna change
    columns_updated = columns[0:first_heading-1]

    # rename the columns in the dataframe to match the header from pfr
    temp_index = first_heading
    for item in columns[first_heading-1:]:
        item = heading_dict[temp_index]+ '_' + item.split('.')[0]
        columns_updated.append(item)
        temp_index+=1
    print columns_updated

    # Pair the old names to the new names
    columns_map = {}
    for i in range(len(columns_updated)):
        columns_map[columns[i]] = columns_updated[i]

    # Hack so this won't be blank
    columns_map['Unnamed: 6'] = 'Home or Away'
    return columns_map

def fetch_player_guid(pfr_name):

    r = requests.get("http://127.0.0.1:8000/pfrguids/"+pfr_name+"/guid", auth=HTTPBasicAuth('andrasta', 'aA187759!'))
#    print "Players guid is: ", str(r.json())
    pguid = r.json()["pguid"]
    print "pguid is:", pguid
    return pguid

'''
    r = requests.post("http://127.0.0.1:8000/games/",
                          auth=HTTPBasicAuth('andrasta', 'aA187759!'))
'''

#print os.path.exists("../tutorial/QB/players_B_BarkMa00_gamelog___stats.csv")
stats_dir = os.path.join(os.getcwd(), "tutorial/")
print get_data(os.path.join(stats_dir, "QB/players_B_BarkMa00_gamelog___stats.csv"))
#with open('tutorial/QB/players_B_BarkMa00_gamelog___stats.csv', 'rb') as csvfile:
#    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
#    for row in spamreader:
#        print ', '.join(row)
#        print '\n'
