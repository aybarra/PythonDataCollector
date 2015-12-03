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


logging.basicConfig(filename='rbs_retired_converter\''+ str(datetime.datetime.now())+'\'.log',level=logging.DEBUG)

def get_data(filename, active, pguid):

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
            if row.startswith(',') or (row.startswith('Rk') and index > 2) or (not row):
                skip_list.append(index)
            index+=1
#        print skip_list
#    print heading_dict

    # Creating a dataframe ignoring the skip_list rows and indexed using the date
    df_temp = pd.read_csv(filename, index_col='Date',
            parse_dates=True, na_values=['nan', 'NaN'], skiprows=skip_list)
    df_temp = df_temp.fillna(0)

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


def create_season_entries(pguid, df_career):

    '''
    'pguid', 'year', 'games_played',
    'pass_tds', 'pass_yards', 'ints_thrown', 'rec_tds',
    'rec_yards', 'rush_tds', 'rush_yards', 'kr_tds', 'pr_tds',
    'fumbles_lost', 'season_ff_pts'
    '''
    unique_years = pd.Series(df_career['Year']).unique()
#    print unique_years
    payloads = []
    frames = []
    for i in unique_years:
        # Slice to get the rows that match a particular year
        temp_df = df_career.loc[df_career['Year'] == i]
        payload,temp_df = assemble_season_payload(pguid, temp_df)
        frames.append(temp_df)
        payloads.append(payload)
#        print "Payload is:", payload
    df_career = pd.concat(frames)
    return payloads, df_career

'''
Takes the sliced df and sums the appropriate columns
    and inserts values into a json payload
'''
def assemble_season_payload(pguid, df_season_slice):

    # Generating a wins column to be used later
#    df_season['Wins']
#    df_season.loc[:,'Wins'] = np.where('W' in df_season['Result'], 1, 0)

    df_season = df_season_slice.copy(deep=True)
    df_season.loc[:,'Wins'] = df_season['Result'].map(lambda x: 1 if 'W' in x else 0)

    payload = {'pguid': str(pguid),
               'year': df_season['Year'][0],
               'game_count_played': len(df_season.index),
               'fumbles_lost':0, #TODO
               'season_ff_pts': df_season['ff_pts'].sum(),
               'games_won': df_season['Wins'].sum()
              }

    # Conditionally setting the passing, rcving and rushing columns, setting to 0 otherwise
    # Using this syntax:
    # payload['value'] = 'Test' if 1 == 1 else 'NoTest'

    # Passing stats
    payload['pass_tds'] = pd.Series(df_season['Passing_TD']).sum() if 'Passing_TD' in df_season.columns else 0
    payload['pass_yards'] = pd.Series(df_season['Passing_Yds']).sum() if 'Passing_Yds' in df_season.columns else 0
    payload['ints_thrown'] = pd.Series(df_season['Passing_Int']).sum() if 'Passing_Int' in df_season.columns else 0

    # Receiving stats
    payload['rec_tds'] = pd.Series(df_season['Receiving_TD']).sum() if 'Receiving_TD' in df_season.columns else 0
    payload['rec_yards'] = pd.Series(df_season['Receiving_Yds']).sum() if 'Receiving_Yds' in df_season.columns else 0

    # Rushing stats
    payload['rush_tds'] = pd.Series(df_season['Rushing_TD']).sum() if 'Rushing_TD' in df_season.columns else 0
    payload['rush_yards'] = pd.Series(df_season['Rushing_Yds']).sum() if 'Rushing_Yds' in df_season.columns else 0

    # Kick return stats
    payload['kr_tds'] = pd.Series(df_season['Kick Returns_TD']).sum() if 'Kick Returns_TD' in df_season.columns else 0

    # Punt return stats
    payload['pr_tds'] = pd.Series(df_season['Punt Returns_TD']).sum() if 'Punt Returns_TD' in df_season.columns else 0

#    print df_season
    return payload, df_season

'''
    Iterates over the loaded df to calculate ff_points and create game entries
'''
def create_game_entries(pguid, df_career):
    payloads = []
     # Iterate over the dataframe
    for index, row in df_career.iterrows():
        ff_points = calculate_game_ffpoints(row)
#        print ff_points
        df_career.ix[index, 'ff_pts'] = int(ff_points)
        payload = assemble_payload(pguid, row, index, ff_points)
#        print payload
        payloads.append(payload)

    return payloads, df_career
#        create_game_entry(payload)

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
        passing_data = {'gs': row['GS'] if 'GS' in row else None,
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

    data['game_ff_pts'] = int(ff_points)
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
#    print columns_updated

    # Pair the old names to the new names
    columns_map = {}
    for i in range(len(columns_updated)):
        columns_map[columns[i]] = columns_updated[i]

    # Hack so this won't be blank
    columns_map['Unnamed: 6'] = 'Home or Away'
    return columns_map

def create_player_guid(payload):
    r = requests.post("http://127.0.0.1:8000/pfrguids/",
                      auth=HTTPBasicAuth('andrasta', 'aA187759!'),
                      data={'player_name': payload['player_name'],
                            'pos_type': payload['position_type'],
                            'pfr_name': payload['pfr_name']})
    if r.status_code == 400:
        return fetch_player_guid(payload['pfr_name'])
    elif r.status_code == 201:
#        print r.json()
        return str(r.json()['pguid'])
    else:
        sys.exit("Invalid response from create player guid route")

def fetch_player_guid(pfr_name):

    r = requests.get("http://127.0.0.1:8000/pfrguids/"+pfr_name+"/guid", auth=HTTPBasicAuth('andrasta', 'aA187759!'))
#    print "Players guid is: ", str(r.json())
    print r.json()
    pguid = r.json()['pguid']
#    print "pguid is:", pguid
    return pguid

'''
    Executes the request to the Django server to make a game entry
'''
def create_game_entry(payload_list):
    for i in payload_list:
        print "Payload is: ",i
        r = requests.post("http://127.0.0.1:8000/games/",
                          data=i,
                          auth=HTTPBasicAuth('andrasta', 'aA187759!'))
        if r.status_code >= 400:
            print "Error is: ", r.status_code
#            print r.text
#            sys.exit("Error occurred")
        else:
            print r.status_code
            print r.json

'''
    Executes the request to the Django server to make a season entry
'''
def create_season_entry(payload_list):

    print "Payload list is: ", payload_list
    for i in payload_list:
        print "Payload is:", i
        r = requests.post("http://127.0.0.1:8000/seasons/",
                          data=i,
                          auth=HTTPBasicAuth('andrasta', 'aA187759!'))
        print r.status_code
        if r.status_code >= 400:
            print "Error is: ",r.status_code
#            print r.text
#            print "Payload is:", i
#            sys.exit("Error debug")
        else:
            print r.status_code

def create_career_entry(career_payload):
    print "Career payload is: ", career_payload
    r = requests.post("http://127.0.0.1:8000/careers/",
                          data=career_payload,
                          auth=HTTPBasicAuth('andrasta', 'aA187759!'))
    print r.status_code
#    print r.json()
    if r.status_code == 201:
        print r.json()
    elif r.status_code >= 400:
        print "Error is: ", r.status_code
#        print r.text
#        sys.exit("Error in career creation")


def assemble_career_payload(pguid, df_career, payload, active):
    '''
    'pguid', 'ff_pts', 'start_date', 'end_date', 'win_pct'
    '''

    win_pct = df_career['Wins'].sum()/float(len(df_career.index))
    win_pct_formatted = format(win_pct, '.5g')
    win_pct = float(win_pct_formatted)
    win_pct_formatted = format(win_pct, '.3f')
    win_pct = float(win_pct_formatted)
    payload = {'pguid': str(pguid),
               'ff_pts': df_career['ff_pts'].sum(),
               'start_year': payload['start_year'],
               'end_year': payload['end_year'],
               'win_pct': win_pct,
               'active': active,
               'pos_type': payload['position_type'],
               'player_name': payload['player_name']}
    return payload

def assemble_empty_career_payload(pguid, payload, active):
    payload = {'pguid': str(pguid),
               'ff_pts': 0,
               'start_year': payload['start_year'],
               'end_year': payload['end_year'],
               'win_pct': 0,
               'active': active,
               'pos_type': payload['position_type'],
               'player_name': payload['player_name']}
    return payload


parser = argparse.ArgumentParser(description='Converter options.')
parser.add_argument('-input_file', metavar='i',
                   help='Input json file that contains player information')
parser.add_argument('-type',
                   help='sum the integers (default: find the max)')
args = parser.parse_args()
print args.input_file
print args.type
if args.type != "QB_Active" and args.type != "RB_Active" and args.type != "QB_Retired" and args.type != "RB_Retired" and args.type != "WR_Active":
    sys.exit("INVALID TYPE")
# Reading the json file
with open(args.input_file) as data_file:
    data = json.load(data_file)
    print len(data)
    count = 0
    for i in data:
        if count > 2547:
            if args.type == "QB_Active":
                fname = "QB_Active/players_" + i['player_name'].split(" ")[1][0] + "_" + i['pfr_name'] + "_gamelog___stats.csv"
            elif args.type == "RB_Active":
                fname = "RB_Active/players_" + i['player_name'].split(" ")[1][0] + "_" + i['pfr_name'] + "_gamelog___stats.csv"
            elif args.type == "QB_Retired":
                fname = "QB_Retired/players_" + i['player_name'].split(" ")[1][0] + "_" + i['pfr_name'] + "_gamelog___stats.csv"
            elif args.type == "RB_Retired":
                fname = "RB_Retired/players_" + i['player_name'].split(" ")[1][0] + "_" + i['pfr_name'] + "_gamelog___stats.csv"
            elif args.type == "WR_Active":
                fname = "WR_Active/players_" + i['player_name'].split(" ")[1][0] + "_" + i['pfr_name'] + "_gamelog___stats.csv"
            print fname
            active = (args.type.find("Active") != -1)

            pguid = i['pfr_name']
        #        print pguid
            if os.path.isfile(fname):
                print "A csv files for " + i['player_name'] + "," + i['pfr_name'] + " exists is: " + str(os.path.isfile(fname))

                import logging
                try:
                    # Fill out the payloads
                    game_payloads, season_payloads, df_career = get_data(fname, active, pguid)

                    # Call assemble career payload
                    career_payload = assemble_career_payload(pguid, df_career, i, active)
                    print career_payload

                    print "=========================="
                    print "Starting GAMES creation"
                    print "=========================="
                    create_game_entry(game_payloads)
                    print "=========================="
                    print "GAMES creation completed"
                    print "=========================="
                    print "=========================="
                    print "Starting SEASONS creation"
                    print "=========================="
                    create_season_entry(season_payloads)
                    print "=========================="
                    print "SEASONS creation completed"
                    print "=========================="
                    create_career_entry(career_payload)
                except Exception:
                    logging.exception("Error Occurred with" + pguid + " on json line " + str(count))
            else:
#            if not os.path.isfile(fname):
                print "No stats for " + i['player_name'] + " defaulting to zeros"
                career_payload = assemble_empty_career_payload(pguid, i, active)
                create_career_entry(career_payload)

        count += 1
    print count + " players processed"
