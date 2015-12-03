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


def fetch_all_players():
    r = requests.get("http://127.0.0.1:8000/careers", auth=HTTPBasicAuth('andrasta', 'aA187759!'))
    return r.json()

#def fetch_seasons(pguid):
#    r = requests.get("http://127.0.0.1:8000/seasons/?starts_with=" + pguid, auth=HTTPBasicAuth('andrasta', 'aA187759!'))
#    if r.status_code < 400:
#        return r.json()['results']
#
#def fetch_games(pguid):
#    r = requests.get("http://127.0.0.1:8000/games/?start_with=" + pguid, auth=HTTPBasicAuth('andrasta', 'aA187759!'))
#    if r.status_code < 400:
#        return r.json()['results']
#
#def update_game(game_guid, player_name):
#    payload = {'player_name': player_name}
#    r = requests.put("http://127.0.0.1:8000/games/" + game_guid,
#                      data=payload,
#                      auth=HTTPBasicAuth('andrasta', 'aA187759!'))
#    return r.status_code
#
#def update_season(season, player_name):
##    payload = season
#    payload = {'player_name': player_name}
#    print payload
#    r = requests.post("http://127.0.0.1:8000/seasons/" + season['season_guid'],
#                      data=payload,
#                      auth=HTTPBasicAuth('andrasta', 'aA187759!'))
##    if r.status_code >= 400:
#    print r.content

def update_ff_pts(player):
    payload = {'ff_pts': player['ff_pts']+2}
    print 'Payload is:', payload
    r = requests.post("http://127.0.0.1:8000/careers/"+player['pguid']+"/",
                      data=payload,
                      headers = {'X-HTTP-Method-Override': 'PATCH'},
                      auth=HTTPBasicAuth('andrasta', 'aA187759!'))
    print r.status_code
#    print r.json()
    print r.text
#    return r.json()
players = fetch_all_players()['results']
#print players
#print len(players)
for i in players:
    update_ff_pts(i)
    sys.exit("DONE")
#    seasons = fetch_seasons(i['pguid'])
#    for j in seasons:
#        status = update_season(j, i['player_name'])
#        print status
#        sys.exit("STOPPED")
#    print seasons


#    sys.exit("COMPLETED")
#    print i['player_name']
#for i in players:

