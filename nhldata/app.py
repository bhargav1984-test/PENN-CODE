'''
	This is the NHL crawler.

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse
import json  # we have import this Bhargav
import csv
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'

    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        '''
        returns a dict tree structure that is like
            "dates": [
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home"
                }
            }

            See tests/resources/boxscore.json for a real example response
        '''
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'


@dataclass
class StorageKey:
    # TODO what propertie are needed to partition?
    gameid: str

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        # TODO use the properties to return the s3 key
        return f'{self.gameid}.csv'


class json_parser():
    def boxscore_parser(gameid):
        api = NHLApi()
        get_players = api.boxscore(gameid)
        csv_list = []
        if 'teams' in get_players.keys() and 'home' in get_players['teams'].keys() and 'players' in get_players['teams']['home'].keys():
            for j in get_players['teams']['home']['players']:
                i = get_players['teams']['home']['players'][j]
                dict_csv = {}
                if 'skaterStats' in i['stats'].keys():
                    dict_csv['player_person_id'] = i['person']['id']
                    if 'currentTeam' in i['person'].keys():
                        dict_csv['player_person_currentTeam_name'] = i['person']['currentTeam']['name']
                    else:
                        dict_csv['player_person_currentTeam_name'] = ''
                    dict_csv['player_person_fullName'] = i['person']['fullName']
                    dict_csv['player_stats_skaterStats_assists'] = i['stats']['skaterStats']['assists']
                    dict_csv['player_stats_skaterStats_goals'] = i['stats']['skaterStats']['goals']
                    dict_csv['side'] = 'home'
                if len(dict_csv.keys()) > 1:
                    csv_list.append(dict_csv)

            for j in get_players['teams']['away']['players']:
                i = get_players['teams']['away']['players'][j]
                dict_csv = {}
                if 'skaterStats' in i['stats'].keys():
                    dict_csv['player_person_id'] = i['person']['id']
                    if 'currentTeam' in i['person'].keys():
                        dict_csv['player_person_currentTeam_name'] = i['person']['currentTeam']['name']
                    else:
                        dict_csv['player_person_currentTeam_name'] = ''
                    dict_csv['player_person_fullName'] = i['person']['fullName']
                    dict_csv['player_stats_skaterStats_assists'] = i['stats']['skaterStats']['assists']
                    dict_csv['player_stats_skaterStats_goals'] = i['stats']['skaterStats']['goals']
                    dict_csv['side'] = 'away'

                if len(dict_csv.keys()) > 1:
                    csv_list.append(dict_csv)
        return(csv_list)


class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True


class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    def crawl(self, startDate: datetime, endDate: datetime) -> None:

        # NOTE the data direct from the API is not quite what we want. Its nested in a way we don't want
        #      so here we are looking for your ability to gently massage a data set.
        api = NHLApi()
        result = api.schedule(startDate, endDate)
        #resultset = json.loads(result)
        print(result.keys())
        l1 = result['dates']
        for i in l1:
            for j in i['games']:
                print(j['gamePk'])
                list_csv = json_parser.boxscore_parser(j['gamePk'])
                print(list_csv)
                print('writing to file now')
                with open('create_games_stats.csv', 'w') as csvfile:
                    fields = list_csv[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fields)
                    writer.writerows(list_csv)

    # TODO error handling
    # TODO get games for dates
    # TODO for each game get all player stats: schedule -> date -> teams.[home|away] -> $playerId: player_object (see boxscore above)
    # TODO ignore goalies (players with "goalieStats")
    # TODO output to S3 should be a csv that matches the schema of utils/create_games_stats


def main():
    import os
    import argparse
    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    # TODO what arguments are needed to make this thing run,  if any?
    args = parser.parse_args()
    dest_bucket = os.environ.get('DEST_BUCKET', 'output')
    startDate = datetime(2020, 1, 2, 00, 00, 00, 000)
    startDate.strftime('%Y-%m-%d')
    endDate = datetime(2020, 1, 2, 23, 59, 59, 000)
    endDate.strftime('%Y-%m-%d')
    api = NHLApi()
    s3client = boto3.client('s3', config=Config(signature_version='s3v4'),
                            endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage)
    crawler.crawl(startDate, endDate)


if __name__ == '__main__':
    main()
