#30 * * * * /Users/terryseyler/opt/anaconda3/bin/python "/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/scripts/pull_raiderio_api.py"

import sys
import math
import requests
import json
import pandas as pd
import urllib3
import numpy as np
import sqlite3
from sqlite3 import Error
urllib3.disable_warnings()
pd.set_option('display.max_colwidth', None)
import datetime as dt
from oauthlib.oauth2 import WebApplicationClient

def get_new_token(token_server_url,client_id,client_secret):
    client = WebApplicationClient(client_id)
    data = client.prepare_request_body(
              client_id = client_id,
              client_secret = client_secret
                )

    token_req_payload = {'grant_type': 'client_credentials'}

    token_response = requests.post(token_server_url,
            data=token_req_payload, verify=False, allow_redirects=False,
            auth=(client_id, client_secret))

    if token_response.status_code !=200:
            print("Failed to obtain token from the OAuth 2.0 server", file=sys.stderr)
            return
    tokens = json.loads(token_response.text)
    return tokens['access_token']

def get_client_and_secret():
    warcraftlogs_url = 'https://www.warcraftlogs.com/oauth/token'
    warcraftlogs_client_id = '970b793f-9729-48a2-a782-f75644aa8b63'
    warcraftlogs_client_secret = '4J1ptz33KyYWa3Ou3Ev0MGDCpBd91FqePSrRwfg6'
    bliz_url = 'https://oauth.battle.net/token'
    bliz_client_id = '638402a95f5440a9aac65c1b358b337f'
    bliz_client_secret = '2iDa5FFssrLdbosHAmW2a25Wx1xjGXzh'
    return warcraftlogs_url,warcraftlogs_client_id,warcraftlogs_client_secret,bliz_url,bliz_client_id,bliz_client_secret
warcraftlogs_url,warcraftlogs_client_id,warcraftlogs_client_secret,bliz_url,bliz_client_id,bliz_client_secret = get_client_and_secret()
warcraftlogs_token = get_new_token(warcraftlogs_url,warcraftlogs_client_id,warcraftlogs_client_secret)
bliz_token = get_new_token(bliz_url,bliz_client_id,bliz_client_secret)

def create_connection():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        file ="/home/terrysey/MythicPlus/mplus.db"
        conn = sqlite3.connect(file
        ,detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory=sqlite3.Row
        #engine = create_engine("sqlite:///"+file)
        return conn

    except Exception as e:
        #print(e)
        try:
            file="/Users/terryseyler/git/MythicPlus/mplus.db"
            conn = sqlite3.connect(file
            ,detect_types=sqlite3.PARSE_DECLTYPES)
            conn.row_factory=sqlite3.Row
            #engine = create_engine("sqlite:///"+file)
            return conn
        except Exception as e:
            #print(e)
            try:
                file="C:/Users/tlsey/git/MythicPlus/mplus.db"
                conn = sqlite3.connect(file
                ,detect_types=sqlite3.PARSE_DECLTYPES)
                conn.row_factory=sqlite3.Row
                #engine = create_engine("sqlite:///"+file)
                return conn
            except Exception as e:
                print(e)
try:
    with open('/Users/terryseyler/git/mythicplus/scripts/characters.txt') as f:
        data = f.read()
    f.close()
except:
    try:
        with open('C:\\Users\\tlsey\\git\\MythicPlus\\scripts\\characters.txt') as f:
            data = f.read()
        f.close()
    except:
        try:
            with open("/home/terrysey/MythicPlus/scripts/characters.txt") as f:
                data = f.read()
            f.close()
        except Exception as e:
            print(e)
conn = create_connection()
conn.execute('delete from mythic_plus_best_runs')
conn.execute('delete from warcraftlogs_raid_allstars')
conn.execute('delete from warcraftlogs_raid')
conn.execute('delete from warcraftlogs_raid_encounter')
conn.commit()

# reconstructing the data as a dictionary
character_json = json.loads(data)

now_string = dt.datetime.now().strftime("%Y-%m-%d")
now__time_string = dt.datetime.now().strftime("%m-%d-%Y %H:%M")
conn.execute('delete from base_characters')
conn.execute('delete from season_best_pivot_df_s1 where scoreboard_date = {}'.format(now_string))
conn.commit()
conn.close()
for char in character_json:
    conn=create_connection()
    conn.execute("""insert or ignore into season_best_pivot_df_s1 (
    name
    ,realm
    ,region


    ,scoreboard_date
    ,unique_key
    )
    VALUES(?,?,?,?,?)

    """,(
                        char['name']
                                ,char['server']
                                ,char['region']
                                ,now_string
                                ,char['name']+char['server']+char['region']+now_string
                    )
                    )
    conn.execute(
    """insert or IGNORE into base_characters(
        name
        ,realm
        ,region
        ,unique_key
        )
        values(?,?,?,?)"""
        ,(char['name'],char['server'],char['region'],char['name']+char['server']+char['region'])
        )
    conn.commit()
    print('{} being pulled'.format(char['name']))
    r = requests.get('https://raider.io/api/v1/characters/profile?region={}&realm={}&name={}&fields=mythic_plus_best_runs%2Cmythic_plus_highest_level_runs%2Cmythic_plus_alternate_runs%2Cgear%2Cmythic_plus_weekly_highest_level_runs%2Cmythic_plus_previous_weekly_highest_level_runs'.format(char['region'],char['server'],char['name']))
    print("Raider IO status code - {}".format(r.status_code))
    r_bliz_equip = requests.get('https://us.api.blizzard.com/profile/wow/character/{0}/{1}/equipment?namespace=profile-us&locale=en_US&access_token={2}'.format(char['server'].lower(),char['name'].lower(),bliz_token))
    print("Bliz equip status code = {}".format(r_bliz_equip.status_code))
    r_bliz_profile = requests.get('https://us.api.blizzard.com/profile/wow/character/{0}/{1}?namespace=profile-us&locale=en_US&access_token={2}'.format(char['server'].lower(),char['name'].lower(),bliz_token))
    print('bliz profile status code - {}'.format(r_bliz_profile.status_code))
    query = """
    query {{
        characterData {{
            character(name:"{0}", serverSlug: "{1}", serverRegion: "{2}") {{
                zoneRankings(byBracket: true, timeframe: Historical, difficulty:3)
                    }}
                }}
            }}

    """.format(char['name'],char['server'],char['region'])

    r_warcraft_logs = requests.get('https://www.warcraftlogs.com/api/v2/client', json={'query': query},headers={
        'Authorization': 'Bearer ' + warcraftlogs_token
        ,'content-type':'application/json'
    })
    print('warcraft logs status code - {}'.format(r_warcraft_logs.status_code))

    if r_warcraft_logs.status_code==200:
        j_warcraft_logs = json.loads(r_warcraft_logs.text)
        try:
            zoneRankings= j_warcraft_logs['data']['characterData']['character']['zoneRankings']
            zoneRankings_Allstar = j_warcraft_logs['data']['characterData']['character']['zoneRankings']['allStars']
            encounterRankings =j_warcraft_logs['data']['characterData']['character']['zoneRankings']['rankings']
        except:
            print('could not parse warcraft logs api')
        try:
            for enc in encounterRankings:
                #print(enc['allStars'])
                if enc['allStars'] == None:

                    enc_allstar_points = 0
                    enc_allstar_possible_points=0
                    enc_allstar_partition=0
                    enc_allstar_rank=0
                    enc_allstar_region_rank=0
                    enc_allstar_server_rank=0
                    enc_allstar_rank_percent=0
                    enc_allstar_total=0
                    spec='none'
                    bestspec='none'
                else:
                    enc_allstar_points = enc['allStars']['points']
                    enc_allstar_possible_points=enc['allStars']['possiblePoints']
                    enc_allstar_partition=enc['allStars']['partition']
                    enc_allstar_rank=enc['allStars']['rank']
                    enc_allstar_region_rank=enc['allStars']['regionRank']
                    enc_allstar_server_rank=enc['allStars']['serverRank']
                    enc_allstar_rank_percent=enc['allStars']['rankPercent']
                    enc_allstar_total=enc['allStars']['total']
                    spec=enc['spec']
                    bestspec=enc['bestSpec']

                conn.execute("""insert or replace into warcraftlogs_raid_encounter
                        (
                        character_name
                        ,realm
                        ,region

                        ,zone
                        ,partition

                        ,encounter_id
                        ,encounter_name
                        ,rankPercent
                        ,medianPercent
                        ,lockedIn
                        ,totalKills
                        ,fastestKill
                        ,allstars_points
                        ,allstars_possiblePoints
                        ,allstars_partition
                        ,allstars_rank
                        ,allstars_region_rank
                        ,allstars_server_rank
                        ,allstars_rank_percent
                        ,allstars_total
                        ,spec
                        ,bestSpec
                        ,bestAmount
                        ,unique_key


                        )values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                            (char['name']
                            ,char['server']
                            ,char['region']
                            ,zoneRankings['zone']
                            ,zoneRankings['partition']
                            ,enc['encounter']['id']
                            ,enc['encounter']['name']
                            ,enc['rankPercent']
                            ,enc['medianPercent']
                            ,enc['lockedIn']
                            ,enc['totalKills']
                            ,enc['fastestKill']
                                ,enc_allstar_points
                                ,enc_allstar_possible_points
                                ,enc_allstar_partition
                                ,enc_allstar_rank
                                ,enc_allstar_region_rank
                                ,enc_allstar_server_rank
                                ,enc_allstar_rank_percent
                                ,enc_allstar_total
                            ,spec
                            ,bestspec
                            ,enc['bestAmount']
                            ,char['name'] + char['server'] + char['region']  + str(enc['encounter']['id'])
                                    ))
            conn.commit()
        except:
            print('could not insert into warcraftlogs_raid_encounter')
        try:
            for rank in zoneRankings_Allstar:
                conn.execute("""insert or replace into warcraftlogs_raid_allstars
                                (
                            character_name
                            ,realm
                            ,region

                            ,zone

                            ,partition
                            ,spec
                            ,points
                            ,possiblePoints
                            ,rank
                            ,regionRank
                            ,serverRank
                            ,rankPercent
                            ,total
                            ,unique_key

                            )VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,(
                    char['name']
                            ,char['server']
                            ,char['region']
                    ,zoneRankings['zone']
                    ,rank['partition']
                        ,rank['spec']
                        ,rank['points']
                        ,rank['possiblePoints']
                        ,rank['rank']
                        ,rank['regionRank']
                        ,rank['serverRank']
                        ,rank['rankPercent']
                        ,rank['total']
                        ,char['name'] + char['server'] + char['region']  + rank['spec'] + str(rank['partition']) + str(zoneRankings['zone'])
                    ))
            conn.commit()
        except:
            print('could not insert into warcraftlogs_raid_allstars')
        try:
            conn.execute("""insert or replace into warcraftlogs_raid
                    (
                    character_name
                        ,realm
                        ,region
                        ,bestPerformanceAverage
                        ,medianPerformanceAverage
                        ,difficulty
                        ,metric
                        ,partition
                        ,zone
                        ,unique_key

                    )VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,(
            char['name']
                            ,char['server']
                            ,char['region']
            ,zoneRankings['bestPerformanceAverage']
                ,zoneRankings['medianPerformanceAverage']
                ,zoneRankings['difficulty']
                ,zoneRankings['metric']
                ,zoneRankings['partition']
                ,zoneRankings['zone']
                ,char['name'] + char['server'] + char['region'] + str(zoneRankings['partition']) + str(zoneRankings['zone'])
            ))
            conn.commit()
        except:
            print('could not insert into warcraftlogs_raid')

    if r_bliz_profile.status_code== 200:
        j_profile = json.loads(r_bliz_profile.text)
        active_spec_name = j_profile['active_spec']['name']
        character_class = j_profile['character_class']['name']
        active_spec = j_profile['active_spec']['name']
    else:
        print('could not pull player profile')
    affixes=[]
    if r.status_code == 200:

        print('{} api pulled'.format(char['name']))
        j = json.loads(r.text)
        for dungeon in j['mythic_plus_alternate_runs']:
            dungeon['type'] = 'alt'
            dungeon['rating'] = dungeon['score'] * 0.5
        for dungeon in j['mythic_plus_best_runs']:
            dungeon['type'] = 'primary'
            dungeon['rating'] = dungeon['score'] * 1.5

        all_best_runs = j['mythic_plus_best_runs'] + j['mythic_plus_alternate_runs']

        for i,dungeon in enumerate(all_best_runs):
            all_best_runs[i]['affix_names'] = [affix['name'] for affix in dungeon['affixes']]
        #18 columns
        all_mythic_dungeons = j['mythic_plus_best_runs'] + j['mythic_plus_alternate_runs'] + j['mythic_plus_previous_weekly_highest_level_runs'] + j['mythic_plus_weekly_highest_level_runs']
        for i,dungeon in enumerate(all_mythic_dungeons):
            all_mythic_dungeons[i]['affix_names'] = [affix['name'] for affix in dungeon['affixes']]
        #insert into mythic_plus_best_runs
        total_rating =sum([dungeon['rating'] for dungeon in all_best_runs])
        #print('best runs')

        for dungeon in all_best_runs:
            try:
                #print(dungeon['name'])
                conn.execute(
                        """INSERT INTO mythic_plus_best_runs (
                            name
                            ,region
                            ,realm
                            ,dungeon
                            ,short_name
                            ,mythic_level
                            ,completed_at
                            ,clear_time_ms
                            ,num_keystone_upgrades
                            ,map_challenge_mode_id
                            ,zone_id
                            ,score
                            ,affixes
                            ,URL
                            ,unique_key
                            ,tyr_or_fort
                            ,type
                            ,rating
                            ,active_spec_name
                            ,active_spec_role
                            ,class
                            )
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (j['name']
                            ,j['region']
                            ,j['realm']
                            ,dungeon['dungeon']
                            ,dungeon['short_name']
                            ,dungeon['mythic_level']
                            ,dungeon['completed_at']
                            ,dungeon['clear_time_ms']
                            ,dungeon['num_keystone_upgrades']
                            ,dungeon['map_challenge_mode_id']
                            ,dungeon['zone_id']
                            ,dungeon['score']
                            ,str(dungeon['affix_names'])
                            ,dungeon['url']
                            ,j['name']  + j['region']  + j['realm'] + '-' + dungeon['dungeon'] + dungeon['type']
                            ,dungeon['affixes'][0]['name']
                            ,dungeon['type']
                            ,dungeon['rating']
                            ,active_spec_name
                            ,j['active_spec_role']
                            ,j['class']
                                )
                )
            except Exception as e:
                print(e)
                print('could not insert into mythic_plus_best_runs')
            #print(dungeon['name'])
            conn.commit()
            #print('new stuff')
            try:
                print(dungeon['dungeon'])
                print('{0} {1}'.format(dungeon['dungeon']
                                        ,dungeon['affixes'][0]['name']))
                num_keystone_upgrade_asterisk = np.where (dungeon['num_keystone_upgrades']==3,'***'
                                            , np.where (dungeon['num_keystone_upgrades']==2,'**'
                                                        , np.where (dungeon['num_keystone_upgrades']==1,'*','')))

                level_and_upgrade= str(dungeon['mythic_level']) +' '+ str(num_keystone_upgrade_asterisk)
                update_sql = """update season_best_pivot_df_s1
                set "{0} {1}" =  "{2}"
                ,total_rating = {3}
                where name = "{4}"
                and scoreboard_date = "{5}" """.format(dungeon['dungeon']
                                        ,dungeon['affixes'][0]['name']
                                        ,level_and_upgrade
                                        ,total_rating
                                        ,j['name']
                                        ,now_string)
                print(update_sql)
                conn.execute(update_sql)
                #print('new complete')
                conn.commit()
            except Exception as e:
                print(e)
                print('did not update season pivot')
        #print('all_mythic_dungeons')
        for dungeon in all_mythic_dungeons:
            try:
                conn.execute(
                        """INSERT OR IGNORE INTO all_mythic_plus_runs (
                            name
                            ,region
                            ,realm
                            ,dungeon
                            ,short_name
                            ,mythic_level
                            ,completed_at
                            ,clear_time_ms
                            ,num_keystone_upgrades
                            ,map_challenge_mode_id
                            ,zone_id
                            ,score
                            ,affixes
                            ,URL
                            ,unique_key
                            ,tyr_or_fort
                            ,active_spec_name
                            ,active_spec_role
                            ,class
                            )
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (j['name']
                            ,j['region']
                            ,j['realm']
                            ,dungeon['dungeon']
                            ,dungeon['short_name']
                            ,dungeon['mythic_level']
                            ,dungeon['completed_at']
                            ,dungeon['clear_time_ms']
                            ,dungeon['num_keystone_upgrades']
                            ,dungeon['map_challenge_mode_id']
                            ,dungeon['zone_id']
                            ,dungeon['score']
                            ,str(dungeon['affix_names'])
                            ,dungeon['url']
                            ,j['name'] + '-' + j['region'] + '-' + j['realm'] + '-' + dungeon['completed_at']
                            ,dungeon['affixes'][0]['name']
                            ,active_spec_name
                            ,j['active_spec_role']
                            ,j['class']

                                )
                        )
                conn.commit()
            except Exception as e:
                print(e)
                print('could not update all mythic plus runs')

    else:
        print('raider io did not work {}'.format(char['name']))
    if r_bliz_equip.status_code == 200:
        j_equip = json.loads(r_bliz_equip.text)
        sum_item_level = 0
        item_count = 0
        offhand = False

        for item in j_equip['equipped_items']:
            if item['slot']['name'] == 'Off Hand':
                #print('offhand')
                offhand=True
        if offhand == True:
            for item in j_equip['equipped_items']:
                if item['slot']['name'] != 'Shirt' and item['slot']['name'] != 'Tabard':
                    sum_item_level = sum_item_level + item['level']['value']
                    item_count = item_count + 1

        else:
            for item in j_equip['equipped_items']:
                if item['slot']['name'] =='Main Hand':
                    sum_item_level = sum_item_level + item['level']['value']*2
                    item_count = item_count + 2
                elif item['slot']['name'] != 'Shirt' and item['slot']['name'] != 'Tabard':
                    sum_item_level = sum_item_level + item['level']['value']
                    item_count = item_count + 1

        try:
            derived_item_level = sum_item_level / item_count
        except Exception as e:
            derived_item_level = 0
        #print('gear')
        for item in j_equip['equipped_items']:
            try:
                conn.execute(
                        """INSERT OR REPLACE INTO character_gear (
                            name
                            ,region
                            ,realm
                            ,last_crawled_at
                            ,equipped_item_level
                            ,item_slot
                            ,item_level
                            ,item_name
                            ,unique_key
                            ,active_spec_name
                            ,active_spec_role
                            ,derived_item_level
                            ,class
                        )
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (char['name']
                        ,char['region']
                        ,char['server']
                        ,now__time_string
                        ,math.floor(derived_item_level)
                        ,item['slot']['name'].lower().replace(" ", "")
                        ,item['level']['value']
                        ,item['name']
                        ,char['name']+char['server']+char['region']+item['slot']['name'].lower().replace(" ", "")+str(derived_item_level)
                        ,active_spec_name
                            ,'NO ROLE'
                            ,derived_item_level
                        ,character_class
                            )
                        )
                conn.commit()
            except Exception as e:
                print(e)
                print('could not insert into character gear')
    else:
        print('blizzard api did not work {}'.format(char['name']))
    conn.close()

print('updating pivot')
conn=create_connection()
df_db = pd.read_sql('select * from mythic_plus_best_runs',conn)
df_db['name_and_server'] = df_db['name'] +'-'+df_db['realm']
df_db['dungeon_and_affix'] = df_db['dungeon'] + ' ' + df_db['tyr_or_fort']

df_db['num_keystone_upgrade_asterisk'] = np.where (df_db['num_keystone_upgrades']==3,'***'
                                            , np.where (df_db['num_keystone_upgrades']==2,'**'
                                                        , np.where (df_db['num_keystone_upgrades']==1,'*','')))

df_db['level_and_upgrade'] = df_db['mythic_level'].astype(str) +' '+ df_db['num_keystone_upgrade_asterisk'].astype(str)

df_total_rating= df_db.pivot(index=['name','realm','region'], columns=['dungeon_and_affix'], values='rating')
df_total_rating['total_rating'] = round(df_total_rating.sum(axis=1),1)

df_total_rating.replace(np.nan,0,inplace=True)
df_total_rating.sort_values(['name','realm','region'],ascending=True,inplace=True)

df_season_best = df_db.pivot(index=['name','realm','region']
                 , columns=['dungeon_and_affix']
                 , values='level_and_upgrade')
df_season_best.replace(np.nan,'',inplace=True)

df_season_best['total_rating'] = df_total_rating['total_rating']

df_season_best['scoreboard_date'] =now_string

conn.execute('delete from season_best_pivot where scoreboard_date = "{}"'.format(now_string))
conn.commit()
#df_season_best.to_sql('season_best_pivot',conn,if_exists='append')
conn.commit()
#update the gear table with increases
print("updating scoreboard changes")
conn.execute("drop table if exists season_best_pivot_ext")
conn.commit()
conn.execute("""create table season_best_pivot_ext as
            select base.name
            ,base.realm
            ,base.region
            ,cur.scoreboard_date
            ,coalesce(cur.total_rating,0) as total_rating

,coalesce(cur."Algeth'ar Academy Fortified","") as "Algeth'ar Academy Fortified"
,coalesce(cur."Algeth'ar Academy Tyrannical","") as "Algeth'ar Academy Tyrannical"

,coalesce(cur."Court of Stars Fortified","") as "Court of Stars Fortified"
,coalesce(cur."Court of Stars Tyrannical","") as "Court of Stars Tyrannical"

,coalesce(cur."Halls of Valor Fortified","")  as "Halls of Valor Fortified"
,coalesce(cur."Halls of Valor Tyrannical","")  as "Halls of Valor Tyrannical"

,coalesce(cur."Ruby Life Pools Fortified","") as "Ruby Life Pools Fortified"
,coalesce(cur."Ruby Life Pools Tyrannical","") as "Ruby Life Pools Tyrannical"

,coalesce(cur."Shadowmoon Burial Grounds Fortified","") as "Shadowmoon Burial Grounds Fortified"
,coalesce(cur."Shadowmoon Burial Grounds Tyrannical","") as "Shadowmoon Burial Grounds Tyrannical"

,coalesce(cur."Temple of the Jade Serpent Fortified","") as "Temple of the Jade Serpent Fortified"
,coalesce(cur."Temple of the Jade Serpent Tyrannical","") as "Temple of the Jade Serpent Tyrannical"

,coalesce(cur."The Azure Vault Fortified","")  as "The Azure Vault Fortified"
,coalesce(cur."The Azure Vault Tyrannical","") as "The Azure Vault Tyrannical"

,coalesce(cur."The Nokhud Offensive Fortified","") as "The Nokhud Offensive Fortified"
,coalesce(cur."The Nokhud Offensive Tyrannical","") as "The Nokhud Offensive Tyrannical"



,coalesce(round(cur.total_rating - pr.total_rating,1),cur.total_rating,0) as daily_rating_change

, pr."Algeth'ar Academy Fortified" as pr_AA_for
, pr."Algeth'ar Academy Tyrannical" as pr_AA_tyr

, pr."Court of Stars Fortified" as pr_COS_for
, pr."Court of Stars Tyrannical" as pr_COS_tyr

            ,  pr."Halls of Valor Fortified" as pr_HOV_for
            ,  pr."Halls of Valor Tyrannical" as pr_HOV_tyr

            ,  pr."Ruby Life Pools Fortified" as pr_RUBY_for
            ,  pr."Ruby Life Pools Tyrannical" as pr_RUBY_tyr

            ,   pr."Shadowmoon Burial Grounds Fortified" as pr_SBG_for
            ,   pr."Shadowmoon Burial Grounds Tyrannical" as pr_SBG_tyr

            ,   pr."Temple of the Jade Serpent Fortified" as pr_JADE_for
            ,  pr."Temple of the Jade Serpent Tyrannical" as pr_JADE_tyr

            ,  pr."The Azure Vault Fortified" as pr_AV_for
            ,  pr."The Azure Vault Tyrannical" as pr_AV_tyr

            ,  pr."The Nokhud Offensive Fortified" as pr_NOKH_for
            ,  pr."The Nokhud Offensive Tyrannical" as pr_NOKH_tyr

            from base_characters base
            inner join  season_best_pivot_df_s1 cur
            on base.name=cur.name
            and base.realm=cur.realm
            and base.region=cur.region

left join season_best_pivot_df_s1 pr
            on pr.name = cur.name
            and pr.realm = cur.realm
            and pr.region = cur.region
            and date(pr.scoreboard_date) = date(cur.scoreboard_date,
                                            case when strftime("%w",cur.scoreboard_date) = "0" then "-5 "
                                            when strftime("%w",cur.scoreboard_date) = "1" then  "-6 "
                                            when strftime("%w",cur.scoreboard_date) = "2" then "-7 "
                                            when strftime("%w",cur.scoreboard_date) = "3" then "-1 "
                                            when strftime("%w",cur.scoreboard_date) = "4" then "-2 "
                                            when strftime("%w",cur.scoreboard_date) = "5" then  "-3 "
                                            when strftime("%w",cur.scoreboard_date) = "6" then "-4 "
                                            end || "days")
            """
            )
conn.commit()
conn.execute("""update season_best_pivot_ext
set scoreboard_date = "{}"
,daily_rating_change=0
where scoreboard_date is null""".format(now_string))


conn.commit()
conn.execute("drop table character_gear_ext")
conn.execute("""create table character_gear_ext as
            select gear.name
            ,gear.realm
            ,gear.region
            ,gear.item_level
            ,gear.item_slot
            ,gear.item_name
            ,gear.derived_item_level
            ,gear.last_crawled_at
            ,gear.equipped_item_level

            ,gear.class
            ,gear.unique_key
            ,gear.active_spec_name
            ,gear.active_spec_role

            ,item_level.slot_item_level_change
            from
            (
            select * from character_gear
            )gear
            left join
            (
            select
            unique_key
            ,item_level - LEAD(item_level,1,0) over (partition by name,realm,region,item_slot order by last_crawled_at desc) as slot_item_level_change
            from character_gear
            )item_level

            on item_level.unique_key = gear.unique_key

            """)
print("gear table updated")
conn.execute('alter table character_gear_ext add column item_lookup')
conn.execute("""update character_gear_ext
             set item_lookup = case when item_slot ="shoulders" then "shoulder"
                                 when item_slot = "trinket1" then "trinket"
                                 when item_slot = "trinket2" then "trinket"
                                 when item_slot = "ring1" then "finger"
                                 when item_slot = "ring2" then "finger"
                                 when item_slot = "back" then "cloak"
                                 when item_slot = "hands" then "hand"
                                 else item_slot
             end
             """)
conn.execute("""update df_season_one_loot
             set item_lookup = case when inventory_type in ("RANGEDRIGHT","TWOHWEAPON","WEAPON","RANGED") then "mainhand"
             when inventory_type in ("HOLDABLE","SHIELD") then "offhand"

             else inventory_type end
             """)
conn.commit()
conn.close()
print('pivot updated')
