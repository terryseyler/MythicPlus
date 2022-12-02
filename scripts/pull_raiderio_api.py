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
    bliz_url = 'https://oauth.battle.net/token'
    bliz_client_id = '638402a95f5440a9aac65c1b358b337f'
    bliz_client_secret = '2iDa5FFssrLdbosHAmW2a25Wx1xjGXzh'
    return bliz_url,bliz_client_id,bliz_client_secret
bliz_url,bliz_client_id,bliz_client_secret = get_client_and_secret()

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

    except Error as e:
        #print(e)
        try:
            file="/Users/terryseyler/git/MythicPlus/mplus.db"
            conn = sqlite3.connect(file
            ,detect_types=sqlite3.PARSE_DECLTYPES)
            conn.row_factory=sqlite3.Row
            #engine = create_engine("sqlite:///"+file)
            return conn
        except Error as e:
            #print(e)
            try:
                file="C:/Users/tlsey/git/MythicPlus/mplus.db"
                conn = sqlite3.connect(file
                ,detect_types=sqlite3.PARSE_DECLTYPES)
                conn.row_factory=sqlite3.Row
                #engine = create_engine("sqlite:///"+file)
                return conn
            except Error as e:
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
        except Error as e:
            print(e)
conn = create_connection()
conn.execute('delete from mythic_plus_best_runs')
conn.commit()
conn.close()
# reconstructing the data as a dictionary
character_json = json.loads(data)

now_string = dt.datetime.now().strftime("%Y-%m-%d")
now__time_string = dt.datetime.now().strftime("%m-%d-%Y %H:%M")
for char in character_json:
    conn=create_connection()
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
    r_bliz_equip = requests.get('https://us.api.blizzard.com/profile/wow/character/{0}/{1}/equipment?namespace=profile-us&locale=en_US&access_token={2}'.format(char['server'].lower(),char['name'].lower(),bliz_token))
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


        for dungeon in all_best_runs:
            conn.execute(
                      """INSERT OR IGNORE INTO mythic_plus_best_runs (
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
                        ,j['name'] + '-' + j['region'] + '-' + j['realm'] + '-' + dungeon['dungeon'] + dungeon['type']
                        ,dungeon['affixes'][0]['name']
                        ,dungeon['type']
                        ,dungeon['rating']
                        ,j['active_spec_name']
                        ,j['active_spec_role']
                        ,j['class']
                            )
            )
            conn.commit()
        for dungeon in all_mythic_dungeons:
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
                        ,j['active_spec_name']
                        ,j['active_spec_role']
                        ,j['class']

                            )
                    )
            conn.commit()
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

        for item in j_equip['equipped_items']:
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
                     ,char['name']+char['server']+char['region']+item['slot']['name'].lower().replace(" ", "")+item['name']+str(item['level']['value'])+str(derived_item_level)
                        ,j['active_spec_name']
                        ,j['active_spec_role']
                        ,derived_item_level
                     ,j['class']
                        )
                    )
            conn.commit()
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
df_season_best.to_sql('season_best_pivot',conn,if_exists='append')
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

            ,coalesce(cur."Tazavesh: So\'leah\'s Gambit Fortified","") as "Tazavesh: So\'leah\'s Gambit Fortified"
            ,coalesce(cur."Tazavesh: So\'leah\'s Gambit Tyrannical","") as "Tazavesh: So\'leah\'s Gambit Tyrannical"

            ,coalesce(cur."Tazavesh: Streets of Wonder Fortified","") as "Tazavesh: Streets of Wonder Fortified"
            ,coalesce(cur."Tazavesh: Streets of Wonder Tyrannical","") as "Tazavesh: Streets of Wonder Tyrannical"

            ,coalesce(cur."Return to Karazhan: Upper Fortified","")  as "Return to Karazhan: Upper Fortified"
            ,coalesce(cur."Return to Karazhan: Upper Tyrannical","")  as "Return to Karazhan: Upper Tyrannical"

            ,coalesce(cur."Return to Karazhan: Lower Fortified","") as "Return to Karazhan: Lower Fortified"
            ,coalesce(cur."Return to Karazhan: Lower Tyrannical","") as "Return to Karazhan: Lower Tyrannical"

            ,coalesce(cur."Iron Docks Fortified","") as "Iron Docks Fortified"
            ,coalesce(cur."Iron Docks Tyrannical","") as "Iron Docks Tyrannical"

            ,coalesce(cur."Grimrail Depot Fortified","")  as "Grimrail Depot Fortified"
            ,coalesce(cur."Grimrail Depot Tyrannical","") as "Grimrail Depot Tyrannical"

            ,coalesce(cur."Mechagon Workshop Fortified","") as "Mechagon Workshop Fortified"
            ,coalesce(cur."Mechagon Workshop Tyrannical","") as "Mechagon Workshop Tyrannical"

            ,coalesce(cur."Mechagon Junkyard Fortified","") as "Mechagon Junkyard Fortified"
            ,coalesce(cur."Mechagon Junkyard Tyrannical","") as "Mechagon Junkyard Tyrannical"

            ,coalesce(round(cur.total_rating - pr.total_rating,1),cur.total_rating,0) as daily_rating_change

            , pr."Tazavesh: So\'leah\'s Gambit Fortified" as pr_GMBT_for
            , pr."Tazavesh: So\'leah\'s Gambit Tyrannical" as pr_GMBT_tyr

            , pr."Tazavesh: Streets of Wonder Fortified" as pr_STRT_for
            ,   pr."Tazavesh: Streets of Wonder Tyrannical" as pr_STRT_tyr

            ,  pr."Return to Karazhan: Upper Fortified" as pr_UPPR_for
            ,  pr."Return to Karazhan: Upper Tyrannical" as pr_UPPR_tyr

            ,  pr."Return to Karazhan: Lower Fortified" as pr_LOWR_for
            ,  pr."Return to Karazhan: Lower Tyrannical" as pr_LOWR_tyr

            ,   pr."Iron Docks Fortified" as pr_ID_for
            ,   pr."Iron Docks Tyrannical" as pr_ID_tyr

            ,   pr."Grimrail Depot Fortified" as pr_GD_for
            ,  pr."Grimrail Depot Tyrannical" as pr_GD_tyr

            ,  pr."Mechagon Workshop Fortified" as pr_WKSP_for
            ,  pr."Mechagon Workshop Tyrannical" as pr_WKSP_tyr

            ,  pr."Mechagon Junkyard Fortified" as pr_JUNK_for
            ,  pr."Mechagon Junkyard Tyrannical" as pr_JUNK_tyr

            from base_characters base
            left join season_best_pivot cur
            on base.name=cur.name
            and base.realm=cur.realm
            and base.region=cur.region

            left join season_best_pivot pr
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
conn.execute("""update season_best_pivot_ext
set scoreboard_date = "2022-11-26"
where name = 'Astari'
""")
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
