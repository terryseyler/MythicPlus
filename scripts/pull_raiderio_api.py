#30 * * * * /Users/terryseyler/opt/anaconda3/bin/python "/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/scripts/pull_raiderio_api.py"



import requests
import sys
import json
import pandas as pd
import urllib3
import numpy as np
import sqlite3
from sqlite3 import Error
urllib3.disable_warnings()
pd.set_option('display.max_colwidth', None)


def create_connection():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        #print('trying python anywhere path')
        file ="/home/terrysey/MythicPlus/mplus.db"
        conn = sqlite3.connect(file)
        #engine = create_engine("sqlite:///"+file
        return conn

    except Error as e:
        #print(e)
        try:
           # print('trying local path')
            file='/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/mplus.db'
            conn = sqlite3.connect(file)
            #engine = create_engine("sqlite:///"+file)
            return conn
        except Error as e:
            print(e)

with open('/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/scripts/characters.txt') as f:
    data = f.read()
f.close()
conn = create_connection()
conn.execute('delete from mythic_plus_best_runs')
conn.commit()
conn.close()
# reconstructing the data as a dictionary
character_json = json.loads(data)


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
    print('{} being pulled'.format(char['name']))
    r = requests.get('https://raider.io/api/v1/characters/profile?region={}&realm={}&name={}&fields=mythic_plus_best_runs%2Cmythic_plus_highest_level_runs%2Cmythic_plus_alternate_runs%2Cgear%2Cmythic_plus_weekly_highest_level_runs%2Cmythic_plus_previous_weekly_highest_level_runs'.format(char['region'],char['server'],char['name']))
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

        sum_item_level = 0
        item_count = 0
        for item in j['gear']['items']:
            sum_item_level = sum_item_level + j['gear']['items'][item]['item_level']
            if item != 'shirt':
                item_count = item_count + 1
        derived_item_level = sum_item_level / item_count
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
        for item in j['gear']['items']:
            conn.execute(
                    """INSERT OR IGNORE INTO character_gear (
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
                    (j['name']
                    ,j['region']
                    ,j['realm']
                    ,j['last_crawled_at']
                    ,j['gear']['item_level_equipped']
                     ,item
                     ,j['gear']['items'][item]['item_level']
                     ,j['gear']['items'][item]['name']
                     ,j['name']+j['realm']+j['region']+item+j['gear']['items'][item]['name']+str(j['gear']['items'][item]['item_level'])+str(derived_item_level)
                        ,j['active_spec_name']
                        ,j['active_spec_role']
                        ,derived_item_level
                     ,j['class']
                        )
                    )
            conn.commit()
    else:
        print('did not work {}'.format(char['name']))
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

conn.execute('delete from season_best_pivot')
conn.commit()
df_season_best.to_sql('season_best_pivot',conn,if_exists='append')

#update the gear table with increases

conn.execute("""with temp_table as
            (
            select *
            ,item_level - LEAD(item_level,1,0) over (partition by name,realm,region,item_slot order by last_crawled_at desc) as item_level_change
            from character_gear
            )
            update character_gear set slot_item_level_change=temp.item_level_change
            from temp_table temp
            where temp.name = character_gear.name
                and temp.region = character_gear.region
                and temp.realm = character_gear.realm
                and temp.item_slot = character_gear.item_slot
                and temp.item_name = character_gear.item_name
                and temp.item_level = character_gear.item_level
                and temp.derived_item_level = character_gear.derived_item_level
            """)
print("gear table updated")
conn.close()
print('pivot updated')
