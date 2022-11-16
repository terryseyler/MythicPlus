#30 * * * * /Users/terryseyler/opt/anaconda3/bin/python "/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/scripts/pull_raiderio_api.py"



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
            file="/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/mplus.db"
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
    with open('/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/scripts/characters.txt') as f:
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
        try:
            derived_item_level = sum_item_level / item_count
        except Exception as e:
            derived_item_level = 0
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
now_string = dt.datetime.now().strftime("%Y-%m-%d")
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
            ,cur.total_rating

            ,cur."Tazavesh: So\'leah\'s Gambit Fortified"
            ,cur."Tazavesh: So\'leah\'s Gambit Tyrannical"

            ,cur."Tazavesh: Streets of Wonder Fortified"
            ,cur."Tazavesh: Streets of Wonder Tyrannical"

            ,cur."Return to Karazhan: Upper Fortified"
            ,cur."Return to Karazhan: Upper Tyrannical"

            ,cur."Return to Karazhan: Lower Fortified"
            ,cur."Return to Karazhan: Lower Tyrannical"

            ,cur."Iron Docks Fortified"
            ,cur."Iron Docks Tyrannical"

            ,cur."Grimrail Depot Fortified"
            ,cur."Grimrail Depot Tyrannical"

            ,cur."Mechagon Workshop Fortified"
            ,cur."Mechagon Workshop Tyrannical"

            ,cur."Mechagon Junkyard Fortified"
            ,cur."Mechagon Junkyard Tyrannical"

            ,round(cur.total_rating - pr.total_rating,1) as daily_rating_change

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
conn.commit()
conn.close()
print('pivot updated')
