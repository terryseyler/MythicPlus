from flask import (
    flash, redirect, render_template, request, url_for
)
from flask import Flask
import sqlite3
from sqlite3 import Error
import json
import pandas as pd
app= Flask(__name__)
#import 'Notebooks/data.py'

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
        print(e)
        try:
            file="/Users/terryseyler/git/mythicplus/mplus.db"
            conn = sqlite3.connect(file
            ,detect_types=sqlite3.PARSE_DECLTYPES)
            conn.row_factory=sqlite3.Row
            #engine = create_engine("sqlite:///"+file)
            return conn
        except Error as e:
            print(e)
            try:
                file="C:/Users/tlsey/git/MythicPlus/mplus.db"
                conn = sqlite3.connect(file
                ,detect_types=sqlite3.PARSE_DECLTYPES)
                conn.row_factory=sqlite3.Row
                #engine = create_engine("sqlite:///"+file)
                return conn
            except Error as e:
                print(e)



@app.route('/')
def index():
    conn = create_connection()
    cursor  = conn.cursor()
    data = cursor.execute("""select scoreboard.*
    ,char.derived_item_level
     from
    (
        select *
        ,round(total_rating,1) as total_rating_round
        ,round(daily_rating_change,1) as daily_rating_change_round
        from base_characters base left join season_best_pivot_ext piv
    on upper(base.name) = upper(piv.name)
    and upper(base.realm) = upper(piv.realm)
    and upper(base.region) = upper(piv.region)
    where scoreboard_date = (select max(scoreboard_date) from  season_best_pivot_ext)
    order by total_rating desc
    ) scoreboard
    left join
    (
        Select Name,realm,region,round(avg(derived_item_level),1) as derived_item_level
        from character_gear_ext
        where last_crawled_at = (select max(last_crawled_at) from character_gear_ext)
        group by Name,realm,region
    )char
    on scoreboard.name  = char.name
    and scoreboard.realm = char.realm
    and scoreboard.region=char.region
    where scoreboard.total_rating > 0
    order by total_rating desc,derived_item_level desc
    """).fetchall()

    max_date = cursor.execute('select max(scoreboard_date) as max_date from season_best_pivot_ext').fetchone()
    print('hi')
    return render_template('index.html',data=data,max_date = max_date)

@app.route('/character_list')
def character_list():
    conn=create_connection()
    character_list =conn.execute("""Select Name,realm,region,round(avg(derived_item_level),2) as derived_item_level
    from character_gear_ext
    where last_crawled_at = (select max(last_crawled_at) from character_gear_ext)
    group by Name,realm,region
    order by avg(derived_item_level) desc
    """
    ).fetchall()
    return render_template('character_list.html',character_list=character_list)

@app.route('/<region>/<realm>/<character_name>',methods=['POST','GET'])
def character_name(region,realm,character_name):
    if request.method=='POST':
        conn = create_connection()
        cursor = conn.cursor()
        item_subclass = request.form["item_subclass"]
        items = conn.execute("""select *
                                from df_season_one_loot
                                where item_subclass='{}'
                                or item_subclass = 'Miscellaneous'
                                or inventory_type = 'CLOAK' or inventory_type='FINGER'
                                or item_lookup in ('mainhand','offhand')
                            """.format(item_subclass)
                            ).fetchall()
        distinct_weapons_offhands = conn.execute("""select distinct item_subclass,inventory_type from df_season_one_loot
                                                    where item_lookup in ('offhand','mainhand')
                                                    order by inventory_type,item_subclass
                                                    """).fetchall()
        current_equip = cursor.execute("""select *
                                from character_gear_ext
                                where name = '{}'
                                    and realm = '{}'
                                    and region = '{}'
                                    and last_crawled_at = (select max(last_crawled_at) from  character_gear_ext)
                                    and item_slot not in ('tabard','shirt','mainhand','offhand','ring2','trinket2')
                                    order by item_level
                                    """.format(character_name,realm,region)).fetchall()
        min_ring_item_level = cursor.execute("""select min(item_level) as min_ring_item_level
                                             from character_gear_ext
                                             where name = '{}'
                                                 and realm = '{}'
                                                 and region = '{}'
                                                 and last_crawled_at = (select max(last_crawled_at) from  character_gear_ext)
                                                 and item_slot in ('ring1','ring2')""".format(character_name,realm,region)).fetchone()
        min_trinket_item_level = cursor.execute("""select min(item_level) as min_trinket_item_level
                                             from character_gear_ext
                                             where name = '{}'
                                                 and realm = '{}'
                                                 and region = '{}'
                                                 and last_crawled_at = (select max(last_crawled_at) from  character_gear_ext)
                                                 and item_slot in ('trinket1','trinket2')""".format(character_name,realm,region)).fetchone()

        current_equip_mainhand = cursor.execute("""select *
                                from character_gear_ext
                                where name = '{}'
                                    and realm = '{}'
                                    and region = '{}'
                                    and last_crawled_at = (select max(last_crawled_at) from  character_gear_ext)
                                    and item_slot in ('mainhand')
                                    order by item_level
                                    """.format(character_name,realm,region)).fetchone()
        current_equip_offhand = cursor.execute("""select *
                                from character_gear_ext
                                where name = '{}'
                                    and realm = '{}'
                                    and region = '{}'
                                    and last_crawled_at = (select max(last_crawled_at) from  character_gear_ext)
                                    and item_slot in ('offhand')
                                    order by item_level
                                    """.format(character_name,realm,region)).fetchone()
        character = cursor.execute("""select *
                                ,last_crawled_at as last_crawled_cleansed
                                from character_gear_ext
                                where name = '{}'
                                    and realm = '{}'
                                    and region = '{}'
                                    and item_slot not in ('tabard','shirt')
                                order by last_crawled_at desc
                                    """.format(character_name,realm,region)).fetchone()
        return render_template('upgrades.html',items=items
        ,current_equip=current_equip
        ,current_equip_mainhand=current_equip_mainhand
        ,current_equip_offhand=current_equip_offhand
        ,min_ring_item_level=min_ring_item_level
        ,min_trinket_item_level=min_trinket_item_level
        ,distinct_weapons_offhands=distinct_weapons_offhands
        ,character=character)

    conn = create_connection()
    cursor = conn.cursor()
    warcraftlogs_encounters = cursor.execute("""select * 
                                        ,round(rankPercent,1) as rankPercent_rounded
                                        from warcraftlogs_raid_encounter
                                        where character_name ='{}'
                                        and realm = '{}'
                                        and region = '{}' """.format(character_name,realm,region)).fetchall()
    distinct_crawl_dates = cursor.execute("""select distinct active_spec_role
                                        ,active_spec_name
                                        ,last_crawled_at as last_crawled_cleansed
                                        ,last_crawled_at
                                        ,equipped_item_level
                                        from character_gear_ext
                                        where name = '{}'
                                            and realm = '{}'
                                            and region = '{}'

                                        order by last_crawled_at desc
                                            """.format(character_name,realm,region)).fetchall()


    results = cursor.execute("""select *
                            from character_gear_ext
                            where name = '{}'
                                and realm = '{}'
                                and region = '{}'
                            and item_slot not in ('tabard','shirt')
                                """.format(character_name,realm,region))

    data = results.fetchall()
    all_mythic_plus_runs = cursor.execute("""select *
                                    ,strftime('%Y-%m-%d %H:%M',completed_at) as completed_at_cleansed
                                    from all_mythic_plus_runs
                                    where name = '{}'
                                        and realm = '{}'
                                        and region = '{}'
                                    and strftime('%Y-%m-%d %H:%M',completed_at) >= '2022-12-01 00:00'
                                    order by completed_at desc
                                        """.format(character_name,realm,region)).fetchall()

    character = cursor.execute("""select *
                            ,last_crawled_at as last_crawled_cleansed
                            from character_gear_ext
                            where name = '{}'
                                and realm = '{}'
                                and region = '{}'
                            and item_slot not in ('tabard','shirt')
                            order by last_crawled_at desc
                                """.format(character_name,realm,region)).fetchone()
    return render_template('character.html',warcraftlogs_encounters=warcraftlogs_encounters,data=data,character=character,distinct_crawl_dates=distinct_crawl_dates,all_mythic_plus_runs=all_mythic_plus_runs)
