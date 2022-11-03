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
            file="/Users/terryseyler/Library/CloudStorage/OneDrive-Personal/git/warcraftLogs/App/mplus.db"
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
    data = cursor.execute('select * from season_best_pivot order by total_rating desc')
    print('hi')
    return render_template('index.html',data=data)

@app.route('/<region>/<realm>/<character_name>')
def character_name(region,realm,character_name):
    conn = create_connection()
    cursor = conn.cursor()

    distinct_crawl_dates = cursor.execute("""select distinct active_spec_role,active_spec_name,last_crawled_at,equipped_item_level
                                        from character_gear
                                        where name = '{}'
                                            and realm = '{}'
                                            and region = '{}'
                                        order by last_crawled_at desc
                                            """.format(character_name,realm,region)).fetchall()


    results = cursor.execute("""select *
                            from character_gear
                            where name = '{}'
                                and realm = '{}'
                                and region = '{}'
                                """.format(character_name,realm,region))

    data = results.fetchall()
    character = cursor.execute("""select *
                            from character_gear
                            where name = '{}'
                                and realm = '{}'
                                and region = '{}'
                                """.format(character_name,realm,region)).fetchone()
    return render_template('character.html',data=data,character=character,distinct_crawl_dates=distinct_crawl_dates)
