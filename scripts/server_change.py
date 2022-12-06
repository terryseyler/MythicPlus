import sqlite3
new_character_name = "Monkgov"
new_realm = "Illidan"
new_region ='us'
old_character_name="Mcmonk"
old_realm="Illidan"
old_region ='us'
unique_key_new = "{}{}{}".format(new_character_name
                                ,new_realm
                                ,new_region)
unique_key_old = "{}{}{}".format(old_character_name
                                ,old_realm
                                ,old_region)

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
            
character_gear_update="""
update character_gear
set name="{0}"
,realm="{1}"
,region="{2}"
,unique_key = REPLACE(unique_key,"{3}","{4}")
where name="{5}"
and realm="{6}"
and region="{7}"
""".format(new_character_name
,new_realm
,new_region
,unique_key_old
,unique_key_new
,old_character_name
,old_realm
,old_region

)

mythic_plus_best_runs_update="""
update mythic_plus_best_runs
set name="{0}"
,realm="{1}"
,region="{2}"
,unique_key = REPLACE(unique_key,"{3}","{4}")
where name="{5}"
and realm="{6}"
and region="{7}"
""".format(new_character_name
,new_realm
,new_region
,unique_key_old
,unique_key_new
,old_character_name
,old_realm
,old_region

)

all_mythic_plus_runs_update="""
update all_mythic_plus_runs
set name="{0}"
,realm="{1}"
,region="{2}"
where name="{3}"
and realm="{4}"
and region="{5}"
""".format(new_character_name
,new_realm
,new_region
,old_character_name
,old_realm
,old_region

)

season_best_pivot_update="""
update season_best_pivot_df_s1
set name="{0}"
,region="{1}"
where name="{2}"
and region="{3}"
""".format(new_character_name
,new_realm
,old_character_name
,old_realm
)

conn=create_connection()

conn.execute(character_gear_update)
conn.execute(mythic_plus_best_runs_update)
conn.execute(all_mythic_plus_runs_update)
conn.execute(season_best_pivot_update)

conn.commit()
conn.close()
