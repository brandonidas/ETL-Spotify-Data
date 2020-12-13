import myconfig

import requests
import json

import pandas as pd 

# from datetime import datetime
import datetime

import sqlalchemy # not working
from sqlalchemy.orm import sessionmaker
import sqlite3

def check_if_valid_data(df: pd.DataFrame) -> bool: # TODO personalise this validation step
    if df.empty:
        print("dataframe is empty")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls TODO impute instead
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    return True

def send_request_to_spotify() -> dict:
        headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {token}".format(token=myconfig.AUTH)
        }
        
        # PART 1: EXTRACT, TODO refactor into a function
        # Unix timestamp in ms      
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

        # Only songs you've listened to after yesterday # TODO some error handling
        # Eg. from spotify website 
        #  "https://api.spotify.com/v1/me/player/recently-played?limit=3&after=1231541235252&before=78908908432"
        r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
        

        return r.json()

def extract_data_to_df(data: dict) -> pd.DataFrame:
    song_names, artist_names, played_at_list = [],[],[]

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
    }

    return pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at"])

def load_into_sqlite(df: pd.DataFrame):
    engine = sqlalchemy.create_engine(myconfig.DATABASE_LOCATION)
    conn = sqlite3.connect('tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    # TODO create duplicate logic
    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")

if __name__ == "__main__":

    # PART 1 EXTRACT
    data = send_request_to_spotify()
    song_df = extract_data_to_df(data)

    print(song_df)

    # PART 2 TRANSFORM
    if check_if_valid_data(song_df):
        print("Data is valid. Proceeding to next step : LOAD")
        # PART 3 LOAD
        load_into_sqlite(song_df)
    else:
        # TODO possibly impute
        print("Data is invalid")