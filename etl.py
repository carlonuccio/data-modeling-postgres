import pandas as pd
import os
from create_tables import insert_from_dataframe
from create_tables import db_connection
import create_tables


def process_files(folder):
    """
    open all json files within a root folder and append to a pandas dataframe
    :param folder: path root folder
    :return: a pandas dataframe
    """
    #
    df = pd.DataFrame()
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".json"):
                 df_new = pd.read_json(os.path.join(root, file), lines=True)
                 df = df.append(df_new, ignore_index=True)
    return df


def process_song_data(hostname, dbname, folder):
    """
    - Process all song data
    - Subset of artists and copy
    - Remove duplicates and insert an ID
    - insert them into database
    :param hostname: Host Database Address
    :param dbname: Database Name
    :param folder: path root folder for song data
    """

    # process song data
    df = process_files(folder)

    # artists and song subset
    artists = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].copy()
    songs = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()

    # convert artist_id to a string, remove duplicates, insert id and insert into a db table
    artists['artist_id'] = artists['artist_id'].astype(str)
    artists = artists.drop_duplicates(subset='artist_id', keep='first')
    artists.insert(loc=0, column='index', value=range(0, 0 + len(artists)))
    insert_from_dataframe(hostname, dbname, "artists", artists)

    # convert song_id to a string, remove duplicates, insert id and insert into a db table
    songs['song_id'] = songs['song_id'].astype(str)
    songs = songs.drop_duplicates(subset='song_id', keep='first')
    songs.insert(loc=0, column='index', value=range(0, 0 + len(songs)))
    insert_from_dataframe(hostname, dbname, "songs", songs)


def process_log_data(hostname, dbname, folder):
    """
    - Process all log data
    - Subset of users, time and songplays
    - insert users, time and songplays into database
    """

    df = process_files(folder)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # users, time and songplays subset
    users = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    time = df[['ts']].copy()
    songplays = df[['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']].copy()

    # insert user records
    users['userId'] =  users['userId'].astype(str)
    insert_from_dataframe(hostname, dbname, "users", users.drop_duplicates(subset=['userId', 'level']))



    # insert time data records
    time_dict = {"start_time": time.ts,
                  "hour": time.ts.dt.hour,
                  "day": time.ts.dt.day,
                  "week": time.ts.dt.dayofweek,
                  "month": time.ts.dt.month,
                  "year": time.ts.dt.year,
                  "weekday": time.ts.dt.weekday
                  }
    time_df = pd.DataFrame.from_dict(time_dict)
    insert_from_dataframe(hostname, dbname, "time", time_df.drop_duplicates())

    # insert songplay records
    conn, cur = db_connection(hostname, dbname)

    # get songid and artistid from song and artist tables
    for index, row in songplays.iterrows():
        sql_select_query = """select idSong from Songs where title = %s"""
        cur.execute(sql_select_query, (row['song'],))
        idSong = cur.fetchone()

        if idSong:
            song = idSong[0]
        else:
            song = None

        sql_select_query = """select idArtist from Artists where name = %s"""
        cur.execute(sql_select_query, (row['artist'],))

        idArtist = cur.fetchone()

        if idArtist:
            Artist = idArtist[0]
        else:
            Artist = None

        songplays.loc[index, 'song'] = song
        songplays.loc[index, 'artist'] = Artist

    cur.close()
    conn.close()

    # insert songplay record
    songplays.insert(loc=0, column='index', value=range(0, 0 + len(songplays)))
    insert_from_dataframe(hostname, dbname, "songplays", songplays)


def main():
    """
    - Drops (if exists) and Creates the sparkify database and relative tables

    - process song data

    - process log data
    """
    hostname,dbname = "127.0.0.1","sparkifydb"
    create_tables.main(hostname, dbname)

    process_song_data(hostname, dbname, "./data/song_data/")
    process_log_data(hostname, dbname, "./data/log_data/")


if __name__ == "__main__":
    main()
