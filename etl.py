import pandas as pd
import os

from create_tables import insertfromdataframe
from create_tables import db_connection
import create_tables

def process_files(folder):
    df = pd.DataFrame()
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".json"):
                 df_new = pd.read_json(os.path.join(root, file), lines=True)
                 df = df.append(df_new, ignore_index=True)
    return df

def process_song_data(hostname, dbname, folder):

    df = process_files(folder)

    artists = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].copy()
    songs = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()

    artists['artist_id'] = artists['artist_id'].astype(str)
    artists.insert(loc=0, column='index', value=range(0, 0 + len(artists)))
    insertfromdataframe(hostname, dbname, "artists", artists.drop_duplicates(subset='artist_id', keep='first'))

    songs['song_id'] = songs['song_id'].astype(str)
    songs.insert(loc=0, column='index', value=range(0, 0 + len(songs)))
    insertfromdataframe(hostname, dbname, "songs", songs.drop_duplicates(subset='song_id', keep='first'))

def process_log_data(hostname, dbname, folder):

    df = process_files(folder)
    df = df[df.page == "NextSong"]

    users = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()
    time = df[['ts']].copy()
    songplays = df[['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']].copy()

    # USERS
    users['userId'] =  users['userId'].astype(str)
    insertfromdataframe(hostname, dbname, "users", users.drop_duplicates(subset=['userId', 'level']))

    #### TIME
    time['ts'] = pd.to_datetime(time['ts'], unit='ms')

    time_dict = {"start_time": time.ts,
                  "hour": time.ts.dt.hour,
                  "day": time.ts.dt.day,
                  "week": time.ts.dt.dayofweek,
                  "month": time.ts.dt.month,
                  "year": time.ts.dt.year,
                  "weekday": time.ts.dt.weekday
                  }
    time_df = pd.DataFrame.from_dict(time_dict)
    insertfromdataframe(hostname, dbname, "time", time_df.drop_duplicates())

    # SONGPLAY

    conn, cur = db_connection(hostname, dbname)

    songplays['ts'] = pd.to_datetime(songplays['ts'], unit='ms')

    for index, row in songplays.iterrows():
        # get songid and artistid from song and artist tables
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

    songplays.insert(loc=0, column='index', value=range(0, 0 + len(songplays)))
    insertfromdataframe(hostname, dbname, "songplays", songplays)


def main():
    hostname,dbname = "127.0.0.1","sparkifydb"
    create_tables.main(hostname, dbname)

    process_song_data(hostname, dbname, "./data/song_data/" )
    process_log_data(hostname, dbname, "./data/log_data/" )

if __name__ == "__main__":
    main()
