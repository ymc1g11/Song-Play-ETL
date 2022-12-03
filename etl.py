import os
import glob
import json
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function processes song file at the given file path. The 
    songs and artists tables are populated by the extract from this 
    file.
    
    The file specified by the file path must contain only individual 
    JSON object(s) for this function to work correctly.
    
    Parameters
    ----------
    cur : cursor
        Postgresql connection cursor.
    filepath : str
        An absolute file path string.

    Returns
    -------
    None
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    data_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = list(data_df.values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    data_df = df[
        ['artist_id', 'artist_name', 'artist_location', 
         'artist_latitude', 'artist_longitude']
    ]
    artist_data = list(data_df.values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function processes log file at the given file path. The time 
    and songplays tables are populated by the extract from this file.
    
    The file specified by the file path must contain only individual 
    JSON object(s) for this function to work correctly.
    
    Parameters
    ----------
    cur : cursor
        Postgresql connection cursor.
    filepath : str
        An absolute file path string.

    Returns
    -------
    None
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # convert timestamp column to datetime
    df['ts'] = df['ts'].apply(lambda x: x/1000.0)
    df['ts'] = df['ts'].apply(datetime.fromtimestamp)
    
    def insert_time_data(df):
        # filter by NextSong action
        nsdf = df[df['page']=='NextSong']
        t = nsdf['ts']

        # insert time data records
        time_data = (
            t, 
            t.dt.hour.values, 
            t.dt.day.values, 
            t.dt.isocalendar().week.values, 
            t.dt.month.values, 
            t.dt.year.values, 
            t.dt.weekday.values
        )
        column_labels = (
            'timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday'
        )
        time_data_dict = dict(zip(column_labels, time_data))
        time_df = pd.DataFrame(time_data_dict)

        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    
    def insert_user_data(df):
        # load user table
        user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

        # filter dataframe to contain only non-empty user
        user_df = user_df[
            (user_df['userId'] != None) & 
            (user_df['userId'] != '')
        ]

        # insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    
    def insert_songplay_data(df):
        # insert songplay records
        for index, row in df.iterrows():
            if not row.userId:
                continue

            # get songid and artistid from song and artist tables
            cur.execute(song_select, [row.song])
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = [
                row.ts, row.userId, row.level, 
                songid, artistid, row.sessionId, row.location,
                row.userAgent
            ]
            cur.execute(songplay_table_insert, songplay_data)
    
    insert_time_data(df)
    insert_user_data(df)
    insert_songplay_data(df)

def process_data(cur, conn, filepath, func):
    """
    This function processes JSON file at the given file path, using the 
    function specified as the 'func' parameter. 
    
    The function specified as the 'func' parameter must take 'cur' and 
    'filepath' as parameters to work correctly.
    
    Parameters
    ----------
    cur : cursor
        Postgresql connection cursor.
    conn : connection
        Postgresql connection.
    filepath : str
        An absolute file path string.
    func : function
        Function to process file taking the cur and filepath as parameters
    
    Returns
    -------
    None
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()