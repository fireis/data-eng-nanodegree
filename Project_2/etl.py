# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster


def get_files():
    """
    Retrieve all csv files under the event_data directory
    
    Returns:
    file_path_list (list):  list of csv files
    
    """
    filepath = os.getcwd() + "/event_data"

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root, "*.csv"))
    return file_path_list


def create_insertion_file(file_path_list, dest_name):
    """
    Create a unique file for correct insertion into the tables
    
    Args:
    file_path_list (list): list of csv files
    dest_name (string): path to save the complete csv file

    """
    print("Creating single file with all information.")
    full_data_rows_list = []

    for f in file_path_list:
        with open(f, "r", encoding="utf8", newline="") as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

    # Select relevat features and save on new file
    csv.register_dialect("myDialect", quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(dest_name, "w", encoding="utf8", newline="") as f:
        writer = csv.writer(f, dialect="myDialect")
        writer.writerow(
            [
                "artist",
                "first_name",
                "gender",
                "item_in_session",
                "last_name",
                "length",
                "level",
                "location",
                "session_id",
                "song",
                "user_id",
            ]
        )
        for row in full_data_rows_list:
            if row[0] == "":
                continue
            writer.writerow(
                (
                    row[0],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[12],
                    row[13],
                    row[16],
                )
            )


def set_cluster():
    """
    Creates and configures a Cassandra cluster and Session
    
    Returns:
    cluster (cluster): active Cassandra cluster
    session (session): active Casssandra session
    
    """
    print("Setting cluster")
    
    cluster = Cluster()
    session = cluster.connect()
    try:
        session.execute(
            """
        CREATE KEYSPACE IF NOT EXISTS udacity 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )
    except Exception as e:
        print(e)
    try:
        session.set_keyspace("udacity")
    except Exception as e:
        print(e)

    return cluster, session


def create_session_library(session, file):
    """
    Creates the session library table
    
    Args:
    session(session): active Cassandra session
    file(string): path to csv file with complete information
    
    Returns:
    session (session): active Cassandra session
    
    """
    print("Creating session library")
    
    # assure that there the session_library table is clean
    query = "drop table if exists session_library"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    # creates the table
    query = "CREATE TABLE IF NOT EXISTS session_library "
    query = (
        query
        + "(session_id int, \
                        item_in_session int, \
                        artist text, \
                        song_title text, \
                        song_length double, \
                        PRIMARY KEY (session_id, item_in_session))"
    )

    try:
        session.execute(query)
    except Exception as e:
        print(e)

    # reads csv and inserts into table
    with open(file, encoding="utf8") as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            query = "INSERT INTO session_library (\
                        session_id, \
                        item_in_session, \
                        artist, \
                        song_title, \
                        song_length)"

            query = query + "VALUES (%s, %s, %s, %s, %s)"
            session.execute(
                query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5]))
            )

    return session


def create_user_library(session, file):
    """
    Creates the user library table
    
    Args:
    session(session): active Cassandra session
    file(string): path to csv file with complete information
    
    Returns:
    session (session): active Cassandra session
    
    """
    print("Creating user library")

    # assure that there the user_library table is clean
    query = "drop table if exists user_library"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    # creates the table
    query = "CREATE TABLE IF NOT EXISTS user_library "
    query = query + "(user_id int, \
                        session_id int, \
                        item_in_session int,\
                        artist text, \
                        song_title text, \
                        first_name text, \
                        last_name text,\
                        PRIMARY KEY ((user_id, session_id), item_in_session))"

    try:
        session.execute(query)
    except Exception as e:
        print(e)       

    # reads csv and inserts into table
    with open(file, encoding="utf8") as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            query = "INSERT INTO user_library (\
                    user_id, \
                    session_id, \
                    item_in_session, \
                    artist, \
                    song_title, \
                    first_name, \
                    last_name)"
        

            query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (int(line[10]), 
                                int(line[8]),
                                int(line[3]),
                                line[0], 
                                line[9], 
                                line[1],
                                line[4]))

    return session


def create_name_library(session, file):
    """
    Creates the session name table
    
    Args:
    session(session): active Cassandra session
    file(string): path to csv file with complete information
    
    Returns:
    session (session): active Cassandra session
    
    """
    print("Creating name library")
    
    # assure that there the name_library table is clean
    query = "drop table if exists name_library"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    # creates the table
    query = "CREATE TABLE IF NOT EXISTS name_library "
    query = (
        query
        + "(song_title text, \
                    user_id text,\
                    first_name text, \
                    last_name text,\
                    PRIMARY KEY ((song_title), user_id))"
    )

    try:
        session.execute(query)
    except Exception as e:
        print(e)

    with open(file, encoding="utf8") as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            query = "INSERT INTO name_library (\
                    song_title, \
                    user_id,\
                    first_name, \
                    last_name)"

            query = query + "VALUES (%s, %s, %s, %s)"
            session.execute(query, (line[9], line[10], line[1], line[4]))
    return session


def test_tables(session):
    """
    Tests the tables by printing the results
    
    Args:
    session(session): active cassandra session
    
    TODO: make test auto by using expected values
    
    """
    print("Testing the tables")
    
    
    # tests session_library
    print("Testing session_library")
    query = "select  artist, song_title, song_length \
            FROM session_library \
            WHERE session_id = 338 AND item_in_session = 4"
    try:
        results = session.execute(query)
    except Exception as e:
        print(e)

    for result in results:
        print(result)

    # tests user_library
    print("Testing user_library")
    query = "select  artist, song_title, first_name, last_name \
            FROM user_library \
            WHERE user_id = 10 AND session_id = 182"
    try:
        results = session.execute(query)
    except Exception as e:
        print(e)
    for result in results:
        print(result)

    # tests name_library
    print("Testing name_library")
    query = "select  first_name, last_name \
            FROM name_library \
            WHERE song_title = 'All Hands Against His Own'"
    try:
        results = session.execute(query)
    except Exception as e:
        print(e)
    for result in results:
        print(result)


def shutdown_cluster(session, cluster):
    """
    Shuts down the session and cluster
    
    Args:
    cluster (cluster): active Cassandra cluster
    session (session): active Casssandra session
    
    """
    print("Shutting clusters down")
    
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":

    files = get_files()
    csv_file = "event_datafile_new.csv"
    create_insertion_file(files, csv_file)

    cluster, session = set_cluster()

    session = create_session_library(session, csv_file)
    session = create_user_library(session, csv_file)
    session = create_name_library(session, csv_file)

    test_tables(session)

    shutdown_cluster(cluster, session)
