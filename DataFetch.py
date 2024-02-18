# -*- coding: utf-8 -*-
"""
Created on Thu Feb 15 18:14:18 2024

@author: sulli
"""

#For data wrangling
import pandas as pd
import numpy as np
import json
import requests
import yaml

#For env vars
import keyring

#For os information
import os
import time
from datetime import datetime, timedelta


#For database set up
import mysql.connector as MSQL


#=================================
#Setting up Functions
#=================================


#Function for creating our database
def setup_database(credentials):
    """
    Function that creates the database STEAM. This function uses MySQL 
    connector to create the database alongside any tables.
    

    Parameters
    ----------
    credentials : Dict of credentials
        username - the username of the connection
        password - the password of the connection
        host - the hostname of the connection
        
    Returns
    -------
    MySQL connection.

    """
    
    #Check to see if credentials exists 
    
    #Creating our connection
    cnx = MSQL.connect(user=credentials['username'],
                                 password=credentials['password'],
                                 host=credentials['host'])
    cursor = cnx.cursor()
    
    print('Creating Database: steam')
    #Creating our database if it does not exist
    cursor.execute(
        "CREATE DATABASE IF NOT EXISTS steam_db;"
        )
    
    #Closing our cursor and connection
    cursor.close()
    cnx.database = 'steam_db'
    
    #Now we go through and make our tables
    #We want to insert many tables, let's create a dict for our tables
    Tables = {}
    
    #Now we can go through and define our tables. These will be strings of the create statements
    Tables['game_info'] = """
        CREATE TABLE IF NOT EXISTS steam_db.game_info(
        app_id INT UNSIGNED UNIQUE NOT NULL,
        name VARCHAR(200) NOT NULL,
        developer VARCHAR(200) NOT NULL,
        rating INT UNSIGNED NOT NULL,
        price DECIMAL(7,2) NOT NULL,
        PRIMARY KEY(app_id)
        );
        """
    
    Tables['player_count'] = """
        CREATE TABLE IF NOT EXISTS steam_db.player_count(
            app_id INT UNSIGNED NOT NULL,
            timestamp DATETIME NOT NULL,
            count INT UNSIGNED NOT NULL,
            FOREIGN KEY(app_id) REFERENCES game_info(app_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
            );
        """
    
    Tables['genre'] = """
        CREATE TABLE IF NOT EXISTS steam_db.genre(
            genre_id INT UNSIGNED NOT NULL,
            genre VARCHAR(100) NOT NULL,
            PRIMARY KEY (genre_id)
            );
    """
    
    Tables['game_genre'] = """
        CREATE TABLE IF NOT EXISTS steam_db.game_genre(
            app_id INT UNSIGNED NOT NULL,
            genre_id INT UNSIGNED,
            PRIMARY KEY(app_id,genre_id),
            FOREIGN KEY(app_id) REFERENCES game_info(app_id),
            FOREIGN KEY(genre_id) REFERENCES genre(genre_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
            );
    """
    
    Tables['tag'] = """
        CREATE TABLE IF NOT EXISTS steam_db.tag(
            tag_id INT UNSIGNED NOT NULL,
            tag VARCHAR(100) NOT NULL,
            PRIMARY KEY (tag_id))
    """
    
    Tables['game_tag'] = """
        CREATE TABLE IF NOT EXISTS steam_db.game_tag(
            app_id INT UNSIGNED NOT NULL,
            tag_id INT UNSIGNED NOT NULL,
            PRIMARY KEY(app_id,tag_id),
            FOREIGN KEY(app_id) REFERENCES game_info(app_id),
            FOREIGN KEY(tag_id) REFERENCES tag(tag_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
            );
    """
    
    #Creating our cursor again
    cursor = cnx.cursor()
    
    #Looping through our tables and creating them
    for table in Tables.keys():
        print(f"\tCreating Table: {table}")
        cursor.execute(Tables[table])
        cnx.commit()
    
    print("Tables successfully created")
    
    #Closing our cursor
    cursor.close()
    
    return cnx

#Function for pinging an API and returning the request
def get_request(url, params=None,attempt=1):
    """
    Function for returning a json response from an API request

    Parameters
    ----------
    url : String of the URL we are pinging
        
    params : Dict of parameters being passed into the API

    Returns
    -------
    json of API response

    """
    
    #Attempt to ping the url
    try:
        response = requests.get(url=url,params=params)
    
    #If we get an SSL error we wait for 5 seconds and try again
    except:
        print('SSL Error:')
        
        #Waiting 5 seconds for next API attempt
        for i in range(5,0,-1):
            print(f'\rWaiting {i}',end='')
            time.sleep(1)    
        print('\rAttempting API request again.')
        
        #Recursive call of function
        return get_request(url, params=params)
    
    #Now we check our response is not null
    if response:
        #If it is return the json
        try:
            return response.json()
        except:
            return None
    
    #Otherwise wait 10 seconds and try again
    else:
        #If we are not at max tries attempt again
        if attempt < 3:
            print('No response, waiting 5 seconds')
            time.sleep(5)
            print('Retrying')
            return get_request(url,params=params,attempt=attempt+1)
        #Otherwise stop
        else:
            print(f'Max retries exceeded.\nIgnoring request with params {params}.'
                  +f'Response: {response}')
            return None


#Function for getting game data
def get_game_data(config,connector,initial=False):
    """
    Function to get information about games and store in SQL table

    Parameters
    ----------
    start : The start index of the app list that we iterate through
    stop : The end index of the app list that we iterate through
    parser : A fuction that is used to handle the request
        Note that parser is just the name of the function.
    pause : How long in seconds to tell the system to sleep
    connector : Connection to MySQL to update tables
    

    Returns
    -------
    None

    """

    #If this is our inital run, run the initial functions
    if initial:
        parsers = config['data_fetch']['run_on_init']
    #Otherwise use the cycle functions
    else:
        parsers = config['data_fetch']['run_on_cycle']['cycle_fns']
        
    #If we don't have any parsers, we replace the None type with an empty list
    if parsers == None:
        parsers = []
        
    #First call to update the game info table
    if 'app_information' in parsers:
        app_information(connector)
    
    #Now pull from the game_info table
    cursor = connector.cursor()
    
    cursor.execute("""
                   SELECT app_id
                   FROM game_info;
                   """
                   )
    
    #Create a dict that has data for each function
    return_dict = {}
    
    functions = [fn for fn in parsers if fn != 'app_information']
    
    for fn in parsers:
        return_dict[fn] = []
    
    #Loop through the rows of our game_list
    for appid in cursor:
        
        #Loop through our functions
        for fn in functions:
                       
            #Now we retrieve the data with the parser logic
            data = eval(f'{fn}(appid)')
            if data:
                return_dict[fn].append(data)
        
    
        
        #Prevents overloading the api
        time.sleep(config['data_fetch']['api_run_params']['pause_between_calls'])
    
    #Insert data into SQL table
    for fn in functions:
        eval(f'{fn}_insert(return_dict[fn],connector)')
    
    cursor.close()
    
    return


#Round to nearest hour
def hour_rounder(t):
    """
    Function for rounding current time to closest hour

    Parameters
    ----------
    t : Current timestamp

    Returns
    -------
    Timestamp of closest hour

    """
    discard = timedelta(minutes=t.minute % 10,
                             seconds=t.second,
                             microseconds=t.microsecond)
    t -= discard
    if discard >=timedelta(minutes=5):
        t += timedelta(minutes=10)
    
    return t

#Function for getting nice output for current time
def get_current_time():
    """
    Function for getting the current time and returning a string formatted to 
    round to the nearest minute.

    Returns
    -------
    Clean datetime string

    """
    #Now we get our timestamp to insert into the table
    current_time = datetime.now()
    
    #Making this a string
    year = current_time.year
    month = f'0{current_time.month}'[-2:]
    day = f'0{current_time.day}'[-2:]
    hour = f'0{current_time.hour}'[-2:]
    minute = f'0{current_time.minute}'[-2:]
    
    #Actually making the string
    current_time = f'{year}-{month}-{day}-{hour}:{minute}:00'
    
    return current_time
    

#Creating function to parse a steam player count
def player_counts(appid):
    """
    Function for parsing player count. Current player count is inserted into 
    the corresponding SQL table

    Parameters
    ----------
    appid : The ID of the game we are attempting
    name : The name of the game we are attempting (unused)

    Returns
    -------
    Data to be inserted

    """
    
    #Retreiving our game request
    response = get_request('https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/',
                           params={'appid':appid})

    #Only if we get a response
    if response:
        #Now we get our timestamp to insert into the table
        current_time = hour_rounder(datetime.now())
    
        #Making this a string
        year = current_time.year
        month = f'0{current_time.month}'[-2:]
        day = f'0{current_time.day}'[-2:]
        hour = f'0{current_time.hour}'[-2:]
        minute = f'0{current_time.minute}'[-2:]
    
        #Actually making the string
        current_time = f'{year}-{month}-{day}-{hour}:{minute}:00'
    
        return_data = [appid[0],current_time,response['response']['player_count']]
    
        return return_data
    
    #Otherwise return None
    else:
        return None


#Creating function to insert player counts into table
def player_counts_insert(data, connector):
    """
    Function for inserting player count values into a table. 

    Parameters
    ----------
    data : Data to insert into table
        2D Array of shape (n,3) in the following order of columns:
            appid
            timestamp
            playercount
    
    connector : A connector to MySQL server to use to insert data

    Returns
    -------
    None.

    """
    
    #Creating the cursor
    cursor = connector.cursor()
    
    #Inserting our data into the player_counts table
    cursor.executemany("""
                       INSERT INTO player_count(app_id, timestamp, count)
                       VALUES ( %s, %s, %s)
                       """,data)

    connector.commit()
    cursor.close()
    

    #Actually making the string
    current_time = get_current_time()
    
    print(f'Game counts sucessfully inserted into table at {current_time}')
    
    return

#Function for fetching the top 100 games of the last 2 weeks
def app_information(connector):
    """
    Function for fetching the top apps of the last 2 weeks. This function
    requests information steamspy.com, gathers the relevant information and then
    inserts it into the relevant table.
    
    Parameters
    ----------    
    connector : A connector to MySQL server to use to insert data
    
    Returns
    -------
    Current valid app ids.

    """
    
    #Getting our json request from steamspy
    response =  get_request('https://steamspy.com/api.php?request=top100in2weeks')
    
    #Making this a pandas datafame
    df = pd.DataFrame.from_dict(response,orient='index')
    
    #We need to subset our columns
    df = df[['appid','name','developer','publisher','positive','negative',
             'initialprice']]
    
    #Initial price needs to be divided by 100
    df['price'] = df['initialprice'].astype(int)/100

    #We also create a user rating feature    
    df['user_rating'] = df['positive'].astype(int)/(
        df['positive'].astype(int)+df['negative'].astype(int))*100
    
    #Dropping redundant columns
    df = df.drop(columns=['initialprice','positive','negative'])
    
    #Adding our database connection
    cursor = connector.cursor()
    
    #Now we need to get our app information in the table
    for index, row in df.iterrows():
        
        #Converting rating to int
        rating = int(row['user_rating'])
        
        #Getting our insertions as a dict
        add_game = {
            'app_id' : row['appid'],
            'name' : row['name'],
            'developer' : row['developer'],
            'rating' : rating,
            'price' : row['price']
            }
        
        insert_game = """
                       INSERT IGNORE INTO game_info
                       (app_id,
                        name,
                        developer,
                        rating,
                        price)
                       VALUES (
                           %(app_id)s,%(name)s,%(developer)s,%(rating)s,%(price)s);
                       """
        
        #Inserting this record into our table if it doesn't exist
        cursor.execute(insert_game,add_game)
                       
    connector.commit()
    
    #Closing the cursor
    cursor.close()
    
    #Now we get our timestamp to insert into the table
    current_time = get_current_time()
    
    print(f'game_info table sucessfully updated at {current_time}')
    
    return
    

#Function for getting other information about a game
def game_tags_genres(app_id):
    """
    Function for getting the game genre and tags for a specific game.

    Parameters
    ----------
    app_id : The app ID of the game you are querying for.

    Returns
    -------
    Dict of genres and top 5 tags.

    """
    
    #Retreiving our game request
    response =  get_request('https://steamspy.com/api.php?request=appdetails',
                            params={'appid':app_id})
    
  
    #If we get a response
    if response['name']:
        #Creating our dict
        return_dict = {}
        
        #Assigning app to return dict
        return_dict['app_id'] = app_id
        
        #Getting genres
        #We split by ', ' in case there are multiple genres
        return_dict['genres'] = response['genre'].split(', ')
    
        #Getting tags
        #We'll just grab the top 5 tags if there are at least 5 tags
        if len(response['tags'].keys()) > 5:
            return_dict['tags'] = list(response['tags'].keys())[:5]
        else:
            return_dict['tags'] = list(response['tags'])
    
        return return_dict
    
    #Otherwise return none
    else:
        return None
 

#Function for inserting tag and genre information into relevant tables
def game_tags_genres_insert(data, connector):
    """
    Function for inserting tags and genres

    Parameters
    ----------
    data : list of dicts for information to input
        dicts contain 'genres' and 'tags'
        
    connector : mysql connector object

    Returns
    -------
    None.

    """
    
    #Getting all unique genre and tags
    genre_set = set()
    tag_set = set()
    
    #Creating dataframe for mappings
    genre_df = pd.DataFrame(data={
        'app_id':[],
        'genre_id':[]
        })
    
    tag_df = pd.DataFrame(data={
        'app_id':[],
        'tag_id':[]
        })
    
    #Looping through each game and concatenating each genre/tag
    for response in data:
        
        #Updating our unique genres
        genre_set.update(response['genres'])
        tag_set.update(response['tags'])
        
        #Adding our genre and tag infor to dataframes
        genre_response = pd.DataFrame(data={
            'app_id':[response['app_id'][0]]*len(response['genres']),
            'genre_id':response['genres']
            })
        
        genre_df = pd.concat([genre_df,genre_response])
        
        tag_response = pd.DataFrame(data={
            'app_id':[response['app_id'][0]]*len(response['tags']),
            'tag_id':response['tags']
            })
        
        tag_df = pd.concat([tag_df,tag_response])
        
    #Next we need to check if there are any tags we haven't seen in the table
    cursor = connector.cursor()
    
    cursor.execute(
        """
        SELECT tag_id, tag
        FROM tag;
        """
        )
    
    tag_dict = {}
    
    #Getting our tag ids
    for tag_id, tag in cursor:
        tag_dict[tag] = tag_id
    
    tags_to_add = []
    
    for tag in tag_set:
        if tag not in tag_dict.keys():
            tags_to_add.append([tag])
            if len(tag_dict.values())==0:
                tag_dict[tag] = 0
            else:
                tag_dict[tag] = max(tag_dict.values()) + 1

    #Creating our query to insert our unseen tags into     
    query = """
        INSERT IGNORE INTO tag
        (tag_id,tag)
        VALUES (
            %s,%s);
    """
    
    
    cursor.executemany(query,pd.DataFrame(data={
        'tag_id':tag_dict.values(),
        'tag':tag_dict.keys()
        }).values.tolist())
    connector.commit()
    
    current_time = get_current_time()
    
    print(f"New tags inserted into tags table at {current_time}")
    
    #Now we insert the game_tag table
    tag_df = tag_df.replace({'tag_id':tag_dict})
    
    query = """
        INSERT IGNORE INTO game_tag
        (app_id,
         tag_id)
        VALUES (
            %s,%s);
    """
    
    cursor.executemany(query,tag_df.values.tolist())
    connector.commit()
    
    current_time = get_current_time()
    
    print(f"game_tag table updated at {current_time}")
    
    #Doing the same for genres
    cursor.execute(
        """
        SELECT genre_id, genre
        FROM genre;
        """
        )
    genre_dict = {}
    
    #Getting our tag ids
    for genre_id, genre in cursor:
        genre_dict[genre] = genre_id
    
    genres_to_add = []
    
    for genre in genre_set:
        if genre not in genre_dict.keys():
            genres_to_add.append([genre])
            if len(genre_dict.values()) ==0:
                genre_dict[genre] = 0
            else:
                genre_dict[genre] = max(genre_dict.values()) + 1

    #Creating our query to insert our unseen tags into     
    query = """
        INSERT IGNORE INTO genre
        (genre_id,genre)
        VALUES (%s,%s);
    """
    
    cursor.executemany(query,pd.DataFrame(data={
        'genre_id':genre_dict.values(),
        'genre':genre_dict.keys()
        }).values.tolist())
    connector.commit()
    
    current_time = get_current_time()
    
    print(f"New genres inserted into genres table at {current_time}")
    
    
    #Finally we do this with the genre table
    genre_df = genre_df.replace({'genre_id':genre_dict})
    
    query = """
        INSERT IGNORE INTO game_genre
        (app_id,
         genre_id)
        VALUES (
            %s,%s);
    """
    
    cursor.executemany(query,genre_df.values.tolist())
    connector.commit()
    
    current_time = get_current_time()
    
    print(f"game_genre table updated at {current_time}")
    
    
    cursor.close()
    
    return
        
    
    
#Now we actually do the main function
if __name__ == '__main__':
    
    #Get informationf rom the config
    with open('config.yaml','r') as file:
        config = yaml.safe_load(file)
    
    
    #If the user has created credentials.json
    if 'mysql_credentials' in config.keys():
        print('Used credentials from config')
        credentials = config['mysql_credentials']
    
    #Otherwise ask for input
    else:
        print('Consider creating credentgials.json with relevant information')
        print("Input relevant information for MySQL server")
        credentials = {}
        
        #Asking user for credentials
        credentials['username'] = str(input('Username:'))
        credentials['password'] = str(input("Password:"))
        credentials['host'] = str(input('Host:'))
    
    #Setting up our connection and database
    cnx = setup_database(credentials)
    
    #Run initial functions
    get_game_data(config,cnx,initial=True)
    
    #Closing the connection, we reopen when we need to update the table
    cnx.close()
    

    
    #Creating our program loop
    while(True):
        
        #Determining how long until next cycle
        minutes = config['data_fetch']['run_on_cycle']['update_cycle_time']
        current_time = datetime.now()
        
        min_wait = 60
        #Loop through times
        for minute in minutes:
            #If replacing the minute is smaller, we've passed that time
            if current_time.replace(minute=minute) < current_time:
                wait = (current_time.replace(minute=minute)+timedelta(hours=1)-
                        current_time)
            #Otherwise we haven't
            else:
                wait = (current_time.replace(minute=minute)-current_time)
                
            #Is the min wait smaller?
            if min_wait > wait.seconds//60:
                min_wait = wait.seconds//60
        
        #Waiting specified minutes for next API attempt
        print(f'\rWaiting {min_wait} minutes for next update cycle',end='')
        time.sleep(30)  
        
        if datetime.now().minute in config['data_fetch']['run_on_cycle']['update_cycle_time']:
            
            print('\n',end='')
            
            #Connect to the database
            cnx = MSQL.connect(user=credentials['username'],
                                         password=credentials['password'],
                                         host=credentials['host'],
                                         database='steam_db')
            
            #Updating our game_info table
            get_game_data(config,cnx)
            
            #Now that we're done with the connection for now, we close the database
            cnx.close()
            
            #So we don't double insert at a minute
            time.sleep(60)