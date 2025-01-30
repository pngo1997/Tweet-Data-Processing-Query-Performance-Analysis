#!/usr/bin/env python
# coding: utf-8
# ## Part 3

# a) Using the database with 650,000 tweets, create a new table that corresponds to the join of all 3 tables in your database, including records without a geo location. This is the equivalent of a materialized view but since SQLite does not support MVs, we will use CREATE TABLE AS SELECT (instead of CREATE MATERIALIZED VIEW AS SELECT).

# In[1]:


import sqlite3

connection = sqlite3.connect('DSC450-Final-650k.db')
cursor = connection.cursor()
query_3A = '''CREATE TABLE Tweet_Join 
            AS SELECT Tweet.*,
                    User.Name,
                    User.SCREEN_NAME,
                    User.DESCRIPTION,
                    User.FRIENDS_COUNT,
                    Geo.Type,
                    Geo.LONGITUDE,
                    Geo.LATITUDE
            FROM Tweet
            LEFT JOIN User ON Tweet.USER_ID = User.ID
            LEFT JOIN Geo ON Tweet.Geo_ID = Geo.Geo_ID;'''

cursor.execute('DROP TABLE IF EXISTS Tweet_Join')
cursor.execute(f'{query_3A}')
cursor.execute('SELECT * FROM Tweet_Join LIMIT 3')
results_3A = cursor.fetchall()
for row in results_3A:
    print(row)
    print("\n")
connection.commit()
connection.close()


# b) Export the contents of 1) the Tweet table and 2) your new table from 3-a into a new JSON file (i.e., create your own JSON file with just the keys you extracted). You do not need to replicate the structure of the input and can come up with any reasonable keys for each field stored in JSON structure (e.g., you can have longitude as “longitude” key when the location is available). 
# How do the file sizes compare to the original input file?

# #### Tweet Table: 11 attributes.

# In[2]:


import json
import os

connection = sqlite3.connect('DSC450-Final-650k.db')
cursor = connection.cursor()

cursor.execute('SELECT * FROM Tweet')
tweetResults = cursor.fetchall()

tweet_columnNames = [col[0] for col in cursor.description]

tweetData = []
for row in tweetResults:
    tweetDict = {}
    for i, value in enumerate(row):
        tweetDict[tweet_columnNames[i]] = value
    tweetData.append(tweetDict)

connection.commit()
connection.close()

with open('Tweet.json', 'w') as tweetFile:
    json.dump(tweetData, tweetFile)

tweetFile_size_json = os.path.getsize('Tweet.json') / (1024 * 1024)
print(f'Tweet JSON file size: {tweetFile_size_json:.2f} MB')


# #### Tweet Join table: 18 atributes.

# In[3]:


connection = sqlite3.connect('DSC450-Final-650k.db')
cursor = connection.cursor()

cursor.execute('SELECT * FROM Tweet_Join')
tweet_joinResults = cursor.fetchall()

tweet_join_columnNames = [col[0] for col in cursor.description]

tweet_joinData = []
for row in tweet_joinResults:
    tweet_joinDict = {}
    for i, value in enumerate(row):
        tweet_joinDict[tweet_join_columnNames[i]] = value
    tweet_joinData.append(tweet_joinDict)

connection.commit()
connection.close()

with open('Tweet_Join.json', 'w') as tweet_joinFile:
    json.dump(tweet_joinData, tweet_joinFile)
    tweet_joinFile.write('\n')

tweet_joinFile_size_json = os.path.getsize('tweet_join.json') / (1024 * 1024)
print(f'Tweet_Join JSON file size: {tweet_joinFile_size_json:.2f} MB')


# #### Original URL text file.

# In[4]:


import urllib.request

textURL = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
with urllib.request.urlopen(textURL) as response:
    fileSize_bytes = int(response.info().get('Content-Length'))

fileSize_gb = fileSize_bytes / (1024 ** 3)
print(f"Original textURL File size: {fileSize_gb:.2f} GB")


# After processing data, the two json files' sizes are Tweet table: 294.24 MB, and Tweet_Join table: 451.45 MB. We have significantly reduced the size from the original textURL file of 12.95 GB. This shows that using JSON file is an effective approach for large data storage. 

# c) Export the contents of 1) the Tweet table and 2) your table from 3-a into a .csv (comma separated value) file. 
# How do the file size compare to the original input file and to the files in 3-b?

# #### Tweet Table: 11 attributes.

# In[18]:


import csv

connection = sqlite3.connect('DSC450-Final-650k.db')
cursor = connection.cursor()

cursor.execute("SELECT * FROM Tweet")
tweetResults = cursor.fetchall()

columnNames = [col[0] for col in cursor.description]

connection.commit()
connection.close()

with open('Tweet.csv', 'w', newline='', encoding='utf-8-sig') as tweet_csvFile:
    tweet_csvWriter = csv.writer(tweet_csvFile)
    tweet_csvWriter.writerow(columnNames)
    tweet_csvWriter.writerows(tweetResults)
        
tweetFile_size_csv = os.path.getsize('Tweet.csv') / (1024 * 1024)
print(f'Tweet CSV file size: {tweetFile_size_csv:.2f} MB')


# #### Tweet Join table: 18 atributes.

# In[19]:


connection = sqlite3.connect('DSC450-Final-650k.db')
cursor = connection.cursor()

cursor.execute("SELECT * FROM Tweet_Join")
tweet_joinResults = cursor.fetchall()

columnNames = [col[0] for col in cursor.description]

connection.commit()
connection.close()

with open('Tweet_Join.csv', 'w', newline='', encoding='utf-8-sig') as tweet_join_csvFile:
    tweet_join_csvWriter = csv.writer(tweet_join_csvFile)
    tweet_join_csvWriter.writerow(columnNames)
    tweet_join_csvWriter.writerows(tweet_joinResults)

tweet_joinFile_size_csv = os.path.getsize('Tweet_Join.csv') / (1024 * 1024)
print(f'Tweet_Join CSV file size: {tweet_joinFile_size_csv:.2f} MB')


# After processing data, the two csv files' sizes are Tweet table: 145.85 MB, and Tweet_Join table: 212.04 MB. We have significantly reduced the size from the original textURL file of 12.95 GB. CSV files size are also smaller than JSON files. This shows that using CSV file is the most effective approach for large data storage in this case. 

# In[ ]:




