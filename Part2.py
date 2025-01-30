#!/usr/bin/env python
# coding: utf-8

# #### Student Name: Mai Ngo 
# #### Course Name and Number: DSC 450 Database Processing for Large-Scale Analytics 
# #### Final - Part 2
# #### Date: 8/20/2023

# ## Part 2

# a) Write and execute a SQL query to find the average latitude value for each user ID, using both AVG and SUM/COUNT. This query does not need the User table because User ID is a foreign key in the Tweet table. E.g., something like SELECT UserID, AVG(latitude), SUM(latitude)/COUNT(latitude) FROM Tweet, Geo WHERE Tweet.GeoFK = Geo.GeoID GROUP BY UserID;

# #### 130,000 tweets.

# In[55]:


import time
import sqlite3

startTime = time.time()
connection = sqlite3.connect('DSC450-Final-130k.db')
cursor = connection.cursor()
#cursor.execute("SELECT Geo_ID FROM Tweet")
cursor.execute ("SELECT Tweet.USER_ID, AVG(Geo.LATITUDE), SUM(Geo.LATITUDE)/COUNT(Geo.LATITUDE) FROM Tweet, Geo WHERE Tweet.GEO_ID = Geo.GEO_ID GROUP BY Tweet.USER_ID;")
results_2A = cursor.fetchall()
for i, row in enumerate(results_2A):
    if i>=10: break
    print(row)
    
connection.commit()
connection.close()
endTime = time.time()
runTime_2A = endTime-startTime
print(f"Query runtime Part 2A (130,000 tweets): {runTime_2A:4f} seconds")


# b) Re-execute the SQL query in part 2-a 5 times and 20 times and measure the total runtime (just re-run the same exact query multiple times using a for-loop, it is as simple as it looks). Does the runtime scale linearly? (i.e., does it take 5X and 20X as much time?)

# The run time does scale linearly, the more execution times, the longer total running time. 

# In[56]:


import time
connection = sqlite3.connect('DSC450-Final-130k.db')
cursor = connection.cursor()

query = "SELECT Tweet.USER_ID, AVG(Geo.LATITUDE), SUM(Geo.LATITUDE)/COUNT(Geo.LATITUDE) FROM Tweet, Geo WHERE Tweet.GEO_ID = Geo.GEO_ID GROUP BY Tweet.USER_ID;"

roundCount = [5, 20]
for count in roundCount:
    total_runTime_2B = 0
    for _ in range(count):
        startTime = time.time()
        cursor.execute(query)
        results_2B = cursor.fetchall()
        endTime = time.time()
        runTime_2B = endTime - startTime
        total_runTime_2B += runTime_2B
    print(f"Query {count} Total Runtimes Part 2B: {total_runTime_2B:.4f} seconds")
connection.commit()
connection.close()


# c) Write the equivalent of the 2-a query in python (without using SQL) by reading it from the file with 650,000 tweets.

# #### 650,000 tweets.

# In[57]:


import statistics
import json

def latitudeAvg (textFile):
    '''Take text file input and calcuate average latitude of each user.''' 

    userLatitudes = {}
    with open(textFile, 'r', encoding='utf-8') as inFile:
        for row in inFile:
            tweet = json.loads(row)
            userID = tweet['user']['id']
            if tweet['geo'] is not None:
                latitude = tweet['geo']['coordinates'][1]
                if userID in userLatitudes:
                    userLatitudes[userID].append(latitude)
                else:
                    userLatitudes[userID] = [latitude]

    results = []

    for userID, latitudes in userLatitudes.items():
        latitudeAve_sumCount = sum(latitudes) / len(latitudes)
        latitudeAve_Stat = statistics.mean(latitudes)
        results.append((userID, latitudeAve_sumCount, latitudeAve_Stat))
    
    return results
    
def latitudeAvg_runTime(textFile, roundCount):
    '''Take input text file and number of execution round. Return total running time of function latitudeAvg.''' 
    
    for count in roundCount:
        total_runTime_2CD = 0
        for _ in range(count):
            startTime = time.time()
            latitudeAvg(textFile)
            endTime = time.time()
            runTime_2CD = endTime - startTime
            total_runTime_2CD += runTime_2CD
        print(f"Query {count} Total Runtime(s) Part 2C-D: {total_runTime_2CD:.4f} seconds")


# In[58]:


latitudeAvg_runTime('650kTweets.txt', [1])


# d) Re-execute the query in part 2-c 5 times and 20 times and measure the total runtime. Does the runtime scale linearly?

# The run time does scale linearly, the more execution times, the longer total running time.

# In[59]:


latitudeAvg_runTime('650kTweets.txt', [5, 20])


# e) Write the equivalent of the 2-a query in python by using regular expressions instead of json.loads(). Do not use json.loads() here. Note that you only need to find userid and geo location (if any) for each tweet, you don’t need to parse the whole thing.

# In[60]:


import re

def latitudeAvg_regex(textFile):
    '''Take text file input and calculate average latitude of each user.'''
    
    userLatitudes = {}
    userID_pattern = re.compile(r'"user":{"id":(\d+)')
    geo_pattern = re.compile(r'"geo":{"type":"Point","coordinates":\[(\-?\d+\.\d+),\s*(-?\d+\.\d+)\]}')
    
    with open(textFile, 'r', encoding='utf-8') as inFile:
        for row in inFile:
            userID_match = userID_pattern.search(row)
            geo_match = geo_pattern.search(row)
        
            if userID_match and geo_match:
                userID = int(userID_match.group(1))
                latitude = float(geo_match.group(2))
                if userID in userLatitudes:
                    userLatitudes[userID].append(latitude)
                else:
                    userLatitudes[userID] = [latitude]
        
        results = []
        
        for userID, latitudes in userLatitudes.items():
            latitudeAve_sumCount = sum(latitudes) / len(latitudes)
            latitudeAve_Stat = statistics.mean(latitudes)
            results.append((userID, latitudeAve_sumCount, latitudeAve_Stat))
        
        return results
    
def latitudeAvg_regex_runTime(textFile, roundCount):
    '''Take input text file and number of execution round. Return total running time of function latitudeAvg.'''
    
    for count in roundCount:
        total_runTime_2EF = 0
        for _ in range(count):
            startTime = time.time()
            latitudeAvg_regex(textFile)
            endTime = time.time()
            runTime_2EF = endTime - startTime
            total_runTime_2EF += runTime_2EF
        print(f"Query {count} Total Runtime(s) Part 2E-F: {total_runTime_2EF:.4f} seconds")


# In[61]:


latitudeAvg_regex_runTime('650kTweets.txt', [1])


# In[62]:


latitudeAvg_regex_runTime('650kTweets.txt', [5, 20])

