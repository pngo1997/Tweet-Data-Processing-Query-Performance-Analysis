# 🏗️ Tweet Data Processing & Query Performance Analysis  

## 📜 Overview  
This project involves **processing, storing, querying, and analyzing tweet data** from a large dataset (4.4M tweets - one day of tweet data). The tasks include **downloading tweets, storing them in a SQLite database, optimizing database operations, comparing query execution performance in SQL vs. Python, and exporting processed data in multiple formats (JSON, CSV)**.  

## 🎯 Problem Explanation  
Tasks are divided into three major sections:  

1. **Processing & Storing Tweets:**  
   - Populate a **3-table schema in SQLite** and measure execution time.  
   - Optimize database inserts using **batching (executemany)**.  
   - Compare execution time across different methods.  

2. **Query Execution & Performance Analysis:**  
   - Execute SQL queries vs. equivalent Python-based queries.  
   - Analyze **linear scalability** of query execution.  
   - Implement **regular expressions** as an alternative to `json.loads()`.  

3. **Data Export & Storage Format Comparison:**  
   - Create a **materialized view** (using `CREATE TABLE AS SELECT`).  
   - Export processed data to **JSON and CSV formats**.  
   - Compare **file sizes** to evaluate the most efficient storage format.  

## 🛠️ Implementation Details  

### **1. Processing & Storing Tweets**  
- **Downloaded tweet data** (130K & 650K tweets) and stored them in a text file.  
- **Populated SQLite tables**:  
  - `Tweet` (Tweet ID, User ID, Text, GeoFK)  
  - `User` (User ID, Screen Name, Friends Count)  
  - `Geo` (Geo ID, Longitude, Latitude)  
- **Optimized insert operations**:  
  - **Single inserts** vs. **batch inserts (executemany, batch size = 2500)**.  


### **2. Query Execution & Performance Analysis**  
- **SQL Queries Executed (for each tweet batch):**  
  - **Find average latitude per user:**  
    ```sql
    SELECT UserID, AVG(latitude), SUM(latitude)/COUNT(latitude)
    FROM Tweet, Geo WHERE Tweet.GeoFK = Geo.GeoID 
    GROUP BY UserID;
    ```
  - **Measure runtime across multiple executions (1x, 5x, 20x).**  
- **Python Query Execution:**  
  - Read & process tweets **without SQL**.  
  - Compare execution time to SQL.  
- **Regular Expression Approach:**  
  - Extract UserID and Geo info using **regex instead of `json.loads()`**.  


### **3. Data Export & Storage Format Comparison**  
- **Created a `Tweet_Join` table** (joins Tweet, User, Geo).  
- Exported processed data into JSON & CSV formats.
- File Size Comparison
