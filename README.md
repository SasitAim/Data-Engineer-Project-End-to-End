# Data-Engineer-Project-End-to-End
Data Engineer Project end to end with public dataset thai_road_accident_2019_2022



### Project Overview
This data engineer ETL project will begin by importing a public dataset (Thai road accidents 2019-2022) into a MySQL database by Python. After that, the data will be cleaned using SSIS by extracting data from the SSIS database. when the cleaning process is complete, the data will be sent to the SSMS database (Assumed to be the Data Warehouse). The final step involves visualizing and analyzing the data using Power BI.

### Tools
- SQL Server Integration Services (SSIS)
- SQL Server Management Studio (SSMS)
- MySQL
- PowerBI

### Questions
1. How many accidents occurred in each year?
2. The top 10 provinces with the highest number of accidents and the 10 provinces with the lowest number of accidents.
3. During which time periods do accidents occur most frequently?
4. The highest number of jointly involved vehicle accidents.
5. Which weather conditions have the highest number of accidents?
6. Which type of road has the highest number of accidents?
7. Number of injuries and fatalities each year.
8. Which type of accidents occurs most frequently?

### Step 1 Create database in MySQL and import data. 
```sql
-- First create database 
CREATE DATABASE datade ;

USE datade ;
```

### Import data from Kaggle
```python
# import library
import opendatasets as od 
import os 
import pandas as pd


# identify the dataset's URL on Kaggle
url_data = 'https://www.kaggle.com/datasets/thaweewatboy/thailand-road-accident-2019-2022?select=thai_road_accident_2019_2022.csv'


# download dataset form kaggle by identify URL of dataset 
# used Kaggle AIP username and key
od.download(url_data)


# identify directory for save data 
data_dir = './thailand-road-accident-2019-2022'
data_dir


# download file in directory 
# output is file name 
os.listdir(data_dir)

```

### Create data frame and Connect database
```python
# Create data frame
file_path = r"{}/thai_road_accident_2019_2022.csv".format(data_dir)
df = pd.read_csv(file_path)
df

# Overview of Data
df.head()
df.info()
df.isnull().sum()


# Connect to MySQL
# Import the neccessary modules
from sqlalchemy import create_engine as ce


# Detail for connect MySQL
host = 'localhost' #localhost
user = 'root' # user name
password = 'password' # Your password
database = 'database name' # Your database name


# Create connection
engine = ce(f'mysql+pymysql://{user}:{password}@{host}/{database}')


# Import DataFrame DB
df.to_sql(name='thai_RAC', con=engine, if_exists='replace', index=False)
host = 'localhost'
user = 'root'
password = 'password'
database = 'database name'


# close connections
engine.dispose()
```


### Step 2 Cleaning data in MySQL.
```sql
-- SELECT database
USE datade ;
SELECT * FROM datade.thai_rac;

-- Check data type in each column
DESCRIBE thai_rac;


-- check length in each columns
 SELECT 
  MAX(CHAR_LENGTH(acc_code)) AS acc_code_len,
  MAX(CHAR_LENGTH(province_en)) AS province_en_len,
  MAX(CHAR_LENGTH(province_th)) AS province_th_len,  
  MAX(CHAR_LENGTH(agency)) AS province_th_len,
  MAX(CHAR_LENGTH(route)) AS province_th_len,
  MAX(CHAR_LENGTH(vehicle_type)) AS province_th_len,
  MAX(CHAR_LENGTH(presumed_cause)) AS province_th_len,
  MAX(CHAR_LENGTH(accident_type)) AS province_th_len,
  MAX(CHAR_LENGTH(weather_condition)) AS slope_description_len,
  MAX(CHAR_LENGTH(road_description)) AS province_th_len,
  MAX(CHAR_LENGTH(slope_description)) AS province_th_len
FROM thai_rac;


-- Change data type and 
ALTER TABLE thai_rac
MODIFY COLUMN acc_code VARCHAR (10),
MODIFY COLUMN incident_datetime TIMESTAMP,
MODIFY COLUMN report_datetime TIMESTAMP,
MODIFY COLUMN province_en VARCHAR (25),
MODIFY COLUMN province_th VARCHAR (50),
MODIFY COLUMN agency VARCHAR (35),
MODIFY COLUMN route VARCHAR (280),
MODIFY COLUMN vehicle_type VARCHAR (30),
MODIFY COLUMN presumed_cause VARCHAR (75),
MODIFY COLUMN accident_type VARCHAR (45),
MODIFY COLUMN weather_condition VARCHAR (20),
MODIFY COLUMN road_description VARCHAR (36),
MODIFY COLUMN slope_description VARCHAR (10);
```

### Step 3 Extract, transform and load (ETL) with SSIS.



### Step 4 Answer a question in SSMS.
```sql
USE PJDB

SELECT *
FROM thai_rac ;

-- QUESTION

-- Q1 How many accidents occurred in each year?
-- number of accidents that occurred each year.
SELECT YEAR(incident_date_n) AS year, COUNT(*) AS Number_of_accident
FROM thai_rac
WHERE YEAR(incident_date_n) IN (2019, 2020, 2021, 2022)
GROUP BY YEAR(incident_date_n)
ORDER BY YEAR(incident_date_n);


--Q2 The top 10 provinces with the highest number of accidents and the 10 provinces with the lowest number of accidents.
SELECT TOP 10 province_en, COUNT(province_en) AS Number_of_accident
FROM thai_rac 
GROUP BY province_en
ORDER BY Number_of_accident DESC;

SELECT TOP 10 province_en, COUNT(province_en) AS Number_of_accident
FROM thai_rac
WHERE province_en != 'unknown'
GROUP BY province_en
ORDER BY Number_of_accident ;


--Q3  During which time periods do accidents occur most frequently?
-- Total accident in each time of day.
SELECT 
	CASE
		WHEN CAST(incident_datetime AS TIME) BETWEEN '00:00:00' AND '05:00:00' THEN 'Night'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '05:00:01' AND '12:00:00' THEN 'Morning'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '12:00:01' AND '17:00:00' THEN 'Afternoon'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '17:00:01' AND '20:00:00' THEN 'Evening'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '20:00:01' AND '23:59:59' THEN 'Night'
		ELSE 'Other time'
	END AS  Time_period,
	COUNT(*) AS Number_of_accident

FROM thai_rac
GROUP BY 
	CASE
		WHEN CAST(incident_datetime AS TIME) BETWEEN '00:00:00' AND '05:00:00' THEN 'Night'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '05:00:01' AND '12:00:00' THEN 'Morning'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '12:00:01' AND '17:00:00' THEN 'Afternoon'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '17:00:01' AND '20:00:00' THEN 'Evening'
		WHEN CAST(incident_datetime AS TIME) BETWEEN '20:00:01' AND '23:59:59' THEN 'Night'
		ELSE 'Other time'
	END ;


--Q4 The highest number of jointly involved vehicle accidents.
SELECT number_of_vehicles_involved, COUNT(number_of_vehicles_involved) AS Number_of_accident
FROM thai_rac 
GROUP BY number_of_vehicles_involved 
ORDER BY number_of_vehicles_involved ;


--Q5 Which weather conditions have the highest number of accidents?
-- Total accident in each weather condition.
SELECT weather_condition, COUNT(weather_condition) AS Number_of_accident
FROM thai_rac
GROUP BY weather_condition
ORDER BY Number_of_accident ;


--Q6 Which type of road has the highest number of accidents?
-- Total accidents in each road type. 
SELECT road_description, COUNT(road_description) AS Number_of_accident
FROM thai_rac
WHERE road_description != 'other'
GROUP BY road_description
ORDER BY Number_of_accident ;



--Q7 Number of injuries and fatalities each year.
-- accident
SELECT 
	YEAR(incident_date_n) AS Year_acc,
	MONTH(incident_date_n) AS Month_acc,
	COUNT(*) AS number_of_accident
FROM thai_rac
GROUP BY YEAR(incident_date_n), MONTH(incident_date_n)
ORDER BY Year_acc, Month_acc;

-- injuries
SELECT 
	YEAR(incident_date_n) AS Year_acc,
	MONTH(incident_date_n) AS Month_acc,
	SUM(number_of_injuries) AS number_of_injuries
FROM thai_rac
GROUP BY YEAR(incident_date_n), MONTH(incident_date_n)
ORDER BY Year_acc, Month_acc;

-- fatalities
SELECT 
	YEAR(incident_date_n) AS Year_acc,
	MONTH(incident_date_n) AS Month_acc,
	SUM(number_of_fatalities) AS number_of_fatalities
FROM thai_rac
GROUP BY YEAR(incident_date_n), MONTH(incident_date_n)
ORDER BY Year_acc, Month_acc;

-- all accident, injuries and fatalities
SELECT 
	YEAR(incident_date_n) AS Year,
	MONTH(incident_date_n) AS Month,
	COUNT(*) AS number_of_accident,
	SUM(number_of_injuries) AS number_of_injuries,
	SUM(number_of_fatalities) AS number_of_fatalities
FROM thai_rac
GROUP BY  YEAR(incident_date_n), MONTH(incident_date_n)
ORDER BY Year, Month;

--Q8 Which type of accidents occurs most frequently?
SELECT accident_type, COUNT(accident_type) AS number_accident
FROM thai_rac
GROUP BY accident_type
ORDER BY number_accident DESC ;


-- Total accident in each province
SELECT 
	province_en,
	COUNT (*) AS number_accident

FROM thai_rac
WHERE province_en != 'unknown'
GROUP BY province_en
ORDER BY province_en ;



```
