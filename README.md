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
1. 
2. 
3. 
4. 
5. 
6. 
7. 
8. 

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
