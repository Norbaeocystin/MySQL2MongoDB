'''
date: December 2018

Short snippet of code to transfer data from 2 related tables in MySQL to MongoDB

dependencies: pymongo, mysql.connector

can be installed by:

pip install mysql-connector-python
pip install pymongo

Tables and data where created by this SQL commands:

DROP TABLE IF EXISTS videos; 
DROP TABLE IF EXISTS video_reviews; 
create table videos ( id varchar(5) not null primary key, video_title varchar(100) not null, lenght int(5) not null, url varchar(50) not null ); 
create table video_reviews ( user_name varchar(15) not null primary key, rating int (1), review varchar(30), id varchar(5) ); 
insert into videos values (112, 'SQL for Beginners. Learn basics of SQL in 1 Hour', 57, 'https://www.youtube.com/watch?v=7Vtl2WggqOg'); 
insert into videos values(113, 'Top 15 Advanced Excel 2016 Tips and Tricks', 22, 'https://www.youtube.com/watch?v=PU8ACyYxJBk'); 
insert into videos values( 114, '10 Super Neat Ways to Clean Data in Excel', 1, 'https://www.youtube.com/watch?v=e0TfIbZXPeA'); 
insert into video_reviews values ('emily_12', 4, 'very helpful!', 112); 
insert into video_reviews values ('adva77', 5, 'great SQL video', 112); 
insert into video_reviews values ('kim122', 3, 'basic tricks. not too helpful', 113); 
insert into video_reviews values ('roni_v1', 4, 'helpul tips!', 113); 

'''
import mysql.connector

#change values below 
USER = 'user'
PASSWORD = 'password'
HOST = '127.0.0.1'
DATABASE = 'videos'

#connection string to your MySQL server, this is case where is used default port for MySQL
cnx = mysql.connector.connect(host = HOST, password = PASSWORD, user= USER)
cursor = cnx.cursor()

def get_data_from_table(table, database = DATABASE):
    '''
    returns all data from specific table
    
    arguments:
    
    table: <string>, name of specific table
    database: <string>, name of database where is table stored
    '''
    cursor.execute('USE {};'.format(database))
    cursor.execute('SELECT * FROM {};'.format(table))
    return cursor.fetchall()

def get_columns_from_table(table, database = DATABASE):
    '''
    returns columns name from specific table,
    with big tables is it better to use generator not fetchall data
    
    arguments:
    
    table: <string>, name of specific table
    database: <string>, name of database where is table stored
    '''
    cursor.execute('USE {};'.format(database))
    cursor.execute('SHOW COLUMNS FROM {};'.format(table))
    return [item[0] for item in cursor.fetchall()]

def get_dict_from_table(table, database = DATABASE):
    '''
    converts data from table to dictionary and returns list of dictionaries, which
    can be inserted in Mongo database 
    
    arguments:
    
    table: <string>, name of specific table
    database: <string>, name of database where is table stored
    '''
    columns = get_columns_from_table(table, database)
    table = get_data_from_table(table, database)
    result = []
    for i, k in enumerate(table):
        document = {}
        for j,l in enumerate(columns):
            document[l] = table[i][j]
        result.append(document)
    return result
