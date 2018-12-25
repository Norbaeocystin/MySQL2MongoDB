'''
date: December 2018

Short snippet of code to transfer data from MySQL tables to MongoDB

dependencies: pymongo, mysql.connector

can be installed by:

pip install mysql-connector-python
pip install pymongo


it is possible to change mysql.connector to cx_Oracle to migrate data from Oracle to Mongo, 
Psycopg2 to migrate data from PostgresSQL to Mongo, of course it needs some tweaking ...

'''
import mysql.connector
from pymongo import MongoClient

#change values below 
USER = 'user'
PASSWORD = 'password'
HOST = '127.0.0.1'
DATABASE = 'videos'

#connection string to your MySQL server, this is case where is used default port for MySQL
cnx = mysql.connector.connect(host = HOST, password = PASSWORD, user= USER)
cursor = cnx.cursor()

# Connectio URI can be in shape mongodb://<username>:<password>@<ip>:<port>/<authenticationDatabase>')
CONNECTION = MongoClient('mongodb://localhost')

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
    with big tables it is better to use generator not fetchall data
    
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

def insert_into_mongodb(mysql_table, mysql_database, mongodb_collection, mongodb_database):
    '''
    get data from MySQL table and will store them in MongoDB 
    
    arguments:
    
    mysql_table: <string>, name of specific MySQL table
    mysql_database: <string>, name of database where is table stored
    mongodb_collection: <string>, name of MongoDB collection, it may be not existent
    mongodb_database: <string>, name of MongoDB database, it may be not existent
    '''
    collection = get_dict_from_table(mysql_table, mysql_database)
    db = CONNECTION[mongodb_database]
    coll = db[mongodb_collection]
    coll.insert_many(collection)
    
def insert_table_relative_to_collection(mysql_table, mysql_database, mongodb_collection, mongodb_database, relative_field):
    '''
    example: if you have two MySQL tables and you already migrate one table to mongodb collection and 
    not yet migrated table is relative to already migrated table you can store data from non migrated table to MongoDB collection
    as array of objects in documents
    arguments:
    
    mysql_table: <string>, name of specific MySQL table
    mysql_database: <string>, name of database where is table stored
    mongodb_collection: <string>, name of MongoDB collection, it is already migrated table
    mongodb_database: <string>, name of MongoDB database
    relative_field: <string>, relative field is in MySQL column name and in MongoDB name of field
    '''
    collection = get_dict_from_table(mysql_table, mysql_database)
    db = CONNECTION[mongodb_database]
    coll = db[mongodb_collection]
    for item in collection:
        rf = item[relative_field]
        #this deletion is removing rebundancy of relative field in original document and in inserted array
        #if you remove it and relative field will be in original document and also in inserted array
        del item[relative_field] 
        coll.update({relative_field: rf}, {'$push': {mysql_table: item}})
    
