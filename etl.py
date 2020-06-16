import sys
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    """
    A function to connect to the database and create a cursor and Load staging data 
    into approriate tables
    conn: database connection
    cur: create database cursor
    """
    
    for query in copy_table_queries:
        #The print statement here helps for easy debugging
        print(query['message'])
        cur.execute(query['query'])
        conn.commit()


def insert_tables(cur, conn):
    
    for query in insert_table_queries:
        #The print statement here helps for easy debugging
        print(query['message'])
        cur.execute(query['query'])
        conn.commit()
           


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    #Print success message for loading staging data
    print("===========> All Staging tables loaded")
    #print sucess message for loading table data
    insert_tables(cur, conn)
    print("===========> All Tables Inserted")

    conn.close()


if __name__ == "__main__":
    main()