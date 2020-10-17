import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import argparse
import time


def load_staging_tables(cur, conn):
    start = time.time()
    print("***************** Loading data into staging tables ***********************")
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Can't execute query : {query}")
            print(e)
    print("***************** Done ***********************")
    print(f"***************** Script ran in {int(time.time() - start)} seconds ***********************")


def insert_tables(cur, conn):
    start = time.time()
    print("***************** Loading data into Analytics tables ***********************")
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Can't execute query : {query}")
            print(e)
    print("***************** Done ***********************")
    print(f"***************** Script ran in {int(time.time() - start)} seconds ***********************")


def main(task="staging"):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            config.get("CLUSTER", "HOST"),
            config.get("CLUSTER", "DB_NAME"),
            config.get("CLUSTER", "DB_USER"),
            config.get("CLUSTER", "DB_PASSWORD"),
            config.get("CLUSTER", "DB_PORT")
        ))
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Can't connect to database")
        print(e)

    if task == "staging":
        load_staging_tables(cur, conn)
    else:
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract data from S3 and load into redshift")
    parser.add_argument("-t", "--task", help="ETL task to perform: staging or analytics",
                        required=True, choices=["staging", "analytics"])
    args = parser.parse_args()
    
    main(args.task)
