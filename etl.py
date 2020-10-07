import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Can't execute query : {query}")
            print(e)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Can't execute query : {query}")
            print(e)


def main():
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

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
