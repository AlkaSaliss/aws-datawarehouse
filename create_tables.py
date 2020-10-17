import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Executes queries that drop the staging, fact and dimension tables

    Args:
        cur: cursor for executing query against the connected database
        conn : database connection object
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(f"Can't execute query : {query}")
            print(e)


def create_tables(cur, conn):
    """Executes queries that creates the staging, fact and dimension tables

    Args:
        cur: cursor for executing query against the connected database
        conn : database connection object
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
