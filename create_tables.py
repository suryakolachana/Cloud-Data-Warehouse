import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    This Function drops tables if they exists.
     :Paramaters:     
       :cur: Cursor Method to Execute.
       :Conn: database connection.
     :Returns:
       :None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
      This Function creates tables and specify all columns  with the right data types and conditions.   
     :Paramaters:     
      :cur: Cursor Method to Execute.
      :Conn: database connection.
     :Returns:
      :None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
     This main Function connects to sparkify database, drops tables if exists and creates tables.
     :Paramaters:     
      :None.
     :Returns:
      :None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()