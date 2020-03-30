import psycopg2
from sql_queries import list_drop_tables, list_create_tables


def insert_from_dataframe(hostname, dbname, table, dataframe):
    """
    Insert Rows in a db table from a dataframe
    :param hostname: Host Database Address
    :param dbname: Database Name
    :param table: Table Name
    :param dataframe: Pandas Dataframe
    """
    conn, cur = db_connection(hostname, dbname)

    for index, row in dataframe.iterrows():
        try:
            cur.execute('''INSERT INTO ''' + table + '''
            VALUES (''' + ','.join(['%s' for x in row]) + ''');''',
        row)
        except psycopg2.Error as e:
            print("Error insert cursor")
            print(e)

    cur.close()
    conn.close()


def create_database(hostname, dbname):
    """
    - Drop and creates the database
    :param hostname: Host Database Address
    :param dbname: Database Name
    :return: connection conn and a cursor cur
    """

    # connect to default database
    conn = psycopg2.connect("host=" + hostname + " dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS " + dbname)
    cur.execute("CREATE DATABASE " + dbname + " WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    cur.close()
    conn.close()


def db_connection(hostname, dbname):
    """
    - Connects to a database
    - Returns the connection and cursor to database
    :param hostname: Host Database Address
    :param dbname: Database Name
    :return: connection conn and a cursor cur
    """
    try:
        conn = psycopg2.connect("host="+ hostname + " dbname=" + dbname)
        conn.set_session(autocommit=True)
    except psycopg2.Error as e:
        print("Error connection to database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error init cursor")
        print(e)

    return conn, cur


def main(hostname, dbname):
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    :param hostname: Host Database Address
    :param dbname: Database Name
    """
    create_database(hostname, dbname)
    conn, cur = db_connection(hostname, dbname)

    # Drops each table using the queries in `list_drop_tables` list.
    for query in list_drop_tables:
        cur.execute(query)

    # Creates each table using the queries in `list_create_tables` list.
    for query in list_create_tables:
        cur.execute(query)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main("127.0.0.1", "sparkifydb")