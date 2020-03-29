import psycopg2
from sql_queries import list_drop_tables, list_create_tables

def insertfromdataframe(hostname, dbname, table, dataframe):
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

def db_connection(hostname, dbname):
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
    conn, cur = db_connection(hostname, dbname)

    for i_drop in list_drop_tables:
        cur.execute(i_drop)

    for i_create in list_create_tables:
        cur.execute(i_create)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main("127.0.0.1", "sparkifydb")