import psycopg2
from psycopg2.extensions import AsIs, quote_ident
import datetime

def save(df):
    # create connection object, create a table with schema, if not exist
    conn = psycopg2.connect(database="postgres", user = "postgres", password = "postgres", host = "127.0.0.1", port = "5432")
    cur = conn.cursor()

    #to avoid dublicates in DB for multiple run (comment in production)
    cur.execute('DROP TABLE user_logins')

    cur.execute("""CREATE TABLE IF NOT EXISTS user_logins( \
    user_id varchar(128), \
    device_type varchar(32), \
    masked_ip varchar(256), \
    masked_device_id varchar(256), \
    locale varchar(32), \
    app_version integer, \
    create_date date \
    );""")
    conn.commit()

    #Save to the DB
    for i in range(df.shape[0]):
        cur.execute("""
            INSERT INTO %s
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            AsIs(quote_ident('user_logins', cur)),
            str(df.iloc[i,0]),
            str(df.iloc[i,2]),
            str(df.iloc[i,3]),
            str(df.iloc[i,5]),
            str(df.iloc[i,4]),
            int(df.iloc[i,1]),
            datetime.date.today()
        ))

    # Changing None to NULL in DB
    cur.execute("SELECT NULLIF('None', 'None');")
    conn.commit()

    # Load from DB
    cur.execute("SELECT user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date from user_logins")
    rows = cur.fetchall()
    count=0
    for row in rows:
        print(row[0], row[1], row[2], row[3], row[4], row[5], row[6],"\n")
        count+=1
    print("Total rows at the DB: ",count)
    conn.close()

