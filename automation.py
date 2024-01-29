# Import libraries required for connecting to mysql
#pip3 install mysql-connector-python
#python3 -m pip install psycopg2
import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='NDI1My1tbnNvci0z',host='127.0.0.1',database='sales')

# create cursor

cursorMySQL = connection.cursor()

# Connect to DB2 or PostgreSql
import psycopg2

# connectction details
dsn_hostname = '127.0.0.1'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='NjAxNS1tbnNvci0x'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"           # i.e. "BLUDB"

# create connection

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

#Crreate a cursor onject using cursor() method

cursorPostgres = conn.cursor()

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    cursorPostgres.execute('SELECT rowid from public.sales_data ORDER BY rowid DESC LIMIT 1')
    last_row_id = cursorPostgres.fetchone()
    return last_row_id[0] if last_row_id else None

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(last_row_id):
    cursorMySQL.execute('SELECT * FROM sales_data WHERE rowid > %s', (last_row_id,))
    return cursorMySQL.fetchall()	

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    if records:
        columns = ['rowid','product_id','customer_id','price','quantity','timeestamp']  
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        query = f"INSERT INTO public.sales_data ({columns_str}) VALUES ({placeholders})"
        
        try:
            cursorPostgres.executemany(query, records)
            conn.commit()
        except IndexError as e:
            print(f"Error: {e}")
            print("Mismatch between the number of columns and values in the records.")
            

insert_records(new_records)
print("New rows inserted into production data warehouse =", len(new_records))


# disconnect from mysql warehouse
connection.close()

# disconnect from DB2 or PostgreSql data warehouse 
conn.close()

# End of program