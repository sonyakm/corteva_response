import psycopg2
from psycopg2 import OperationalError, errorcodes

# Database connection parameters
dbname = "wxdata"
dbuser = "postgres"
dbpassword = "test"
dbhost = "localhost"
dbport = "5432"

# PostGreSQL utilities

def create_table(conn, create_table_sql, logger):
    """
    Creates a new table in the PostgreSQL database.

    Input:
        conn: The connection object to the PostgreSQL database
        table_name: The name of the table
        create_table_sql: The SQL statement for creating the table
    """
    try:
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
    except OperationalError as e:
        logger.exception(f"Error creating table: {e}")
        conn.rollback()
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        conn.rollback()

    return True

def connect_to_db(logger):
    """
    Connects to a PostgreSQL database.

    Input:
        dbname: database name
        user: username
        password: password
        host: hostname of the database server 
        port: port number for connecting to database 

    Returns:
        A connection object if the connection is successful, None otherwise.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=dbuser,
            password=dbpassword,
            host=dbhost,
            port=dbport
        )
        #print("Connected to the database successfully.")
    except OperationalError as e:
        logger.exception(f"Error connecting to the database: {e}")
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            logger.exception("Please check your password and try again.")
        elif e.pgcode == errorcodes.INVALID_CATALOG_NAME:
            logger.exception("Database does not exist.  Please check the database name.")
        elif e.pgcode == errorcodes.CONNECTION_DOES_NOT_EXIST:
            logger.exception("Check that the server is running and accepting connections")
        else:
            logger.exception(f"Error Details: {e.diag.message_detail}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    return conn

def init_table(table_sql, logger):
    """
    Connect to the wxdata database and create the station_data table.
    Input:
        table_sql: the SQL DDL code for the table
    """

    conn = connect_to_db(logger)
    if conn is not None:
        create_table(conn, table_sql, logger)
        conn.close()
    else:
        logger.exceptions("Failed to connect to the database.")


def execute_insert_db(conn, logger, sqlcommand, data=None):
    """
    Executes the given command

    Input:
        conn: The connection object to the db
        sqlcommand: The PostgreSQL command
        logger: logging object 

    """
    cursor = conn.cursor()
    try:
        if data:
            cursor.execute(sqlcommand, data)
        else:
            cursor.execute(sqlcommand)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.exception(f"Error inserting or updating data: {e}")
    finally:
        cursor.close()


def execute_select_db(conn, logger, sqlcommand, data=None):
    """
    Executes the given command

    Input:
        conn: The connection object to the db
        sqlcommand: The PostgreSQL command
        logger: logging object 
    Output: 
        results of select command

    """
    cursor = conn.cursor()
    try:
        if data:
            cursor.execute(sqlcommand, data)
        else:
            cursor.execute(sqlcommand)
        return cursor.fetchall()
    except psycopg2.Error as e:
        conn.rollback()
        logger.exception(f"Error inserting or updating data: {e}")
        return None
    finally:
        cursor.close()
