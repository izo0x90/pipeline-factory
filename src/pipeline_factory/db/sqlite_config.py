sqlite_file_name = "database.db"
db_url = f"sqlite:///{sqlite_file_name}"

connect_args = {"check_same_thread": False}


def do_connect(dbapi_connection, connection_record):
    # disable pysqlite's emitting of the BEGIN statement entirely.
    # also stops it from emitting COMMIT before any DDL.
    dbapi_connection.isolation_level = None


def do_begin(conn):
    conn.exec_driver_sql("BEGIN EXCLUSIVE")


event_handlers = {"connect": do_connect, "begin": do_begin}
