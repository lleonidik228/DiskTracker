from tqdm import tqdm
from main import convert_time_stamp_to_time
import sqlite3


def clear_table(connection, cursor, table: str):
    cursor.execute(f"""DELETE FROM {table}""")
    cursor.execute(f"""DELETE FROM sqlite_sequence WHERE name='{table}'""")
    connection.commit()


def create_table(connection, cursor, table, **kwargs):
    cursor.execute(f"""
                CREATE TABLE {table} (
                        Number INTEGER PRIMARY KEY AUTOINCREMENT
                )
                    """)
    # print(kwargs)
    for name_column in kwargs:
        cursor.execute(f"""ALTER TABLE {table} ADD COLUMN {name_column} {kwargs[name_column]}""")

    connection.commit()
    return


def filter_last_modification():
    TABLE = "ChangedFiles"
    print("[+]", TABLE, "database in progress")
    # initial connection and cursor
    # refactor database before work


    # if in database don't exist needed table, we will create it
    if not cursor_filter_db.execute(f"""SELECT name FROM sqlite_master WHERE type='table' AND name=?""", (TABLE,)).fetchone():
        create_table(connection=connect_filter_db, cursor=cursor_filter_db, table=TABLE, **{
            "Full_path": "TEXT",
            "Extension": "TEXT",
            "Time_Before": "TEXT",
            "Time_After": "TEXT",
            "Time_stamp_before": "INTEGER",
            "Time_stamp_after": "INTEGER"
            }
        )
    else:
        clear_table(connection=connect_filter_db, cursor=cursor_filter_db, table=TABLE)  # clear table before work


    connect_filter_db.commit()

    first_data = cursor_first_db.execute("SELECT Full_path, Date_of_last_change_time_stamp FROM Database").fetchall()
    second_data = dict(cursor_second_db.execute("SELECT Full_path, Date_of_last_change_time_stamp FROM Database").fetchall())
    second_data_with_extension = dict(cursor_second_db.execute("SELECT Full_path, Extension FROM Database").fetchall())

    for first_path, first_time_stamp in tqdm(first_data):
        if first_path in second_data:
            if first_time_stamp != second_data[first_path]:
                cursor_filter_db.execute("""
                    INSERT INTO ChangedFiles (
                    Full_path,
                    Extension,
                    Time_Before,
                    Time_After,
                    Time_stamp_before,
                    Time_stamp_after
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (first_path, second_data_with_extension[first_path], convert_time_stamp_to_time(first_time_stamp),
                      convert_time_stamp_to_time(second_data[first_path]), first_time_stamp, second_data[first_path])
                    )


    connect_filter_db.commit()
    return


def filter_added_files():
    TABLE = "AddedFiles"
    print("[+]", TABLE, "database in progress")
    # if in database don't exist needed table, we will create it
    if not cursor_filter_db.execute(f"""SELECT name FROM sqlite_master WHERE type='table' AND name=?""", (TABLE,)).fetchone():
        create_table(connect_filter_db, cursor_filter_db, TABLE, **{
            "Added_files": "TEXT",
            "Extension": "TEXT"
        }
                     )
    else:
        clear_table(connection=connect_filter_db, cursor=cursor_filter_db, table=TABLE)

    # Refactor table
    path_from_first_db = set(i[0] for i in cursor_first_db.execute("""SELECT Full_path FROM DataBase""").fetchall())
    path_and_extension_from_second_db = dict(
        cursor_second_db.execute("""SELECT Full_path, Extension FROM DataBase""").fetchall())

    added_files = (set(i for i in path_and_extension_from_second_db.keys())
                   - path_from_first_db)

    for added_file in tqdm(added_files):

        cursor_filter_db.execute("""INSERT INTO AddedFiles(
            Added_files,
            Extension
        ) values(?, ?)""", (added_file, path_and_extension_from_second_db[added_file])
                             )

    connect_filter_db.commit()
    return


def filter_removed_files():
    TABLE = "RemovedFiles"
    print("[+]", TABLE, "database in progress")
    if not cursor_filter_db.execute(f"""SELECT name FROM sqlite_master WHERE type='table' AND name=?""", (TABLE,)).fetchone():
        create_table(connect_filter_db, cursor_filter_db, TABLE, **{
            "Removed_files": "TEXT",
            "Extension": "TEXT"
        }
                     )
    else:
        clear_table(connection=connect_filter_db, cursor=cursor_filter_db, table=TABLE)

    path_from_first_db = dict(cursor_first_db.execute("""SELECT Full_path, Extension FROM DataBase""").fetchall())
    path_from_second_db = set(i[0] for i in cursor_second_db.execute("""SELECT Full_path FROM DataBase""").fetchall())

    removed_files = path_from_first_db.keys() - path_from_second_db
    # print(removed_files)

    for removed_file in tqdm(removed_files):
        cursor_filter_db.execute("""INSERT INTO RemovedFiles(
            Removed_files,
            Extension
        ) values(?, ?)""", (removed_file, path_from_first_db[removed_file])
                             )
    connect_filter_db.commit()
    return



if __name__ == "__main__":

    # connect to databases
    connection_first_db = sqlite3.connect(r'firstChecking.db')
    connection_second_db = sqlite3.connect(r'secondChecking.db')
    connect_filter_db = sqlite3.connect(r'filter.db')

    # creating cursors
    cursor_first_db = connection_first_db.cursor()
    cursor_second_db = connection_second_db.cursor()
    cursor_filter_db = connect_filter_db.cursor()

    # starting working with filter
    filter_last_modification()
    filter_added_files()
    filter_removed_files()

    # commit and close
    connect_filter_db.commit()
    connect_filter_db.close()
    connection_first_db.close()
    connection_second_db.close()
