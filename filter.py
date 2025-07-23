from tqdm import tqdm
from main import convert_time_stamp_to_time, SLASH
import sqlite3
import time


def drop_table(connection, cursor, table: str):
    cursor.execute(f"""DROP TABLE {table}""")
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
    # initial connection and cursor
    connect_filter_db = sqlite3.connect(r'filter.db')
    cursor_filter_db = connect_filter_db.cursor()


    # refactor database before work
    drop_table(connection=connect_filter_db, cursor=cursor_filter_db, table="ChangedFiles")
    create_table(connection=connect_filter_db, cursor=cursor_filter_db, table="ChangedFiles", **{
                "Full_path": "TEXT",
                "Time_Before": "TEXT",
                "Time_After": "TEXT",
                "Time_stamp_before": "INTEGER",
                "Time_stamp_after": "INTEGER"
            }
                 )


    first_data = cursor_first_db.execute("SELECT Full_path, Date_creation_time_stamp FROM Database").fetchall()
    second_data = dict(cursor_second_db.execute("SELECT Full_path, Date_creation_time_stamp FROM Database").fetchall())

    for first_path, first_time_stamp in tqdm(first_data):
        if first_path in second_data:
            if first_time_stamp != second_data[first_path]:
                cursor_filter_db.execute("""
                    INSERT INTO ChangedFiles (
                    Full_path,
                    Time_Before,
                    Time_After,
                    Time_stamp_before,
                    Time_stamp_after
                    ) VALUES (?, ?, ?, ?, ?)
                """, (first_path, convert_time_stamp_to_time(first_time_stamp),
                      convert_time_stamp_to_time(second_data[first_path]), first_time_stamp, second_data[first_path])
                    )


    connect_filter_db.commit()
    connect_filter_db.close()
    connection_first_db.commit()
    connection_second_db.commit()


def filter_added_files():

    connect_filter_db = sqlite3.connect(r'filter.db')
    cursor_filter_db = connect_filter_db.cursor()
    # Refactor table
    drop_table(connection=connect_filter_db, cursor=cursor_filter_db, table="AddedFiles")
    create_table(connect_filter_db, cursor_filter_db, "AddedFiles", **{
        "Added_files": "TEXT",
        "Time_creation": "TEXT"
        }
    )

    added_files = (set(cursor_first_db.execute("""SELECT Full_path FROM DataBase""").fetchall()) -
                   set(cursor_second_db.execute("""SELECT Full_path FROM DataBase""").fetchall()))

    for added_file in tqdm(added_files):

        start_time = time.time()
        cursor_filter_db.execute("""INSERT INTO AddedFiles(
            Added_files,
            Time_creation
        ) values(?, ?)""", (added_file[0], *cursor_first_db.execute(f"""SELECT Date_creation FROM DataBase WHERE Full_path = ?""", (added_file[0],)).fetchall()[0])
                             )
        # заметки по проге, сделать так чтобы премя создания файла шло вместе с его именем в словарь, бд слишком большая и потому
        # каждый новый запрос сканит всю бд на поиск нужного значения, поэтому надо засунуть все в дикт и рабоать по ключам, а то что я сделал - гавно долгое

        print(time.time() - start_time)

    connect_filter_db.commit()
    connect_filter_db.close()
    return



if __name__ == "__main__":
    # connect_to_filter_data_base()
    connection_first_db = sqlite3.connect(r'firstChecking.db')
    connection_second_db = sqlite3.connect(r'secondChecking.db')
    cursor_first_db = connection_first_db.cursor()
    cursor_second_db = connection_second_db.cursor()

    # filter_last_modification()
    filter_added_files()
    connection_first_db.close()
    connection_second_db.close()





#
# def create_data_base(connection, cursor, table, **kwargs):
#     cursor.execute("""
#                 CREATE TABLE ChangedFiles (
#                         Number INTEGER PRIMARY KEY AUTOINCREMENT,
#                         Full_path TEXT,
#                         Time_Before TEXT,
#                         Time_After TEXT,
#                         Time_stamp_before INTEGER,
#                         Time_stamp_after INTEGER
#                 )
#                     """)
#     return