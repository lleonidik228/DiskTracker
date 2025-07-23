from tqdm import tqdm
from main import convert_time_stamp_to_time, SLASH
import sqlite3


def connect_to_filter_data_base():
    def create_data_base():
        cursor.execute("""
                    CREATE TABLE ChangedFiles (
                            Number INTEGER PRIMARY KEY AUTOINCREMENT,
                            Full_path TEXT,
                            Time_Before TEXT,
                            Time_After TEXT,
                            Time_stamp_before INTEGER,
                            Time_stamp_after INTEGER
                    )
                        """)
        return

    def drop_data_base():
        cursor.execute("""DROP TABLE ChangedFiles""")
        return


    connect_filter_db = sqlite3.connect(r'filter.db')
    cursor = connect_filter_db.cursor()
    drop_data_base()
    create_data_base()
    connect_filter_db.commit()

    return connect_filter_db, cursor


def filter_result():
    connection_first_db = sqlite3.connect(r'firstChecking.db')
    connection_second_db = sqlite3.connect(r'secondChecking.db')

    cursor_first_db = connection_first_db.cursor()
    cursor_second_db = connection_second_db.cursor()

    connect_filter_db, cursor_filter_db = connect_to_filter_data_base()

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
    connection_first_db.close()
    connection_second_db.close()


if __name__ == "__main__":
    # connect_to_filter_data_base()
    filter_result()

