import sqlite3
import os
from datetime import datetime
from pathlib import Path
import ctypes
from multiprocessing import Process
# import sys
# from tqdm import tqdm

SLASH = r"\\"[0]
LOCAL_C = r'C:' + SLASH


def convert_to_short_path(path: str) -> str:
    buf = ctypes.create_unicode_buffer(260)
    ctypes.windll.kernel32.GetShortPathNameW(r'\\?' + SLASH + path, buf, 260)
    return buf.value


def scan_directory(cursor, path):
    short_path = convert_to_short_path(path)
    try:
        files = os.listdir(short_path)
    except PermissionError:
        print('Access denied[+] ', path, short_path)
        return
    except Exception as e:
        print("unexpected error - ", e)
        print(path, short_path)
        exit()


    for file in files:
        file_in_question = short_path + SLASH + file

        if os.path.isdir(file_in_question):
            try:
                add_values(cursor, "DataBase", (path + SLASH + file, path, short_path, file,
                                                "directory", put_time_creation_to_table(file_in_question), os.path.getmtime(file_in_question)))
                scan_directory(cursor, path + SLASH + file)
            except Exception as e:
                print('critical error - ', e)
                exit()
        else:
            extensions = Path(file_in_question).suffixes
            extensions = ''.join(extensions)
            add_values(cursor, "DataBase", (path + SLASH + file, path, short_path, file,
                                            extensions if extensions else str(None),
                                            put_time_creation_to_table(file_in_question), os.path.getmtime(file_in_question)))

    return


def put_time_creation_to_table(path: str) -> float:
    return os.path.getctime(path)  # return time in seconds(), return time in timestamp from 1970 unix


def get_time_creation_from_table(timestamp_number):
    return datetime.fromtimestamp(timestamp_number)


# def print_result(cursor, request):
#     cursor.execute(f"{request}")
#     print(cursor.fetchall())


def add_values(cursor, table, values: tuple):
    cursor.execute(f"""
            INSERT INTO {table} (
                Full_path,
                Directory,
                Short_path,
                File_name,
                Extension,
                Date_creation,
                Date_of_last_change
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, values)



def drop_data_base(cursor):
    cursor.execute("""DROP TABLE DataBase""")
    print("Data base was deleted")


def create_table(cursor):
    cursor.execute("""
        CREATE TABLE DataBase(
            Number INTEGER PRIMARY KEY AUTOINCREMENT,
            Full_path TEXT,
            Directory TEXT,
            Short_path TEXT,
            File_name TEXT,
            Extension TEXT,
            Date_creation REAL,
            Date_of_last_change REAL
        )
            """)
    print("Data base was created! ")


def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


def collect_data(root_directory=LOCAL_C):
    operation = input("1: Create first data base; 2: Create second data base - ")
    if operation == "1":
        connection = sqlite3.connect(r'C:\Users\Leonid\PycharmProjects\filelists\firstChecking.db')
    elif operation == "2":
        connection = sqlite3.connect(r'C:\Users\Leonid\PycharmProjects\filelists\secondChecking.db')
    else:
        print("unexpected answer")
        return

    cursor = connection.cursor()
    # drop_data_base(cursor)
    create_table(cursor)

    scan_directory(cursor, root_directory)
    connection.commit()
    connection.close()
    print("Process complete[+]")


def find_difference():
    connection_to_firstChecking = sqlite3.connect(r'C:\Users\Leonid\PycharmProjects\filelists\firstChecking.db')
    connection_to_secondChecking = sqlite3.connect(r'C:\Users\Leonid\PycharmProjects\filelists\secondChecking.db')
    cursor_firstChecking = connection_to_firstChecking.cursor()
    cursor_secondChecking = connection_to_secondChecking.cursor()

    cursor_firstChecking.execute('''
    SELECT Full_path FROM DataBase
    ''')
    cursor_secondChecking.execute('''
    SELECT Full_path FROM DataBase
    ''')
    names_file_from_firstChecking = cursor_firstChecking.fetchall()
    names_file_from_secondChecking = cursor_secondChecking.fetchall()

    print("len first - ", len(names_file_from_firstChecking), "len from second - ", len(names_file_from_secondChecking))

    first_set = set(name[0] for name in names_file_from_firstChecking)
    second_set = set(name[0] for name in names_file_from_secondChecking)

    added_files = second_set - first_set
    removed_files = first_set - second_set
    print("Added files ---", len(added_files))
    print([name for name in added_files])
    print("Removed files ---", len(removed_files))
    print([name for name in removed_files])

    connection_to_firstChecking.close()
    connection_to_secondChecking.close()


def main():
    operation = input("1: Create data base; 2: Find difference between data bases - ")
    if operation == "1":
        while True:
            root_directory = input(r"Please, enter the root folder(default is C:\) - ")
            if root_directory == "":
                root_directory = LOCAL_C
                collect_data(root_directory)
                break
            elif os.path.exists(root_directory):
                collect_data(root_directory)
                break
            else:
                print("Entered path does not exist[+]")



    elif operation == "2":
        find_difference()

    else:
        print("unexpected answer")

    return


if __name__ == '__main__':
    main()

    # if not is_admin():
    #     ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
    #     sys.exit()


