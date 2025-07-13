from multiprocessing import Process, Queue, current_process
from datetime import datetime
from pathlib import Path
import sqlite3
import os
import ctypes
import time
# import sys
# from tqdm import tqdm


SLASH = r"\\"[0]
LOCAL_C = r'C:' + SLASH
MAXIMUM_NUMBER_OF_PROCESSES = 3

#1 -347
#2 - 158
#3 -143 130
#4 - 204
#5 - 225.5799
#6 - 203.4246
#7 - 201.0505
#8 - 105.1849 202.8435 196.8880 114.3475 191.1193 204.7012
#9 - 208


def put_time_creation_to_table(path: str) -> float:
    return os.path.getctime(path)  # return time in seconds(), return time in timestamp from 1970 unix


def get_time_creation_from_table(timestamp_number):
    return datetime.fromtimestamp(timestamp_number)


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
    return True if ctypes.windll.shell32.IsUserAnAdmin() else False


def find_difference():
    connection_to_firstChecking = sqlite3.connect(os.getcwd() + r'\firstChecking.db')
    connection_to_secondChecking = sqlite3.connect(os.getcwd() + r'\secondChecking.db')
    cursor_firstChecking = connection_to_firstChecking.cursor()
    cursor_secondChecking = connection_to_secondChecking.cursor()

    connection_to_db_difference = sqlite3.connect(os.getcwd() + r'\databaseDifference.db')
    cursor_difference = connection_to_db_difference.cursor()

    # cursor_difference.execute("""
    # CREATE TABLE DataBase(
    #         Number INTEGER ,
    #         AddedFiles TEXT,
    #         RemovedFiles TEXT
    #     )
    #
    #                     """)

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

    for item in [name for name in added_files]:
        cursor_difference.execute("""
                INSERT INTO DataBase (
                
                            AddedFiles
                        ) VALUES (?)
                    """, (item,))
    for item in [name for name in removed_files]:
        cursor_difference.execute("""
                INSERT INTO DataBase (
                            RemovedFiles
                        ) VALUES (?)
                    """, (item,))
    connection_to_db_difference.commit()
    connection_to_db_difference.close()
    connection_to_firstChecking.close()
    connection_to_secondChecking.close()


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
                                            extensions if extensions else "None",
                                            put_time_creation_to_table(file_in_question), os.path.getmtime(file_in_question)))

    return


def start_multy_process(root_directory):
    print(current_process().name, " i exist", root_directory)
    starting = time.time()
    _connection = sqlite3.connect(os.getcwd() + SLASH + "timed_data_base" + SLASH + current_process().name + '.db')
    _cursor = _connection.cursor()
    # drop_data_base(_cursor)
    create_table(_cursor)
    scan_directory(_cursor, root_directory)

    _connection.commit()
    _connection.close()
    print(current_process().name, ": i finished", root_directory, f"time: {(starting - time.time()):.4f}")



def collect_data_with_multiprocessing(root_directory: str):
    operation = input("1: Create first data base; 2: Create second data base - ")
    if operation == "1":
        connection = sqlite3.connect(os.getcwd() + r'\firstChecking.db')
    elif operation == "2":
        connection = sqlite3.connect(os.getcwd() + r'\secondChecking.db')
    else:
        print("unexpected answer")
        return

    start_time = time.time()
    cursor = connection.cursor()
    drop_data_base(cursor)
    create_table(cursor)

    if not os.path.exists('timed_data_base'):
        os.makedirs('timed_data_base')

    _files = []

    for file in os.listdir(root_directory):

        if os.path.isdir(root_directory + SLASH + file):
            _files.append(file)
        else:
            _short_path = convert_to_short_path(root_directory + SLASH + file)
            extensions = Path(_short_path).suffixes
            extensions = ''.join(extensions)
            try:
                _time_creation = put_time_creation_to_table(_short_path)
                _time_modification = os.path.getmtime(_short_path)
            except FileNotFoundError:
                _time_creation = put_time_creation_to_table(root_directory + SLASH + file)
                _time_modification = os.path.getmtime(root_directory + SLASH + file)

            add_values(cursor, "DataBase", (root_directory + SLASH + file, root_directory, _short_path,
                                            file, extensions if extensions else "None",
                                            _time_creation, _time_modification))

    connection.commit()
    #
    # while True:
    #     try:
    #         print(line.get_nowait())
    #     except queue.Empty:
    #         break
    print(_files)
    processes = [Process(target=start_multy_process
                         , args=(root_directory + SLASH + _file,)) for _file in _files]
    alive_processes = []

    while True:
        if len(alive_processes) < MAXIMUM_NUMBER_OF_PROCESSES and processes:
            process = processes.pop()
            alive_processes.append(process)
            process.start()

        else:
            for alive_process in alive_processes:
                if not alive_process.is_alive():
                    alive_processes.remove(alive_process)

        if not processes and not alive_processes:
            break

    for file in os.listdir('timed_data_base'):
        if file.endswith(".db"):
            cursor.executescript(f"""
            ATTACH 'timed_data_base/{file}' AS db1;
            INSERT INTO DataBase (
                Full_path,
                Directory,
                Short_path,
                File_name,
                Extension,
                Date_creation,
                Date_of_last_change
            )
             SELECT 
                Full_path,
                Directory,
                Short_path,
                File_name,
                Extension,
                Date_creation,
                Date_of_last_change
             
             FROM db1.DataBase;
            DETACH db1;
            """)
            print("Data base", file, "was added")

            connection.commit()


    connection.commit()
    connection.close()
    print("Process complete[+]")
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Программа выполнилась за {execution_time:.4f} секунд")


def main():
    operation = input("1: Create data base; 2: Find difference between data bases - ")
    if operation == "1":
        while True:
            root_directory = input(r"Please, enter the root folder(default is C:\) - ")
            if root_directory == "":
                root_directory = LOCAL_C
                collect_data_with_multiprocessing(root_directory)
                break
            elif os.path.exists(root_directory):
                collect_data_with_multiprocessing(root_directory)
                break
            else:
                print("Entered path does not exist[+]")



    elif operation == "2":
        find_difference()

    else:
        print("unexpected answer")

    return


if __name__ == '__main__':
    line = Queue()
    main()

    # if not is_admin():
    #     ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
    #     sys.exit()
