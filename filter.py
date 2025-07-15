import sqlite3


def filter_result():
    connection_first_db = sqlite3.connect(r'firstChecking.db')
    connection_second_db = sqlite3.connect(r'secondChecking.db')
    cursor_first_db = connection_first_db.cursor()
    cursor_second_db = connection_second_db.cursor()

    # фильтрация данных за счет последней даты изменения

    connection_first_db.commit()
    connection_second_db.commit()
    connection_first_db.close()
    connection_second_db.commit()


if __name__ == "__main__":
    pass