"""Скрипт для заполнения данными таблиц в БД Postgres."""

import csv
import os
import psycopg2


class ImportCSV:
    """
    Класс для получения данных из файла csv.
    Принимает:
        file_csv - название файла csv.
    """

    def __init__(self, file_csv: str) -> None:
        self.file_csv = file_csv

    def reader_csv(self) -> list:
        """
        Возвращает данные из файла csv в виде списка словарей.
        """

        list_csv = []
        with open(os.path.join('..', 'homework-1', 'north_data', self.file_csv), newline='', encoding="utf-8") as file:
            reader_csv_file = csv.DictReader(file)
            for row in reader_csv_file:
                list_csv.append(row)
            return list_csv


if __name__ == '__main__':
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='22021983')
    try:
        with conn:
            with conn.cursor() as cur:
                employees_data = ImportCSV('employees_data.csv')
                for row in employees_data.reader_csv():
                    add_table = tuple(row.values())
                    cur.execute('INSERT INTO employees_data VALUES (%s, %s, %s, %s, %s, %s)', add_table)

                customers_data = ImportCSV('customers_data.csv')
                for row in customers_data.reader_csv():
                    add_table = tuple(row.values())
                    cur.execute('INSERT INTO customers_data VALUES (%s, %s, %s)', add_table)

                orders_data = ImportCSV('orders_data.csv')
                for row in orders_data.reader_csv():
                    add_table = tuple(row.values())
                    cur.execute('INSERT INTO orders_data VALUES (%s, %s, %s, %s, %s)', add_table)

    finally:
        conn.close()
