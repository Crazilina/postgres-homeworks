"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


def load_data_from_csv(file_path, insert_query, connection):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Пропустить заголовок (или не надо лучше?)
        for row in reader:
            cur = connection.cursor()
            cur.execute(insert_query, row)
            connection.commit()


# Параметры подключения
conn = psycopg2.connect(host="localhost", dbname="north", user="postgres", password="9205")

try:
    # Загрузка данных из каждого CSV файла
    load_data_from_csv('north_data/customers_data.csv', "INSERT INTO customers VALUES (%s, %s, %s)", conn)
    load_data_from_csv('north_data/employees_data.csv', "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", conn)
    load_data_from_csv('north_data/orders_data.csv', "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", conn)
finally:
    conn.close()
